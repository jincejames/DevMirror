"""Tests for devmirror.utils.id_generator (Stage 4 US-34).

Covers pure helpers (``format_dr_id`` / ``is_legacy_dr_id``) and the
``IdCounterRepository`` optimistic-retry increment loop via a fake
:class:`DbClient`.
"""

from __future__ import annotations

import pytest

from devmirror.settings import Settings
from devmirror.utils.id_generator import (
    MAX_COUNTER_RETRIES,
    IdCounterRepository,
    format_dr_id,
    is_legacy_dr_id,
    next_dr_id,
)


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------

class TestFormatDrId:
    @pytest.mark.parametrize("prefix,counter,padding,expected", [
        ("DR", 1, 5, "DR00001"),
        ("DR", 23, 5, "DR00023"),
        ("DR", 99999, 5, "DR99999"),
        ("DR", 100000, 5, "DR100000"),  # overflow -- counter rendered in full
        ("PROJ", 42, 6, "PROJ000042"),
        ("X", 7, 3, "X007"),
    ])
    def test_format(self, prefix: str, counter: int, padding: int, expected: str) -> None:
        assert format_dr_id(prefix, counter, padding) == expected

    def test_rejects_negative_counter(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            format_dr_id("DR", -1, 5)

    def test_rejects_zero_padding(self) -> None:
        with pytest.raises(ValueError, match=">= 1"):
            format_dr_id("DR", 1, 0)

    def test_sorts_lexically(self) -> None:
        """US-34 AC5: zero-padded IDs sort lexically."""
        ids = [format_dr_id("DR", n, 5) for n in [2, 10, 1, 100, 99]]
        assert sorted(ids) == ["DR00001", "DR00002", "DR00010", "DR00099", "DR00100"]


class TestIsLegacyDrId:
    @pytest.mark.parametrize("dr_id,expected", [
        ("DR-1", True),
        ("DR-1042", True),
        ("DR-0", True),
        ("DR00001", False),
        ("dr-1", False),  # case sensitive
        ("DR-", False),
        ("", False),
        ("PROJ-1", False),
    ])
    def test_detection(self, dr_id: str, expected: bool) -> None:
        assert is_legacy_dr_id(dr_id) is expected


# ---------------------------------------------------------------------------
# IdCounterRepository (optimistic-retry increment)
# ---------------------------------------------------------------------------

class FakeDbClient:
    """In-memory stand-in for ``DbClient`` that understands the three
    statements used by :class:`IdCounterRepository`.

    ``rows`` maps ``prefix -> last_value``.  ``insert_fail_once`` / other
    hooks let tests simulate race conditions.
    """

    def __init__(self) -> None:
        self.rows: dict[str, int] = {}
        # Hooks for race simulation.
        self.insert_should_race: bool = False
        self.update_should_race: int = 0  # number of times UPDATE should "lose"
        self.ddl_calls: int = 0

    # -- matching plain sql_exec (DDL only) --------------------------------

    def sql_exec(self, statement: str) -> None:
        if statement.upper().startswith("CREATE TABLE"):
            self.ddl_calls += 1
            return
        raise AssertionError(f"Unexpected sql_exec: {statement}")

    # -- matching sql_with_params / sql_exec_with_params -------------------

    def sql_with_params(self, statement: str, params: dict[str, str | None]) -> list[dict]:
        stmt = statement.strip()
        if stmt.startswith("SELECT last_value"):
            prefix = params["prefix"]
            if prefix in self.rows:
                return [{"last_value": self.rows[prefix]}]
            return []
        raise AssertionError(f"Unexpected SELECT: {statement}")

    def sql_exec_with_params(
        self, statement: str, params: dict[str, str | None]
    ) -> None:
        stmt = statement.strip()
        if stmt.startswith("INSERT INTO"):
            if self.insert_should_race:
                # Simulate another worker winning the race.
                self.insert_should_race = False
                self.rows[params["prefix"]] = 1
                raise RuntimeError("duplicate prefix")
            self.rows[params["prefix"]] = 1
            return
        if stmt.startswith("UPDATE"):
            prefix = params["prefix"]
            current = int(params["current"])  # type: ignore[arg-type]
            new_value = int(params["new_value"])  # type: ignore[arg-type]
            stored = self.rows.get(prefix)
            if self.update_should_race > 0:
                # Simulate another worker that already advanced the counter
                # past ``current + 1`` -- our CAS is a no-op and the read-
                # back verification will detect the loss.
                self.update_should_race -= 1
                self.rows[prefix] = (stored or 0) + 2
                return
            if stored == current:
                self.rows[prefix] = new_value
            return
        raise AssertionError(f"Unexpected DML: {statement}")


class TestIdCounterRepository:
    def test_bootstrap_returns_one(self) -> None:
        db = FakeDbClient()
        repo = IdCounterRepository("cat.schema")
        repo.ensure_table(db)
        assert repo.next_value(db, "DR") == 1
        assert db.rows["DR"] == 1
        assert db.ddl_calls == 1

    def test_sequential_increments(self) -> None:
        db = FakeDbClient()
        repo = IdCounterRepository("cat.schema")
        repo.ensure_table(db)
        values = [repo.next_value(db, "DR") for _ in range(5)]
        assert values == [1, 2, 3, 4, 5]

    def test_separate_prefixes_separate_counters(self) -> None:
        db = FakeDbClient()
        repo = IdCounterRepository("cat.schema")
        repo.ensure_table(db)
        assert repo.next_value(db, "DR") == 1
        assert repo.next_value(db, "PROJ") == 1
        assert repo.next_value(db, "DR") == 2

    def test_insert_race_falls_back_to_update(self) -> None:
        """If INSERT races (another worker bootstrapped), we retry via UPDATE."""
        db = FakeDbClient()
        db.insert_should_race = True
        repo = IdCounterRepository("cat.schema")
        repo.ensure_table(db)
        # The first attempt fails the insert; the second iteration reads
        # last_value=1 and UPDATEs to 2.
        assert repo.next_value(db, "DR") == 2

    def test_update_race_retries_and_succeeds(self) -> None:
        """A single UPDATE loss is retried and eventually wins."""
        db = FakeDbClient()
        repo = IdCounterRepository("cat.schema")
        repo.ensure_table(db)
        repo.next_value(db, "DR")  # seed to 1
        db.update_should_race = 1  # next update "loses" once
        value = repo.next_value(db, "DR")
        # After the race another worker advanced to 3 (simulating a
        # past-current writer); our retry reads 3 and CAS-advances to 4.
        assert value == 4

    def test_retry_exhaustion_raises(self) -> None:
        db = FakeDbClient()
        repo = IdCounterRepository("cat.schema")
        repo.ensure_table(db)
        repo.next_value(db, "DR")
        db.update_should_race = MAX_COUNTER_RETRIES  # every attempt loses
        with pytest.raises(RuntimeError, match="Could not acquire"):
            repo.next_value(db, "DR")


# ---------------------------------------------------------------------------
# next_dr_id (settings-driven composition)
# ---------------------------------------------------------------------------

class TestNextDrId:
    def test_uses_prefix_and_padding(self) -> None:
        db = FakeDbClient()
        settings = Settings(
            control_catalog="cat",
            control_schema="schema",
            dr_id_prefix="DR",
            dr_id_padding=5,
        )
        first = next_dr_id(db, settings)
        second = next_dr_id(db, settings)
        assert first == "DR00001"
        assert second == "DR00002"

    def test_custom_prefix_and_padding(self) -> None:
        db = FakeDbClient()
        settings = Settings(
            control_catalog="cat",
            control_schema="schema",
            dr_id_prefix="PROJ",
            dr_id_padding=6,
        )
        assert next_dr_id(db, settings) == "PROJ000001"

    def test_counter_is_not_rolled_back_on_caller_error(self) -> None:
        """Gaps are acceptable (per spec): a caller that fails after
        allocating an ID is NOT expected to roll the counter back.
        Each call to ``next_dr_id`` advances the counter.
        """
        db = FakeDbClient()
        settings = Settings(
            control_catalog="cat",
            control_schema="schema",
            dr_id_prefix="DR",
            dr_id_padding=5,
        )
        first = next_dr_id(db, settings)
        # Simulate a caller who allocates but then crashes / fails
        # validation: no rollback hook is exposed.  The next allocation
        # simply moves on -- the spec explicitly allows gaps.
        second = next_dr_id(db, settings)
        third = next_dr_id(db, settings)
        assert first == "DR00001"
        assert second == "DR00002"
        assert third == "DR00003"
