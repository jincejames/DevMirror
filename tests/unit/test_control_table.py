"""Unit tests for devmirror.control.control_table."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from devmirror.control.control_table import (
    DrAccessRepository,
    DrObjectRepository,
    DRRepository,
    DRStatus,
    ObjectStatus,
    StatusTransitionError,
    apply_control_ddl,
    render_ddl,
    validate_dr_status_transition,
    validate_object_status_transition,
)

FQN = "dev_analytics.devmirror_admin"


def _mock_db() -> MagicMock:
    m = MagicMock()
    m.sql_exec = MagicMock()
    m.sql_exec_with_params = MagicMock()
    m.sql = MagicMock(return_value=[])
    # Wire sql_with_params to delegate to sql so existing return_value/side_effect work
    m.sql_with_params.side_effect = lambda stmt, params: m.sql(stmt, params)
    return m


# ------------------------------------------------------------------
# Status transitions (parametrized)
# ------------------------------------------------------------------

_DR_ALLOWED = [
    (DRStatus.PENDING_REVIEW, DRStatus.PROVISIONING),
    (DRStatus.PROVISIONING, DRStatus.ACTIVE),
    (DRStatus.ACTIVE, DRStatus.EXPIRING_SOON),
    (DRStatus.ACTIVE, DRStatus.CLEANUP_IN_PROGRESS),
    (DRStatus.EXPIRING_SOON, DRStatus.EXPIRED),
    (DRStatus.EXPIRED, DRStatus.CLEANUP_IN_PROGRESS),
    (DRStatus.CLEANUP_IN_PROGRESS, DRStatus.CLEANED_UP),
    # FAILED is recoverable: re-provision and refresh can lift it back.
    (DRStatus.FAILED, DRStatus.PROVISIONING),
    (DRStatus.FAILED, DRStatus.ACTIVE),
    (DRStatus.FAILED, DRStatus.CLEANUP_IN_PROGRESS),
]

_DR_DISALLOWED = [
    (DRStatus.CLEANED_UP, DRStatus.ACTIVE),
    (DRStatus.ACTIVE, DRStatus.PENDING_REVIEW),
    (DRStatus.EXPIRED, DRStatus.ACTIVE),
    # REJECTED is terminal and only reachable from PENDING_REVIEW.
    (DRStatus.ACTIVE, DRStatus.REJECTED),
    (DRStatus.PROVISIONING, DRStatus.REJECTED),
    (DRStatus.REJECTED, DRStatus.ACTIVE),
    (DRStatus.REJECTED, DRStatus.PROVISIONING),
    (DRStatus.REJECTED, DRStatus.CLEANUP_IN_PROGRESS),
]

_DR_ALLOWED_REJECT = [
    (DRStatus.PENDING_REVIEW, DRStatus.REJECTED),
]


class TestDRTransitions:
    @pytest.mark.parametrize("cur,tgt", _DR_ALLOWED + _DR_ALLOWED_REJECT)
    def test_allowed(self, cur, tgt) -> None:
        validate_dr_status_transition(cur, tgt)

    @pytest.mark.parametrize("cur,tgt", _DR_DISALLOWED)
    def test_disallowed(self, cur, tgt) -> None:
        with pytest.raises(StatusTransitionError):
            validate_dr_status_transition(cur, tgt)


class TestObjectTransitions:
    @pytest.mark.parametrize("cur,tgt", [
        (ObjectStatus.PROVISIONED, ObjectStatus.REFRESH_PENDING),
        (ObjectStatus.PROVISIONED, ObjectStatus.DROPPED),
        (ObjectStatus.REFRESH_PENDING, ObjectStatus.PROVISIONED),
        (ObjectStatus.FAILED, ObjectStatus.PROVISIONED),
    ])
    def test_allowed(self, cur, tgt) -> None:
        validate_object_status_transition(cur, tgt)

    @pytest.mark.parametrize("cur,tgt", [
        (ObjectStatus.DROPPED, ObjectStatus.PROVISIONED),
        (ObjectStatus.REFRESH_PENDING, ObjectStatus.DROPPED),
    ])
    def test_disallowed(self, cur, tgt) -> None:
        with pytest.raises(StatusTransitionError):
            validate_object_status_transition(cur, tgt)


# ------------------------------------------------------------------
# DRRepository
# ------------------------------------------------------------------

class TestDRRepository:
    def test_insert(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        sql = repo.insert(db, dr_id="DR-1", description="Test", status="PENDING_REVIEW",
                          config_yaml=None, created_at="2026-01-01T00:00:00Z",
                          created_by="u@x.com", expiration_date="2026-06-15")
        assert "INSERT INTO" in sql
        db.sql_exec_with_params.assert_called_once()
        called_sql, params = db.sql_exec_with_params.call_args[0]
        assert called_sql == sql
        assert params["dr_id"] == "DR-1"
        assert params["description"] == "Test"
        assert params["status"] == "PENDING_REVIEW"
        assert params["created_by"] == "u@x.com"
        # config_yaml and last_modified_at are None -> rendered as NULL, not bound
        assert "config_yaml" not in params
        assert "last_modified_at" not in params
        assert ", NULL," in sql or "NULL," in sql  # NULL literal present

    def test_insert_passes_quotes_unescaped(self) -> None:
        """With parameterized queries, the driver handles quote escaping."""
        repo, db = DRRepository(FQN), _mock_db()
        repo.insert(db, dr_id="DR-1", description="It's", status="PENDING_REVIEW",
                    config_yaml=None, created_at="2026-01-01T00:00:00Z",
                    created_by="u@x.com", expiration_date="2026-06-15")
        params = db.sql_exec_with_params.call_args[0][1]
        assert params["description"] == "It's"

    def test_insert_params_shape(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        repo.insert(db, dr_id="DR-1", description="Test", status="PENDING_REVIEW",
                    config_yaml="cfg: 1", created_at="2026-01-01T00:00:00Z",
                    created_by="u@x.com", expiration_date="2026-06-15",
                    last_modified_at="2026-02-01T00:00:00Z")
        params = db.sql_exec_with_params.call_args[0][1]
        assert params == {
            "dr_id": "DR-1",
            "description": "Test",
            "status": "PENDING_REVIEW",
            "config_yaml": "cfg: 1",
            "created_at": "2026-01-01T00:00:00Z",
            "created_by": "u@x.com",
            "expiration_date": "2026-06-15",
            "last_modified_at": "2026-02-01T00:00:00Z",
        }

    def test_update_status(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        repo.update_status(db, dr_id="DR-1", current_status=DRStatus.PENDING_REVIEW,
                           new_status=DRStatus.PROVISIONING, last_modified_at="now")
        db.sql_exec_with_params.assert_called_once()
        called_sql, params = db.sql_exec_with_params.call_args[0]
        assert "status = :new_status" in called_sql
        assert params == {
            "dr_id": "DR-1",
            "new_status": "PROVISIONING",
            "current_status": "PENDING_REVIEW",
            "last_modified_at": "now",
        }

    def test_update_status_invalid(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        with pytest.raises(StatusTransitionError):
            repo.update_status(db, dr_id="DR-1", current_status=DRStatus.CLEANED_UP,
                               new_status=DRStatus.ACTIVE, last_modified_at="now")
        db.sql_exec_with_params.assert_not_called()

    def test_force_status_unconditional(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        repo.force_status(db, dr_id="DR-1", new_status=DRStatus.ACTIVE,
                          last_modified_at="now")
        called_sql, params = db.sql_exec_with_params.call_args[0]
        # No CAS gate -- status should not appear in WHERE clause
        assert "status = :current_status" not in called_sql
        assert "WHERE dr_id = :dr_id" in called_sql
        assert params == {
            "dr_id": "DR-1",
            "new_status": "ACTIVE",
            "last_modified_at": "now",
        }

    def test_force_status_skips_transition_validation(self) -> None:
        # CLEANED_UP -> ACTIVE is rejected by update_status, but force_status
        # exists precisely so the runner can recover from a stuck row even
        # if the transition would have been invalid by the state machine.
        repo, db = DRRepository(FQN), _mock_db()
        repo.force_status(db, dr_id="DR-1", new_status=DRStatus.ACTIVE,
                          last_modified_at="now")
        db.sql_exec_with_params.assert_called_once()

    def test_get_found_and_missing(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        db.sql.return_value = [{"dr_id": "DR-1"}]
        assert repo.get(db, dr_id="DR-1") is not None
        called_sql, params = db.sql_with_params.call_args[0]
        assert ":dr_id" in called_sql
        assert params == {"dr_id": "DR-1"}
        db.sql.return_value = []
        assert repo.get(db, dr_id="DR-99") is None

    def test_list_active(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        db.sql.return_value = [{"dr_id": "DR-1"}]
        assert len(repo.list_active(db)) == 1
        called_sql, params = db.sql_with_params.call_args[0]
        assert "status IN (:s_pending, :s_provisioning, :s_active, :s_expiring)" in called_sql
        assert params == {
            "s_pending": "PENDING_REVIEW",
            "s_provisioning": "PROVISIONING",
            "s_active": "ACTIVE",
            "s_expiring": "EXPIRING_SOON",
        }

    def test_update_notification_sent_params(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        repo.update_notification_sent(db, dr_id="DR-1", notification_sent_at="2026-04-01T00:00:00Z")
        called_sql, params = db.sql_exec_with_params.call_args[0]
        assert ":notification_sent_at" in called_sql
        assert params == {"dr_id": "DR-1", "notification_sent_at": "2026-04-01T00:00:00Z"}

    def test_reject_writes_metadata_columns_and_status(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        sql = repo.reject(
            db, dr_id="DR-1",
            current_status=DRStatus.PENDING_REVIEW,
            comment="Doesn't follow naming convention",
            rejected_by="admin@co.com",
            rejected_at="2026-05-22T10:00:00Z",
        )
        # SQL touches all five fields in one UPDATE -> CAS-gated on current
        # status so concurrent provisions on the same DR don't silently flip
        # the row.
        assert "UPDATE" in sql
        assert "rejection_comment = :rejection_comment" in sql
        assert "rejected_by = :rejected_by" in sql
        assert "rejected_at = :rejected_at" in sql
        assert "status = :new_status" in sql
        assert "AND status = :current_status" in sql
        params = db.sql_exec_with_params.call_args[0][1]
        assert params["new_status"] == "REJECTED"
        assert params["current_status"] == "PENDING_REVIEW"
        assert params["rejection_comment"] == "Doesn't follow naming convention"
        assert params["rejected_by"] == "admin@co.com"
        assert params["rejected_at"] == "2026-05-22T10:00:00Z"
        # last_modified_at mirrors rejected_at so the row's audit timestamp
        # stays consistent.
        assert params["last_modified_at"] == "2026-05-22T10:00:00Z"

    def test_reject_validates_transition(self) -> None:
        # Only PENDING_REVIEW -> REJECTED is allowed.  Attempting to reject
        # an ACTIVE DR must raise BEFORE any UPDATE is issued.
        repo, db = DRRepository(FQN), _mock_db()
        with pytest.raises(StatusTransitionError):
            repo.reject(
                db, dr_id="DR-1",
                current_status=DRStatus.ACTIVE,
                comment="Too late",
                rejected_by="admin@co.com",
                rejected_at="2026-05-22T10:00:00Z",
            )
        db.sql_exec_with_params.assert_not_called()


# ------------------------------------------------------------------
# DrObjectRepository
# ------------------------------------------------------------------

_SAMPLE_OBJ = {"dr_id": "DR-1", "source_fqn": "p.s.t", "target_fqn": "d.s.t",
               "target_environment": "dev", "object_type": "table", "access_mode": "READ_ONLY",
               "clone_strategy": "shallow_clone", "clone_revision_mode": "latest",
               "clone_revision_value": None, "provisioned_at": None, "last_refreshed_at": None,
               "status": "PROVISIONED", "estimated_size_gb": None}


class TestDrObjectRepository:
    def test_bulk_insert(self) -> None:
        repo, db = DrObjectRepository(FQN), _mock_db()
        stmts = repo.bulk_insert(db, objects=[_SAMPLE_OBJ])
        assert len(stmts) == 1 and "INSERT INTO" in stmts[0]
        db.sql_exec_with_params.assert_called_once()
        params = db.sql_exec_with_params.call_args[0][1]
        assert params["dr_id"] == "DR-1"
        assert params["source_fqn"] == "p.s.t"
        assert params["status"] == "PROVISIONED"
        # Optional fields are None -> not bound, NULL in SQL
        assert "clone_revision_value" not in params

    def test_bulk_insert_params_shape(self) -> None:
        repo, db = DrObjectRepository(FQN), _mock_db()
        obj = {**_SAMPLE_OBJ, "provisioned_at": "2026-04-01T00:00:00Z",
               "estimated_size_gb": 2.5}
        repo.bulk_insert(db, objects=[obj])
        params = db.sql_exec_with_params.call_args[0][1]
        assert params["provisioned_at"] == "2026-04-01T00:00:00Z"
        # estimated_size_gb is interpolated as numeric literal, not bound
        assert "estimated_size_gb" not in params
        assert "2.5" in db.sql_exec_with_params.call_args[0][0]

    def test_update_status(self) -> None:
        repo, db = DrObjectRepository(FQN), _mock_db()
        repo.update_object_status(db, dr_id="DR-1", source_fqn="p.s.t",
                                  target_environment="dev",
                                  current_status=ObjectStatus.PROVISIONED,
                                  new_status=ObjectStatus.REFRESH_PENDING)
        called_sql, params = db.sql_exec_with_params.call_args[0]
        assert ":new_status" in called_sql
        assert params["new_status"] == "REFRESH_PENDING"
        assert params["current_status"] == "PROVISIONED"

    def test_update_status_invalid(self) -> None:
        repo, db = DrObjectRepository(FQN), _mock_db()
        with pytest.raises(StatusTransitionError):
            repo.update_object_status(db, dr_id="DR-1", source_fqn="p.s.t",
                                      target_environment="dev",
                                      current_status=ObjectStatus.DROPPED,
                                      new_status=ObjectStatus.PROVISIONED)

    def test_list_by_dr_id_params(self) -> None:
        repo, db = DrObjectRepository(FQN), _mock_db()
        db.sql.return_value = []
        repo.list_by_dr_id(db, dr_id="DR-1")
        called_sql, params = db.sql_with_params.call_args[0]
        assert ":dr_id" in called_sql
        assert params == {"dr_id": "DR-1"}

    def test_delete_by_dr_id_params(self) -> None:
        repo, db = DrObjectRepository(FQN), _mock_db()
        repo.delete_by_dr_id(db, dr_id="DR-1")
        called_sql, params = db.sql_exec_with_params.call_args[0]
        assert "DELETE FROM" in called_sql
        assert params == {"dr_id": "DR-1"}

    def test_counts_by_dr_id_empty_input(self) -> None:
        # No DRs to count -> short-circuit, no SQL issued.
        repo, db = DrObjectRepository(FQN), _mock_db()
        result = repo.counts_by_dr_id(db, dr_ids=[])
        assert result == {}
        db.sql.assert_not_called()

    def test_counts_by_dr_id_returns_map(self) -> None:
        # The query is now an unfiltered GROUP BY; the repo filters by
        # dr_ids in Python.  DR-X exists in the table but isn't in the
        # caller's wanted list -- it's omitted from the result map.
        repo, db = DrObjectRepository(FQN), _mock_db()
        db.sql.return_value = [
            {"dr_id": "DR-1", "n": 5},
            {"dr_id": "DR-2", "n": 12},
            {"dr_id": "DR-X", "n": 99},
        ]
        result = repo.counts_by_dr_id(db, dr_ids=["DR-1", "DR-2", "DR-3"])
        assert result == {"DR-1": 5, "DR-2": 12}
        # DR-3 is absent from the result map -- caller treats absence as 0.
        # DR-X exists in the table but wasn't requested -- correctly dropped.

    def test_counts_by_dr_id_sql_shape(self) -> None:
        # No WHERE, no params -- intentionally simple to dodge any
        # upstream IN-clause/named-param edge cases.
        repo, db = DrObjectRepository(FQN), _mock_db()
        db.sql.return_value = []
        repo.counts_by_dr_id(db, dr_ids=["DR-1", "DR-2"])
        sql = db.sql.call_args[0][0]
        assert "COUNT(*)" in sql
        assert "GROUP BY dr_id" in sql
        assert "WHERE" not in sql
        # Verify no named-param SQL path was used.
        db.sql_with_params.assert_not_called()

    def test_counts_by_dr_id_handles_string_counts(self) -> None:
        # Statement Execution API JSON_ARRAY format returns numbers
        # as strings; the repo must coerce.
        repo, db = DrObjectRepository(FQN), _mock_db()
        db.sql.return_value = [{"dr_id": "DR-1", "n": "7"}]
        result = repo.counts_by_dr_id(db, dr_ids=["DR-1"])
        assert result == {"DR-1": 7}

    def test_counts_by_dr_id_skips_malformed_rows(self) -> None:
        # Rows with missing/unparseable values shouldn't blow up the
        # whole call -- they're skipped, the rest are returned.
        repo, db = DrObjectRepository(FQN), _mock_db()
        db.sql.return_value = [
            {"dr_id": "DR-1", "n": 5},
            {"dr_id": None, "n": 99},          # missing dr_id
            {"dr_id": "DR-2", "n": "not-a-number"},
            {"dr_id": "DR-3", "n": 3},
        ]
        result = repo.counts_by_dr_id(
            db, dr_ids=["DR-1", "DR-2", "DR-3"],
        )
        assert result == {"DR-1": 5, "DR-3": 3}


# ------------------------------------------------------------------
# DrAccessRepository
# ------------------------------------------------------------------

_SAMPLE_ROW = {"dr_id": "DR-1", "user_email": "a@x.com", "environment": "dev",
               "access_level": "READ_WRITE", "granted_at": "2026-01-01T00:00:00Z"}


class TestDrAccessRepository:
    def test_bulk_insert(self) -> None:
        repo, db = DrAccessRepository(FQN), _mock_db()
        stmts = repo.bulk_insert(db, rows=[_SAMPLE_ROW])
        assert len(stmts) == 1 and "INSERT INTO" in stmts[0]
        db.sql_exec_with_params.assert_called_once()
        params = db.sql_exec_with_params.call_args[0][1]
        assert params == {
            "dr_id": "DR-1",
            "user_email": "a@x.com",
            "environment": "dev",
            "access_level": "READ_WRITE",
            "granted_at": "2026-01-01T00:00:00Z",
        }

    def test_list_by_dr_id_params(self) -> None:
        repo, db = DrAccessRepository(FQN), _mock_db()
        db.sql.return_value = []
        repo.list_by_dr_id(db, dr_id="DR-1")
        called_sql, params = db.sql_with_params.call_args[0]
        assert ":dr_id" in called_sql
        assert params == {"dr_id": "DR-1"}

    def test_delete_by_dr_id_params(self) -> None:
        repo, db = DrAccessRepository(FQN), _mock_db()
        repo.delete_by_dr_id(db, dr_id="DR-1")
        called_sql, params = db.sql_exec_with_params.call_args[0]
        assert "DELETE FROM" in called_sql
        assert params == {"dr_id": "DR-1"}



# ------------------------------------------------------------------
# DDL
# ------------------------------------------------------------------

class TestDDL:
    def test_render_ddl(self) -> None:
        # render_ddl now concatenates every migration in devmirror/migrations/
        # (001 + 002 + 003), so it emits all six control-plane tables PLUS
        # two forward-compat ALTERs that add rejection columns to
        # fastsetup_development_requests (legacy planned reject path) and
        # fastsetup_configs (current canonical reject path).  Total: 8.
        stmts = render_ddl("c", "s")
        assert len(stmts) == 8
        joined = "\n".join(stmts)
        for name in [
            "fastsetup_development_requests",
            "fastsetup_dr_objects",
            "fastsetup_dr_access",
            "audit_log",
            "fastsetup_configs",
            "fastsetup_id_counter",
        ]:
            assert name in joined
        # Rejection columns land via both CREATE (new deploys) and an
        # idempotent ALTER (existing deploys).
        assert "rejection_comment" in joined
        assert "rejected_by" in joined
        assert "rejected_at" in joined
        assert "ALTER TABLE" in joined
        assert "{control_catalog}" not in joined

    def test_apply_ddl(self) -> None:
        db, settings = _mock_db(), MagicMock()
        settings.control_catalog = "c"
        settings.control_schema = "s"
        assert len(apply_control_ddl(db, settings)) == 8

    def test_apply_ddl_is_best_effort_on_per_statement_failure(self) -> None:
        # ALTER TABLE ADD COLUMNS fails on a second run because the columns
        # already exist.  apply_control_ddl must swallow per-statement
        # failures and keep going so the rest of the migration runs.
        db, settings = MagicMock(), MagicMock()
        settings.control_catalog = "c"
        settings.control_schema = "s"
        # Mid-loop failure on the 4th statement; later ones must still run.
        outcomes = [None, None, None, RuntimeError("boom"), None, None, None, None]
        db.sql_exec = MagicMock(side_effect=outcomes)
        applied = apply_control_ddl(db, settings)
        # All 8 statements were attempted; 7 succeeded, 1 swallowed.
        assert len(applied) == 7
        assert db.sql_exec.call_count == 8
