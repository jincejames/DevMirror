"""Tests for devmirror.utils.validation."""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest

from devmirror.config.schema import (
    Access,
    DataRevision,
    DevelopmentRequest,
    DevMirrorConfig,
    EnvironmentDev,
    EnvironmentQA,
    Environments,
    Lifecycle,
    StreamRef,
)
from devmirror.utils.validation import (
    ConfigValidationError,
    validate_config_for_submission,
    validate_delta_retention,
    validate_expiration,
)

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _cfg(*, exp=None, devs=None, qa=False, qa_users=None):
    return DevMirrorConfig(
        version="1.0",
        development_request=DevelopmentRequest(
            dr_id="DR-1", streams=[StreamRef(name="s")],
            environments=Environments(dev=EnvironmentDev(),
                                      qa=EnvironmentQA(enabled=True) if qa else None),
            data_revision=DataRevision(mode="latest"),
            access=Access(developers=devs or ["d@co.com"], qa_users=qa_users),
            lifecycle=Lifecycle(expiration_date=exp or date.today() + timedelta(days=30)),
        ),
    )

_TODAY = date(2026, 4, 13)

# ------------------------------------------------------------------
# validate_expiration
# ------------------------------------------------------------------

class TestValidateExpiration:
    @pytest.mark.parametrize("exp,ok", [
        (date(2026, 5, 1), True),
        (date(2026, 4, 12), False),  # past
        (date(2026, 4, 13), False),  # today
        (date(2026, 4, 14), True),   # tomorrow
    ])
    def test_date_validity(self, exp, ok) -> None:
        if ok:
            validate_expiration(exp, max_duration_days=90, today=_TODAY)
        else:
            with pytest.raises(ConfigValidationError, match="must be in the future"):
                validate_expiration(exp, today=_TODAY)

    def test_exceeds_max_duration(self) -> None:
        with pytest.raises(ConfigValidationError, match="exceeds"):
            validate_expiration(date(2026, 4, 2), max_duration_days=90, today=date(2026, 1, 1))

    def test_error_contains_latest_allowed(self) -> None:
        with pytest.raises(ConfigValidationError, match="2026-04-01"):
            validate_expiration(date(2026, 12, 31), max_duration_days=90, today=date(2026, 1, 1))


# ------------------------------------------------------------------
# validate_config_for_submission
# ------------------------------------------------------------------

class TestValidateConfigForSubmission:
    def test_valid(self) -> None:
        assert validate_config_for_submission(_cfg(exp=date(2026, 5, 1)), today=_TODAY) == []

    def test_past_expiration(self) -> None:
        errs = validate_config_for_submission(_cfg(exp=date(2026, 4, 1)), today=_TODAY)
        assert any("future" in e for e in errs)

    def test_qa_without_users(self) -> None:
        errs = validate_config_for_submission(_cfg(exp=date(2026, 5, 1), qa=True), today=_TODAY)
        assert any("qa_users" in e for e in errs)

    def test_qa_with_users_ok(self) -> None:
        assert validate_config_for_submission(
            _cfg(exp=date(2026, 5, 1), qa=True, qa_users=["q@co.com"]), today=_TODAY) == []

    def test_multiple_errors(self) -> None:
        errs = validate_config_for_submission(
            _cfg(exp=date(2026, 1, 1), qa=True), today=_TODAY)
        assert len(errs) >= 2


# ------------------------------------------------------------------
# validate_delta_retention
# ------------------------------------------------------------------

def _db(rows=None, side_effect=None):
    m = MagicMock()
    m.sql_exec = MagicMock()
    if side_effect:
        m.sql = MagicMock(side_effect=side_effect)
    else:
        m.sql = MagicMock(return_value=rows or [])
    return m


class TestValidateDeltaRetention:
    def test_latest_skips(self) -> None:
        db = _db()
        assert validate_delta_retention(db, ["a.b.c"], DataRevision(mode="latest")) == []
        db.sql.assert_not_called()

    @pytest.mark.parametrize("version,oldest,expect_error", [
        (10, "5", False), (5, "5", False), (3, "10", True),
    ])
    def test_version_mode(self, version, oldest, expect_error) -> None:
        errs = validate_delta_retention(
            _db([{"version": oldest, "timestamp": "2026-01-01T00:00:00Z"}]),
            ["a.b.c"], DataRevision(mode="version", version=version))
        assert bool(errs) == expect_error

    def test_timestamp_within(self) -> None:
        assert validate_delta_retention(
            _db([{"version": "1", "timestamp": "2026-01-01T00:00:00Z"}]),
            ["a.b.c"], DataRevision(mode="timestamp", timestamp="2026-06-01T00:00:00Z")) == []

    def test_timestamp_outside(self) -> None:
        errs = validate_delta_retention(
            _db([{"version": "1", "timestamp": "2026-06-01T00:00:00Z"}]),
            ["a.b.c"], DataRevision(mode="timestamp", timestamp="2025-01-01T00:00:00Z"))
        assert len(errs) == 1

    def test_sql_failure_warning(self) -> None:
        errs = validate_delta_retention(
            _db(side_effect=RuntimeError("denied")),
            ["a.b.c"], DataRevision(mode="version", version=5))
        assert "Could not check" in errs[0]

    def test_empty_history(self) -> None:
        assert validate_delta_retention(
            _db([]), ["a.b.c"], DataRevision(mode="version", version=5)) == []

    def test_multiple_tables(self) -> None:
        def side(sql):
            if "tbl_a" in sql:
                return [{"version": "10", "timestamp": "2026-01-01T00:00:00Z"}]
            return [{"version": "1", "timestamp": "2025-01-01T00:00:00Z"}]
        errs = validate_delta_retention(_db(side_effect=side), ["c.s.tbl_a", "c.s.tbl_b"],
                                        DataRevision(mode="version", version=5))
        assert len(errs) == 1 and "tbl_a" in errs[0]
