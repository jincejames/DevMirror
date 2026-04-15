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
    m.sql = MagicMock(return_value=[])
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
]

_DR_DISALLOWED = [
    (DRStatus.CLEANED_UP, DRStatus.ACTIVE),
    (DRStatus.FAILED, DRStatus.ACTIVE),
    (DRStatus.ACTIVE, DRStatus.PENDING_REVIEW),
    (DRStatus.EXPIRED, DRStatus.ACTIVE),
]


class TestDRTransitions:
    @pytest.mark.parametrize("cur,tgt", _DR_ALLOWED)
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
        assert "INSERT INTO" in sql and "DR-1" in sql
        db.sql_exec.assert_called_once()

    def test_insert_escapes_quotes(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        sql = repo.insert(db, dr_id="DR-1", description="It's", status="PENDING_REVIEW",
                          config_yaml=None, created_at="2026-01-01T00:00:00Z",
                          created_by="u@x.com", expiration_date="2026-06-15")
        assert "It''s" in sql

    def test_update_status(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        sql = repo.update_status(db, dr_id="DR-1", current_status=DRStatus.PENDING_REVIEW,
                                 new_status=DRStatus.PROVISIONING, last_modified_at="now")
        assert "PROVISIONING" in sql
        db.sql_exec.assert_called_once()

    def test_update_status_invalid(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        with pytest.raises(StatusTransitionError):
            repo.update_status(db, dr_id="DR-1", current_status=DRStatus.CLEANED_UP,
                               new_status=DRStatus.ACTIVE, last_modified_at="now")
        db.sql_exec.assert_not_called()

    def test_get_found_and_missing(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        db.sql.return_value = [{"dr_id": "DR-1"}]
        assert repo.get(db, dr_id="DR-1") is not None
        db.sql.return_value = []
        assert repo.get(db, dr_id="DR-99") is None

    def test_list_active(self) -> None:
        repo, db = DRRepository(FQN), _mock_db()
        db.sql.return_value = [{"dr_id": "DR-1"}]
        assert len(repo.list_active(db)) == 1
        sql = db.sql.call_args[0][0]
        assert "ACTIVE" in sql and "CLEANED_UP" not in sql


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

    def test_update_status(self) -> None:
        repo, db = DrObjectRepository(FQN), _mock_db()
        sql = repo.update_object_status(db, dr_id="DR-1", source_fqn="p.s.t",
                                        target_environment="dev",
                                        current_status=ObjectStatus.PROVISIONED,
                                        new_status=ObjectStatus.REFRESH_PENDING)
        assert "REFRESH_PENDING" in sql

    def test_update_status_invalid(self) -> None:
        repo, db = DrObjectRepository(FQN), _mock_db()
        with pytest.raises(StatusTransitionError):
            repo.update_object_status(db, dr_id="DR-1", source_fqn="p.s.t",
                                      target_environment="dev",
                                      current_status=ObjectStatus.DROPPED,
                                      new_status=ObjectStatus.PROVISIONED)


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



# ------------------------------------------------------------------
# DDL
# ------------------------------------------------------------------

class TestDDL:
    def test_render_ddl(self) -> None:
        stmts = render_ddl("c", "s")
        assert len(stmts) == 4
        joined = "\n".join(stmts)
        for name in ["devmirror_development_requests", "devmirror_dr_objects", "devmirror_dr_access", "audit_log"]:
            assert name in joined
        assert "{control_catalog}" not in joined

    def test_apply_ddl(self) -> None:
        db, settings = _mock_db(), MagicMock()
        settings.control_catalog = "c"
        settings.control_schema = "s"
        assert len(apply_control_ddl(db, settings)) == 4
        assert db.sql_exec.call_count == 4
