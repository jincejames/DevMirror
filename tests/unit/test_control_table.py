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
