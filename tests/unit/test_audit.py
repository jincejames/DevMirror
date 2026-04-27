"""Unit tests for devmirror.control.audit.

All tests are offline -- DbClient is mocked.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from devmirror.control.audit import AuditRepository

FQN_PREFIX = "dev_analytics.devmirror_admin"


class TestAuditRepository:
    def _mock_db(self) -> MagicMock:
        m = MagicMock()
        m.sql_exec = MagicMock()
        m.sql_exec_with_params = MagicMock()
        m.sql = MagicMock(return_value=[])
        # Wire sql_with_params to delegate to sql so existing return_value/side_effect work
        m.sql_with_params.side_effect = lambda stmt, params: m.sql(stmt, params)
        return m

    def test_table_fqn(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        assert repo.table_fqn == f"{FQN_PREFIX}.audit_log"

    def test_append_with_explicit_log_id(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        sql = repo.append(
            db,
            dr_id="DR-1042",
            action="CREATE",
            performed_by="user@example.com",
            performed_at="2026-04-13T10:00:00Z",
            status="SUCCESS",
            log_id="log-001",
            action_detail='{"key": "value"}',
        )
        assert "INSERT INTO" in sql
        assert "audit_log" in sql
        db.sql_exec_with_params.assert_called_once()
        called_sql, params = db.sql_exec_with_params.call_args[0]
        assert called_sql == sql
        assert params["log_id"] == "log-001"
        assert params["dr_id"] == "DR-1042"
        assert params["action"] == "CREATE"
        assert params["performed_by"] == "user@example.com"
        assert params["status"] == "SUCCESS"
        assert params["action_detail"] == '{"key": "value"}'

    def test_append_generates_uuid_when_log_id_omitted(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        with patch("devmirror.control.audit.uuid.uuid4") as mock_uuid:
            mock_uuid.return_value = "fake-uuid-1234"
            repo.append(
                db,
                dr_id="DR-1",
                action="PROVISION",
                performed_by="SYSTEM",
                performed_at="2026-04-13T10:00:00Z",
                status="SUCCESS",
            )
        params = db.sql_exec_with_params.call_args[0][1]
        assert params["log_id"] == "fake-uuid-1234"

    def test_append_null_optional_fields(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        repo.append(
            db,
            dr_id="DR-1",
            action="CLEANUP",
            performed_by="SYSTEM",
            performed_at="2026-04-13T10:00:00Z",
            status="FAILED",
            log_id="log-x",
            action_detail=None,
            error_message=None,
        )
        # Both optional fields should be bound as None (driver renders NULL)
        params = db.sql_exec_with_params.call_args[0][1]
        assert params["action_detail"] is None
        assert params["error_message"] is None

    def test_append_with_error_message(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        repo.append(
            db,
            dr_id="DR-1",
            action="PROVISION",
            performed_by="SYSTEM",
            performed_at="2026-04-13T10:00:00Z",
            status="FAILED",
            log_id="log-err",
            error_message="Something went wrong",
        )
        params = db.sql_exec_with_params.call_args[0][1]
        assert params["error_message"] == "Something went wrong"

    def test_append_passes_quotes_unescaped(self) -> None:
        """With parameterized queries, callers pass raw strings; the driver handles escaping."""
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        repo.append(
            db,
            dr_id="DR-1",
            action="MODIFY",
            performed_by="O'Brien",
            performed_at="2026-04-13T10:00:00Z",
            status="SUCCESS",
            log_id="log-q",
        )
        params = db.sql_exec_with_params.call_args[0][1]
        assert params["performed_by"] == "O'Brien"

    def test_append_params_shape(self) -> None:
        """Validate exact params dict shape for regression safety."""
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        repo.append(
            db,
            dr_id="DR-1042",
            action="CREATE",
            performed_by="user@example.com",
            performed_at="2026-04-13T10:00:00Z",
            status="SUCCESS",
            log_id="log-001",
            action_detail="detail",
            error_message="err",
        )
        params = db.sql_exec_with_params.call_args[0][1]
        assert params == {
            "log_id": "log-001",
            "dr_id": "DR-1042",
            "action": "CREATE",
            "action_detail": "detail",
            "performed_by": "user@example.com",
            "performed_at": "2026-04-13T10:00:00Z",
            "status": "SUCCESS",
            "error_message": "err",
        }

    def test_list_by_dr_id(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        db.sql.return_value = [
            {"log_id": "l1", "dr_id": "DR-1042", "action": "CREATE"},
            {"log_id": "l2", "dr_id": "DR-1042", "action": "PROVISION"},
        ]
        results = repo.list_by_dr_id(db, dr_id="DR-1042")
        assert len(results) == 2
        called_sql, params = db.sql_with_params.call_args[0]
        assert "ORDER BY performed_at DESC" in called_sql
        assert "LIMIT 500" in called_sql
        assert params == {"dr_id": "DR-1042"}

    def test_list_by_dr_id_custom_limit(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        db.sql.return_value = []
        repo.list_by_dr_id(db, dr_id="DR-1", limit=10)
        called_sql = db.sql_with_params.call_args[0][0]
        assert "LIMIT 10" in called_sql

    def test_list_by_dr_id_empty_result(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        db.sql.return_value = []
        results = repo.list_by_dr_id(db, dr_id="DR-9999")
        assert results == []

    def test_list_by_action_params_shape(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        db.sql.return_value = []
        repo.list_by_action(db, action="CLEANUP")
        called_sql, params = db.sql_with_params.call_args[0]
        assert "WHERE action = :action" in called_sql
        assert "ORDER BY performed_at DESC" in called_sql
        assert params == {"action": "CLEANUP"}

    def test_purge_old_entries_generates_correct_sql(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        deleted = repo.purge_old_entries(db, retention_days=90)
        called_sql = db.sql_exec.call_args[0][0]
        assert "DELETE FROM" in called_sql
        assert "audit_log" in called_sql
        assert "DATEADD(DAY, -90, CURRENT_TIMESTAMP())" in called_sql
        assert "performed_at" in called_sql
        assert deleted == 0

    def test_purge_old_entries_default_retention(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        repo.purge_old_entries(db)
        called_sql = db.sql_exec.call_args[0][0]
        assert "-365" in called_sql

    def test_purge_old_entries_returns_zero(self) -> None:
        """purge_old_entries now returns 0 (can't count affected rows via spark.sql)."""
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        deleted = repo.purge_old_entries(db, retention_days=30)
        assert deleted == 0

    def test_purge_old_entries_raises_on_sql_failure(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        db.sql_exec.side_effect = RuntimeError("SQL error")
        with pytest.raises(RuntimeError, match="SQL error"):
            repo.purge_old_entries(db, retention_days=365)
