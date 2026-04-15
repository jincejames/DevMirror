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
        m.sql = MagicMock(return_value=[])
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
        assert "log-001" in sql
        assert "DR-1042" in sql
        assert "CREATE" in sql
        assert "user@example.com" in sql
        assert "SUCCESS" in sql
        assert '{"key": "value"}' in sql
        db.sql_exec.assert_called_once_with(sql)

    def test_append_generates_uuid_when_log_id_omitted(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        with patch("devmirror.control.audit.uuid.uuid4") as mock_uuid:
            mock_uuid.return_value = "fake-uuid-1234"
            sql = repo.append(
                db,
                dr_id="DR-1",
                action="PROVISION",
                performed_by="SYSTEM",
                performed_at="2026-04-13T10:00:00Z",
                status="SUCCESS",
            )
        assert "fake-uuid-1234" in sql

    def test_append_null_optional_fields(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        sql = repo.append(
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
        # Both optional fields should be NULL
        assert sql.count("NULL") == 2

    def test_append_with_error_message(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        sql = repo.append(
            db,
            dr_id="DR-1",
            action="PROVISION",
            performed_by="SYSTEM",
            performed_at="2026-04-13T10:00:00Z",
            status="FAILED",
            log_id="log-err",
            error_message="Something went wrong",
        )
        assert "Something went wrong" in sql

    def test_append_escapes_single_quotes(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        sql = repo.append(
            db,
            dr_id="DR-1",
            action="MODIFY",
            performed_by="O'Brien",
            performed_at="2026-04-13T10:00:00Z",
            status="SUCCESS",
            log_id="log-q",
        )
        assert "O''Brien" in sql

    def test_list_by_dr_id(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        db.sql.return_value = [
            {"log_id": "l1", "dr_id": "DR-1042", "action": "CREATE"},
            {"log_id": "l2", "dr_id": "DR-1042", "action": "PROVISION"},
        ]
        results = repo.list_by_dr_id(db, dr_id="DR-1042")
        assert len(results) == 2
        called_sql = db.sql.call_args[0][0]
        assert "DR-1042" in called_sql
        assert "ORDER BY performed_at DESC" in called_sql
        assert "LIMIT 500" in called_sql

    def test_list_by_dr_id_custom_limit(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        db.sql.return_value = []
        repo.list_by_dr_id(db, dr_id="DR-1", limit=10)
        called_sql = db.sql.call_args[0][0]
        assert "LIMIT 10" in called_sql

    def test_list_by_dr_id_empty_result(self) -> None:
        repo = AuditRepository(FQN_PREFIX)
        db = self._mock_db()
        db.sql.return_value = []
        results = repo.list_by_dr_id(db, dr_id="DR-9999")
        assert results == []

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
