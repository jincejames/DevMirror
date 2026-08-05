"""Tests for ConfigRepository with mocked DbClient."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from backend.repository import ConfigRepository


@pytest.fixture()
def mock_db():
    db = MagicMock()
    db.sql.return_value = []
    db.sql_exec.return_value = None
    db.sql_exec_with_params.return_value = None
    # Wire sql_with_params to delegate to sql so return_value/side_effect work
    db.sql_with_params.side_effect = lambda stmt, params: db.sql(stmt, params)
    return db


@pytest.fixture()
def repo():
    return ConfigRepository("test_catalog.test_schema")


class TestInsert:
    def test_insert_generates_correct_sql(self, repo, mock_db):
        repo.insert(
            mock_db,
            dr_id="DR-100",
            config_json='{"dr_id":"DR-100"}',
            config_yaml="version: '1.0'",
            status="valid",
            validation_errors="[]",
            created_by="user@example.com",
            expiration_date="2026-05-01",
            description="Test config",
        )

        mock_db.sql_exec_with_params.assert_called_once()
        sql = mock_db.sql_exec_with_params.call_args[0][0]
        params = mock_db.sql_exec_with_params.call_args[0][1]
        assert "INSERT INTO test_catalog.test_schema.fastsetup_configs" in sql
        assert params["dr_id"] == "DR-100"
        assert params["created_by"] == "user@example.com"
        assert params["expiration_date"] == "2026-05-01"
        assert params["description"] == "Test config"

    def test_insert_null_description(self, repo, mock_db):
        repo.insert(
            mock_db,
            dr_id="DR-101",
            config_json="{}",
            config_yaml="",
            status="valid",
            validation_errors="[]",
            created_by="user@example.com",
            expiration_date="2026-05-01",
            description=None,
        )

        sql = mock_db.sql_exec_with_params.call_args[0][0]
        params = mock_db.sql_exec_with_params.call_args[0][1]
        assert "NULL" in sql
        assert "description" not in params


class TestGet:
    def test_get_returns_parsed_dict(self, repo, mock_db):
        mock_db.sql.return_value = [
            {
                "dr_id": "DR-100",
                "config_json": "{}",
                "status": "valid",
                "created_at": "2026-04-01T00:00:00",
                "created_by": "user@example.com",
                "expiration_date": "2026-05-01",
            }
        ]
        result = repo.get(mock_db, dr_id="DR-100")
        assert result is not None
        assert result["dr_id"] == "DR-100"

        mock_db.sql_with_params.assert_called_once()
        sql = mock_db.sql_with_params.call_args[0][0]
        params = mock_db.sql_with_params.call_args[0][1]
        assert "WHERE dr_id = :dr_id" in sql
        assert params["dr_id"] == "DR-100"

    def test_get_returns_none_when_not_found(self, repo, mock_db):
        mock_db.sql.return_value = []
        result = repo.get(mock_db, dr_id="DR-999")
        assert result is None


class TestListAll:
    def test_list_all_returns_rows(self, repo, mock_db):
        mock_db.sql.return_value = [
            {"dr_id": "DR-1", "status": "valid"},
            {"dr_id": "DR-2", "status": "invalid"},
        ]
        result = repo.list_all(mock_db)
        assert len(result) == 2

        sql = mock_db.sql.call_args[0][0]
        assert "ORDER BY created_at DESC" in sql


class TestUpdate:
    def test_update_generates_correct_sql(self, repo, mock_db):
        repo.update(
            mock_db,
            dr_id="DR-100",
            config_json='{"updated": true}',
            config_yaml="updated: yaml",
            status="valid",
            validation_errors="[]",
            expiration_date="2026-06-01",
            description="Updated",
        )

        sql = mock_db.sql_exec_with_params.call_args[0][0]
        params = mock_db.sql_exec_with_params.call_args[0][1]
        assert "UPDATE test_catalog.test_schema.fastsetup_configs" in sql
        assert "WHERE dr_id = :dr_id" in sql
        assert params["dr_id"] == "DR-100"
        assert params["description"] == "Updated"


class TestReject:
    def test_reject_writes_status_and_rejection_columns(self, repo, mock_db):
        repo.reject(
            mock_db,
            dr_id="DR-100",
            current_status="scanned",
            comment="Doesn't follow naming convention",
            rejected_by="admin@example.com",
            rejected_at="2026-05-22T10:00:00Z",
        )
        mock_db.sql_exec_with_params.assert_called_once()
        sql = mock_db.sql_exec_with_params.call_args[0][0]
        params = mock_db.sql_exec_with_params.call_args[0][1]
        # SQL hits all four columns + WHERE clause.
        assert "UPDATE test_catalog.test_schema.fastsetup_configs" in sql
        assert "status = 'rejected'" in sql
        assert "rejection_comment = :rejection_comment" in sql
        assert "rejected_by = :rejected_by" in sql
        assert "rejected_at = :rejected_at" in sql
        assert "WHERE dr_id = :dr_id" in sql
        assert params == {
            "dr_id": "DR-100",
            "current_status": "scanned",
            "rejection_comment": "Doesn't follow naming convention",
            "rejected_by": "admin@example.com",
            "rejected_at": "2026-05-22T10:00:00Z",
        }

    def test_reject_has_compare_and_set_status_guard(self, repo, mock_db):
        """finding #6: the reject UPDATE must gate on the expected current
        status so a concurrent state change can't be overwritten."""
        repo.reject(
            mock_db,
            dr_id="DR-100",
            current_status="valid",
            comment="x",
            rejected_by="admin@example.com",
            rejected_at="2026-05-22T10:00:00Z",
        )
        sql = mock_db.sql_exec_with_params.call_args[0][0]
        params = mock_db.sql_exec_with_params.call_args[0][1]
        assert "AND status = :current_status" in sql
        assert params["current_status"] == "valid"


class TestDelete:
    def test_delete_succeeds(self, repo, mock_db):
        mock_db.sql.return_value = [{"dr_id": "DR-100", "status": "valid"}]
        result = repo.delete(mock_db, dr_id="DR-100")
        assert result is True
        # sql_with_params called for get + sql_exec_with_params for delete
        mock_db.sql_exec_with_params.assert_called_once()

    def test_delete_blocked_when_provisioned(self, repo, mock_db):
        mock_db.sql.return_value = [{"dr_id": "DR-100", "status": "provisioned"}]
        result = repo.delete(mock_db, dr_id="DR-100")
        assert result is False
        mock_db.sql_exec_with_params.assert_not_called()

    def test_delete_returns_false_when_not_found(self, repo, mock_db):
        mock_db.sql.return_value = []
        result = repo.delete(mock_db, dr_id="DR-999")
        assert result is False
        mock_db.sql_exec_with_params.assert_not_called()
