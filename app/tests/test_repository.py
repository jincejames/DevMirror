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

        mock_db.sql_exec.assert_called_once()
        sql = mock_db.sql_exec.call_args[0][0]
        assert "INSERT INTO test_catalog.test_schema.devmirror_configs" in sql
        assert "DR-100" in sql
        assert "user@example.com" in sql
        assert "2026-05-01" in sql
        assert "Test config" in sql

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

        sql = mock_db.sql_exec.call_args[0][0]
        assert "NULL" in sql


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

        sql = mock_db.sql.call_args[0][0]
        assert "WHERE dr_id = 'DR-100'" in sql

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

        sql = mock_db.sql_exec.call_args[0][0]
        assert "UPDATE test_catalog.test_schema.devmirror_configs" in sql
        assert "WHERE dr_id = 'DR-100'" in sql
        assert "Updated" in sql


class TestDelete:
    def test_delete_succeeds(self, repo, mock_db):
        mock_db.sql.return_value = [{"dr_id": "DR-100", "status": "valid"}]
        result = repo.delete(mock_db, dr_id="DR-100")
        assert result is True
        # sql called for get + sql_exec for delete
        mock_db.sql_exec.assert_called_once()

    def test_delete_blocked_when_provisioned(self, repo, mock_db):
        mock_db.sql.return_value = [{"dr_id": "DR-100", "status": "provisioned"}]
        result = repo.delete(mock_db, dr_id="DR-100")
        assert result is False
        mock_db.sql_exec.assert_not_called()

    def test_delete_returns_false_when_not_found(self, repo, mock_db):
        mock_db.sql.return_value = []
        result = repo.delete(mock_db, dr_id="DR-999")
        assert result is False
        mock_db.sql_exec.assert_not_called()
