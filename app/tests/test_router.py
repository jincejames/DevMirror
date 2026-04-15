"""Endpoint tests with FastAPI TestClient and mocked dependencies."""

from __future__ import annotations

import json
from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest
from backend.config import get_current_user, get_db_client, get_settings
from backend.main import app
from fastapi.testclient import TestClient

from devmirror.settings import Settings


def _future_date(days: int = 30) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


def _valid_config_payload(**overrides) -> dict:
    defaults = {
        "dr_id": "DR-1042",
        "streams": ["my-job-1"],
        "developers": ["dev@example.com"],
        "expiration_date": _future_date(30),
    }
    defaults.update(overrides)
    return defaults


@pytest.fixture()
def mock_db():
    return MagicMock()


@pytest.fixture()
def settings():
    return Settings(
        warehouse_id="test-wh",
        control_catalog="test_catalog",
        control_schema="test_schema",
    )


@pytest.fixture()
def client(mock_db, settings):
    """Create a TestClient with mocked deps."""

    app.dependency_overrides[get_db_client] = lambda: mock_db
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_current_user] = lambda: "testuser@example.com"

    with TestClient(app, raise_server_exceptions=False) as c:
        yield c

    app.dependency_overrides.clear()


def _make_db_row(dr_id="DR-1042", status="valid", **overrides):
    """Build a fake DB row dict."""
    config_in_data = _valid_config_payload(dr_id=dr_id)
    row = {
        "dr_id": dr_id,
        "config_json": json.dumps(config_in_data),
        "config_yaml": "version: '1.0'\n",
        "status": status,
        "validation_errors": "[]",
        "created_at": "2026-04-01T00:00:00+00:00",
        "created_by": "testuser@example.com",
        "updated_at": None,
        "expiration_date": config_in_data["expiration_date"],
        "description": None,
    }
    row.update(overrides)
    return row


class TestHealthCheck:
    def test_health_returns_ok(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


class TestCreateConfig:
    def test_create_valid_config(self, client, mock_db):
        payload = _valid_config_payload()
        # After insert, the repo.get re-fetch returns the row
        mock_db.sql.return_value = [_make_db_row()]

        resp = client.post("/api/configs", json=payload)
        assert resp.status_code == 201
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["status"] == "valid"

    def test_create_invalid_dr_id(self, client, mock_db):
        payload = _valid_config_payload(dr_id="INVALID")
        # The repo.get re-fetch: row with invalid status
        mock_db.sql.return_value = [_make_db_row(dr_id="INVALID", status="invalid")]

        resp = client.post("/api/configs", json=payload)
        # Should still return 201 (stored as invalid), not 422
        assert resp.status_code == 201
        data = resp.json()
        assert data["status"] == "invalid"

    def test_create_with_empty_streams_422(self, client, mock_db):
        payload = _valid_config_payload(streams=[])
        resp = client.post("/api/configs", json=payload)
        assert resp.status_code == 422


class TestListConfigs:
    def test_list_returns_configs(self, client, mock_db):
        mock_db.sql.return_value = [
            _make_db_row(dr_id="DR-1"),
            _make_db_row(dr_id="DR-2"),
        ]
        resp = client.get("/api/configs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["configs"]) == 2


class TestGetConfig:
    def test_get_existing_config(self, client, mock_db):
        mock_db.sql.return_value = [_make_db_row()]
        resp = client.get("/api/configs/DR-1042")
        assert resp.status_code == 200
        assert resp.json()["dr_id"] == "DR-1042"

    def test_get_not_found(self, client, mock_db):
        mock_db.sql.return_value = []
        resp = client.get("/api/configs/DR-9999")
        assert resp.status_code == 404


class TestDeleteConfig:
    def test_delete_success(self, client, mock_db):
        mock_db.sql.return_value = [_make_db_row(status="valid")]
        resp = client.delete("/api/configs/DR-1042")
        assert resp.status_code == 204

    def test_delete_provisioned_409(self, client, mock_db):
        mock_db.sql.return_value = [_make_db_row(status="provisioned")]
        resp = client.delete("/api/configs/DR-1042")
        assert resp.status_code == 409

    def test_delete_not_found(self, client, mock_db):
        mock_db.sql.return_value = []
        resp = client.delete("/api/configs/DR-9999")
        assert resp.status_code == 404


class TestRevalidateConfig:
    def test_revalidate_updates_status(self, client, mock_db):
        row = _make_db_row(status="invalid")
        mock_db.sql.return_value = [row]

        resp = client.post("/api/configs/DR-1042/validate")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ("valid", "invalid")

    def test_revalidate_not_found(self, client, mock_db):
        mock_db.sql.return_value = []
        resp = client.post("/api/configs/DR-9999/validate")
        assert resp.status_code == 404


class TestExportYaml:
    def test_yaml_download(self, client, mock_db):
        row = _make_db_row()
        row["config_yaml"] = "version: '1.0'\ndevelopment_request:\n  dr_id: DR-1042\n"
        mock_db.sql.return_value = [row]

        resp = client.get("/api/configs/DR-1042/yaml")
        assert resp.status_code == 200
        assert "text/yaml" in resp.headers.get("content-type", "")
        assert "Content-Disposition" in resp.headers
        assert "DR-1042.yaml" in resp.headers["Content-Disposition"]
        assert "version:" in resp.text

    def test_yaml_not_found(self, client, mock_db):
        mock_db.sql.return_value = []
        resp = client.get("/api/configs/DR-9999/yaml")
        assert resp.status_code == 404
