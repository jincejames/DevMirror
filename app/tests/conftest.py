"""Shared test fixtures and helpers for DevMirror app tests."""

from __future__ import annotations

import json
from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest
from backend.auth import get_user_role
from backend.config import get_current_user, get_db_client, get_settings, get_task_tracker
from backend.main import app
from backend.tasks import TaskTracker
from fastapi.testclient import TestClient

from devmirror.settings import Settings


def future_date(days: int = 30) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


def valid_config_payload(**overrides) -> dict:
    """Build a minimal valid ConfigIn payload.

    By default ``dr_id`` is NOT included (US-34: the server auto-assigns
    it on create).  Callers that need a specific dr_id (e.g. PUT tests
    reading from ``make_db_row``) can pass it explicitly as an override.
    """
    defaults = {
        "streams": ["my-job-1"],
        "developers": ["dev@example.com"],
        "expiration_date": future_date(30),
    }
    defaults.update(overrides)
    return defaults


def make_db_row(dr_id="DR-1042", status="valid", **overrides) -> dict:
    """Build a fake DB row dict for devmirror_configs."""
    config_in_data = valid_config_payload(dr_id=dr_id)
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
        "manifest_json": None,
        "scanned_at": None,
    }
    row.update(overrides)
    return row


def make_dr_control_row(dr_id="DR-1042", status="ACTIVE", **overrides) -> dict:
    """Build a fake control-table DR row."""
    row = {
        "dr_id": dr_id,
        "status": status,
        "expiration_date": future_date(30),
    }
    row.update(overrides)
    return row


def mock_scan_pipeline(mock_resolve, mock_lineage, mock_classify, mock_build_manifest, mock_table_sizes) -> dict:
    """Wire up the standard scan-pipeline mocks and return the manifest dict."""
    resolved_stream = MagicMock()
    resolved_stream.name = "my-job-1"
    resolved_stream.resource_id = "12345"
    resolved_stream.resource_type = "job"
    resolved_stream.task_keys = []
    mock_resolve.return_value = ([resolved_stream], [])

    lineage_result = MagicMock()
    lineage_result.edges = []
    lineage_result.row_limit_hit = False
    mock_lineage.return_value = lineage_result

    classification = MagicMock()
    classification.objects = []
    classification.review_required = False
    mock_classify.return_value = classification

    mock_table_sizes.return_value = {}

    manifest = {
        "scan_result": {
            "dr_id": "DR-1042",
            "scanned_at": "2026-04-15T10:00:00+00:00",
            "streams_scanned": [{"name": "my-job-1", "workflow_id": "12345"}],
            "objects": [],
            "schemas_required": [],
            "total_objects": 0,
            "review_required": False,
        }
    }
    mock_build_manifest.return_value = manifest
    return manifest


def mock_provision_result():
    """Build a standard provision result mock."""
    mock_result = MagicMock()
    mock_result.final_status = "ACTIVE"
    mock_result.objects_succeeded = []
    mock_result.objects_failed = []
    mock_result.schemas_created = []
    mock_result.grants_applied = 0
    return mock_result


def mock_refresh_result(mode="incremental"):
    """Build a standard refresh result mock."""
    mock_result = MagicMock()
    mock_result.audit_status = "SUCCESS"
    mock_result.mode = mode
    mock_result.objects_succeeded = []
    mock_result.objects_failed = []
    return mock_result


@pytest.fixture()
def mock_db():
    db = MagicMock()
    # Wire sql_with_params to delegate to sql so that tests setting
    # mock_db.sql.return_value / side_effect still work after the
    # repository switched from sql() to sql_with_params().
    db.sql_with_params.side_effect = lambda stmt, params: db.sql(stmt, params)
    return db


@pytest.fixture(autouse=True)
def mock_next_dr_id(monkeypatch):
    """Pin next_dr_id to ``"DR-1042"`` (legacy format) for all tests.

    US-34 moved DR-ID generation server-side; existing test fixtures still
    expect the dr_id ``"DR-1042"`` (the value baked into :func:`make_db_row`).
    Monkey-patching the generator keeps those tests unchanged and makes the
    create-config path deterministic without requiring the counter table.
    """
    from backend import router as router_module

    monkeypatch.setattr(
        router_module, "next_dr_id", lambda db_client, settings: "DR-1042", raising=False
    )
    # Also patch on the source module in case the router imports it lazily.
    from devmirror.utils import id_generator as id_generator_module

    monkeypatch.setattr(
        id_generator_module, "next_dr_id", lambda db_client, settings: "DR-1042"
    )


@pytest.fixture()
def settings():
    return Settings(
        warehouse_id="test-wh",
        control_catalog="test_catalog",
        control_schema="test_schema",
    )


@pytest.fixture()
def task_tracker():
    return TaskTracker()


@pytest.fixture()
def client(mock_db, settings, task_tracker):
    """Create a TestClient with all deps mocked (including task_tracker).

    The ``get_user_role`` dependency is overridden to return ``"admin"`` so
    that existing tests continue to work unchanged.
    """
    app.dependency_overrides[get_db_client] = lambda: mock_db
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_current_user] = lambda: "testuser@example.com"
    app.dependency_overrides[get_task_tracker] = lambda: task_tracker
    app.dependency_overrides[get_user_role] = lambda: "admin"

    with TestClient(app, raise_server_exceptions=False) as c:
        yield c

    app.dependency_overrides.clear()


@pytest.fixture()
def user_client(mock_db, settings, task_tracker):
    """TestClient where the caller has role ``"user"`` (not admin)."""
    app.dependency_overrides[get_db_client] = lambda: mock_db
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_current_user] = lambda: "testuser@example.com"
    app.dependency_overrides[get_task_tracker] = lambda: task_tracker
    app.dependency_overrides[get_user_role] = lambda: "user"

    with TestClient(app, raise_server_exceptions=False) as c:
        yield c

    app.dependency_overrides.clear()


def make_client(role: str = "admin", email: str = "testuser@example.com"):
    """Factory: create a TestClient with a specific role and email.

    Uses a fresh ``MagicMock`` for ``db_client`` and returns
    ``(TestClient, mock_db)`` so callers can set up DB expectations.
    """
    mock_db = MagicMock()
    mock_db.sql_with_params.side_effect = lambda stmt, params: mock_db.sql(stmt, params)
    _settings = Settings(
        warehouse_id="test-wh",
        control_catalog="test_catalog",
        control_schema="test_schema",
    )
    _tracker = TaskTracker()

    app.dependency_overrides[get_db_client] = lambda: mock_db
    app.dependency_overrides[get_settings] = lambda: _settings
    app.dependency_overrides[get_current_user] = lambda: email
    app.dependency_overrides[get_task_tracker] = lambda: _tracker
    app.dependency_overrides[get_user_role] = lambda: role

    return TestClient(app, raise_server_exceptions=False), mock_db
