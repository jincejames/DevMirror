"""Stage 2 endpoint tests: scan, provision, task status, DR status/list, cleanup."""

from __future__ import annotations

import json
import time
from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest
from backend.config import get_current_user, get_db_client, get_settings, get_task_tracker
from backend.main import app
from backend.tasks import TaskTracker
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


def _make_db_row(dr_id="DR-1042", status="valid", **overrides):
    """Build a fake DB row dict for devmirror_configs."""
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
        "manifest_json": None,
        "scanned_at": None,
    }
    row.update(overrides)
    return row


def _make_dr_control_row(dr_id="DR-1042", status="ACTIVE", **overrides):
    """Build a fake control-table DR row."""
    row = {
        "dr_id": dr_id,
        "status": status,
        "expiration_date": _future_date(30),
    }
    row.update(overrides)
    return row


def _mock_scan_pipeline(mock_resolve, mock_lineage, mock_classify, mock_build_manifest, mock_table_sizes):
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


def _mock_provision_result():
    """Build a standard provision result mock."""
    mock_result = MagicMock()
    mock_result.final_status = "ACTIVE"
    mock_result.objects_succeeded = []
    mock_result.objects_failed = []
    mock_result.schemas_created = []
    mock_result.grants_applied = 0
    return mock_result


def _mock_refresh_result(mode="incremental"):
    """Build a standard refresh result mock."""
    mock_result = MagicMock()
    mock_result.audit_status = "SUCCESS"
    mock_result.mode = mode
    mock_result.objects_succeeded = []
    mock_result.objects_failed = []
    return mock_result


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
def task_tracker():
    return TaskTracker()


@pytest.fixture()
def client(mock_db, settings, task_tracker):
    """Create a TestClient with mocked deps."""
    app.dependency_overrides[get_db_client] = lambda: mock_db
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[get_current_user] = lambda: "testuser@example.com"
    app.dependency_overrides[get_task_tracker] = lambda: task_tracker

    with TestClient(app, raise_server_exceptions=False) as c:
        yield c

    app.dependency_overrides.clear()


# ---- Scan endpoint tests ----


class TestScanConfig:
    @patch("devmirror.scan.lineage.query_table_sizes")
    @patch("devmirror.scan.manifest.build_manifest")
    @patch("devmirror.scan.dependency_classifier.classify_dependencies")
    @patch("devmirror.scan.lineage.query_lineage")
    @patch("devmirror.scan.stream_resolver.resolve_streams")
    def test_scan_success(
        self,
        mock_resolve,
        mock_lineage,
        mock_classify,
        mock_build_manifest,
        mock_table_sizes,
        client,
        mock_db,
    ):
        mock_db.sql.return_value = [_make_db_row(status="valid")]
        mock_db.client = MagicMock()
        _mock_scan_pipeline(mock_resolve, mock_lineage, mock_classify, mock_build_manifest, mock_table_sizes)

        resp = client.post("/api/configs/DR-1042/scan")
        assert resp.status_code == 200
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["status"] == "scanned"
        assert "scan_result" in data["manifest"]

    @patch("devmirror.scan.stream_resolver.resolve_streams")
    def test_scan_unresolved_streams(self, mock_resolve, client, mock_db):
        mock_db.sql.return_value = [_make_db_row()]
        mock_db.client = MagicMock()
        mock_resolve.return_value = ([], ["bad-stream"])

        resp = client.post("/api/configs/DR-1042/scan")
        assert resp.status_code == 400
        assert "Unresolved streams" in resp.json()["detail"]

    def test_scan_config_not_found(self, client, mock_db):
        mock_db.sql.return_value = []
        resp = client.post("/api/configs/DR-9999/scan")
        assert resp.status_code == 404


# ---- Manifest endpoint tests ----


class TestGetManifest:
    def test_get_manifest_success(self, client, mock_db):
        manifest = {"scan_result": {"dr_id": "DR-1042", "objects": []}}
        row = _make_db_row(
            manifest_json=json.dumps(manifest),
            scanned_at="2026-04-15T10:00:00+00:00",
        )
        mock_db.sql.return_value = [row]

        resp = client.get("/api/configs/DR-1042/manifest")
        assert resp.status_code == 200
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert "scan_result" in data["manifest"]
        assert data["scanned_at"] == "2026-04-15T10:00:00+00:00"

    def test_get_manifest_not_scanned(self, client, mock_db):
        row = _make_db_row(manifest_json=None)
        mock_db.sql.return_value = [row]

        resp = client.get("/api/configs/DR-1042/manifest")
        assert resp.status_code == 404
        assert "No manifest" in resp.json()["detail"]

    def test_get_manifest_config_not_found(self, client, mock_db):
        mock_db.sql.return_value = []
        resp = client.get("/api/configs/DR-9999/manifest")
        assert resp.status_code == 404


class TestUpdateManifest:
    def test_update_manifest_success(self, client, mock_db):
        mock_db.sql.return_value = [_make_db_row()]
        new_manifest = {"scan_result": {"dr_id": "DR-1042", "objects": [{"fqn": "a.b.c"}]}}

        resp = client.put("/api/configs/DR-1042/manifest", json=new_manifest)
        assert resp.status_code == 200
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["manifest"] == new_manifest
        assert data["scanned_at"] is not None

    def test_update_manifest_invalid_structure(self, client, mock_db):
        mock_db.sql.return_value = [_make_db_row()]
        resp = client.put("/api/configs/DR-1042/manifest", json={"data": "test"})
        assert resp.status_code == 400
        assert "scan_result.objects" in resp.json()["detail"]

    def test_update_manifest_not_found(self, client, mock_db):
        mock_db.sql.return_value = []
        resp = client.put("/api/configs/DR-9999/manifest", json={"scan_result": {"objects": []}})
        assert resp.status_code == 404


# ---- Provision endpoint tests ----


class TestProvisionConfig:
    @patch("devmirror.provision.runner.provision_dr")
    def test_provision_returns_202_with_task_id(self, mock_prov, client, mock_db):
        manifest = {"scan_result": {"dr_id": "DR-1042", "objects": []}}
        row = _make_db_row(status="scanned", manifest_json=json.dumps(manifest))
        mock_db.sql.return_value = [row]
        mock_prov.return_value = _mock_provision_result()

        resp = client.post("/api/configs/DR-1042/provision")
        assert resp.status_code == 202
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["task_id"].startswith("task-")
        assert data["status"] == "provisioning"
        assert "Poll GET" in data["message"]

    def test_provision_no_manifest_400(self, client, mock_db):
        row = _make_db_row(manifest_json=None)
        mock_db.sql.return_value = [row]

        resp = client.post("/api/configs/DR-1042/provision")
        assert resp.status_code == 400
        assert "No manifest" in resp.json()["detail"]

    def test_provision_not_found(self, client, mock_db):
        mock_db.sql.return_value = []
        resp = client.post("/api/configs/DR-9999/provision")
        assert resp.status_code == 404


# ---- Task status endpoint tests ----


class TestGetTaskStatus:
    def test_get_task_status_found(self, client, task_tracker):
        # Submit a quick task
        task_id = task_tracker.submit("DR-1042", "provision", lambda: {"done": True})
        # Wait briefly for it to complete
        time.sleep(0.1)

        resp = client.get(f"/api/tasks/{task_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["task_id"] == task_id
        assert data["dr_id"] == "DR-1042"
        assert data["task_type"] == "provision"
        assert data["status"] == "completed"
        assert data["result"] == {"done": True}

    def test_get_task_status_not_found(self, client):
        resp = client.get("/api/tasks/task-doesnotexist")
        assert resp.status_code == 404

    def test_get_task_status_failed(self, client, task_tracker):
        def fail():
            raise RuntimeError("something broke")

        task_id = task_tracker.submit("DR-1042", "provision", fail)
        time.sleep(0.1)

        resp = client.get(f"/api/tasks/{task_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "failed"
        assert "something broke" in data["error"]


# ---- DR status endpoint tests ----


class TestGetDrStatus:
    @patch("devmirror.control.audit.AuditRepository")
    @patch("devmirror.control.control_table.DrObjectRepository")
    @patch("devmirror.control.control_table.DRRepository")
    def test_dr_status_success(
        self, MockDRRepo, MockObjRepo, MockAuditRepo, client, mock_db
    ):
        dr_repo = MockDRRepo.return_value
        dr_repo.get.return_value = {
            "dr_id": "DR-1042",
            "status": "ACTIVE",
            "description": "Test DR",
            "expiration_date": "2026-06-01",
            "created_at": "2026-04-01T00:00:00",
            "last_refreshed_at": None,
        }

        obj_repo = MockObjRepo.return_value
        obj_repo.list_by_dr_id.return_value = [
            {"source_fqn": "prod.schema.t1", "target_fqn": "dev.dr_1042.t1", "status": "PROVISIONED"},
            {"source_fqn": "prod.schema.t2", "target_fqn": "dev.dr_1042.t2", "status": "PROVISIONED"},
        ]

        audit_repo = MockAuditRepo.return_value
        audit_repo.list_by_dr_id.return_value = [
            {"action": "PROVISION", "status": "SUCCESS", "performed_at": "2026-04-01T10:00:00"},
        ]

        resp = client.get("/api/drs/DR-1042/status")
        assert resp.status_code == 200
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["status"] == "ACTIVE"
        assert data["total_objects"] == 2
        assert data["object_breakdown"]["PROVISIONED"] == 2
        assert len(data["recent_audit"]) == 1

    @patch("devmirror.control.control_table.DRRepository")
    def test_dr_status_not_found(self, MockDRRepo, client, mock_db):
        dr_repo = MockDRRepo.return_value
        dr_repo.get.return_value = None

        resp = client.get("/api/drs/DR-9999/status")
        assert resp.status_code == 404


# ---- DR list endpoint tests ----


class TestListDrs:
    @patch("devmirror.control.control_table.DrObjectRepository")
    @patch("devmirror.control.control_table.DRRepository")
    def test_list_drs(self, MockDRRepo, MockObjRepo, client, mock_db):
        dr_repo = MockDRRepo.return_value
        dr_repo.list_active.return_value = [
            {
                "dr_id": "DR-1042",
                "status": "ACTIVE",
                "description": "Test",
                "expiration_date": "2026-06-01",
                "created_at": "2026-04-01T00:00:00",
                "created_by": "dev@example.com",
            },
        ]

        resp = client.get("/api/drs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["drs"][0]["dr_id"] == "DR-1042"
        # N+1 fix: list endpoint no longer queries objects per DR
        assert data["drs"][0]["total_objects"] == 0


# ---- Cleanup endpoint tests ----


class TestCleanupDr:
    @patch("devmirror.cleanup.cleanup_engine.cleanup_dr")
    def test_cleanup_success(self, mock_cleanup, client, mock_db):
        mock_result = MagicMock()
        mock_result.final_status = "CLEANED_UP"
        mock_result.objects_dropped = 5
        mock_result.schemas_dropped = 2
        mock_result.revokes_succeeded = 4
        mock_cleanup.return_value = mock_result

        resp = client.post("/api/drs/DR-1042/cleanup")
        assert resp.status_code == 200
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["final_status"] == "CLEANED_UP"
        assert data["objects_dropped"] == 5
        assert data["schemas_dropped"] == 2
        assert data["revokes_succeeded"] == 4

    @patch("devmirror.cleanup.cleanup_engine.cleanup_dr")
    def test_cleanup_not_found(self, mock_cleanup, client, mock_db):
        mock_result = MagicMock()
        mock_result.final_status = "NOT_FOUND"
        mock_cleanup.return_value = mock_result

        resp = client.post("/api/drs/DR-9999/cleanup")
        assert resp.status_code == 404


# ---- TaskTracker unit tests ----


class TestTaskTracker:
    def test_submit_and_complete(self):
        tracker = TaskTracker()
        task_id = tracker.submit("DR-1", "scan", lambda: {"result": "ok"})
        assert task_id.startswith("task-")

        time.sleep(0.1)
        task = tracker.get(task_id)
        assert task is not None
        assert task.status == "completed"
        assert task.result == {"result": "ok"}
        assert task.completed_at is not None

    def test_submit_and_fail(self):
        tracker = TaskTracker()

        def fail():
            raise ValueError("test error")

        task_id = tracker.submit("DR-1", "provision", fail)
        time.sleep(0.1)

        task = tracker.get(task_id)
        assert task is not None
        assert task.status == "failed"
        assert task.error == "test error"
        assert task.completed_at is not None

    def test_get_unknown_returns_none(self):
        tracker = TaskTracker()
        assert tracker.get("task-nonexistent") is None

    def test_list_for_dr(self):
        tracker = TaskTracker()
        tracker.submit("DR-1", "scan", lambda: None)
        tracker.submit("DR-1", "provision", lambda: None)
        tracker.submit("DR-2", "scan", lambda: None)

        time.sleep(0.1)
        dr1_tasks = tracker.list_for_dr("DR-1")
        assert len(dr1_tasks) == 2
        assert all(t.dr_id == "DR-1" for t in dr1_tasks)

        dr2_tasks = tracker.list_for_dr("DR-2")
        assert len(dr2_tasks) == 1


# ---- Refresh endpoint tests ----


class TestRefreshDr:
    @patch("devmirror.control.audit.AuditRepository")
    @patch("devmirror.control.control_table.DrObjectRepository")
    @patch("devmirror.control.control_table.DRRepository")
    @patch("devmirror.refresh.refresh_engine.refresh_dr")
    def test_refresh_returns_202(
        self, mock_refresh, MockDRRepo, MockObjRepo, MockAuditRepo, client, mock_db
    ):
        MockDRRepo.return_value.get.return_value = _make_dr_control_row()
        result = _mock_refresh_result()
        result.objects_succeeded = [MagicMock()]
        mock_refresh.return_value = result

        resp = client.post("/api/drs/DR-1042/refresh", json={"mode": "incremental"})
        assert resp.status_code == 202
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["task_id"].startswith("task-")
        assert data["status"] == "refreshing"

    @patch("devmirror.control.control_table.DRRepository")
    def test_refresh_not_found(self, MockDRRepo, client, mock_db):
        MockDRRepo.return_value.get.return_value = None
        resp = client.post("/api/drs/DR-9999/refresh", json={"mode": "full"})
        assert resp.status_code == 404

    @patch("devmirror.control.control_table.DRRepository")
    def test_refresh_wrong_status(self, MockDRRepo, client, mock_db):
        MockDRRepo.return_value.get.return_value = _make_dr_control_row(status="CLEANED_UP")
        resp = client.post("/api/drs/DR-1042/refresh", json={"mode": "incremental"})
        assert resp.status_code == 409

    @patch("devmirror.control.audit.AuditRepository")
    @patch("devmirror.control.control_table.DrObjectRepository")
    @patch("devmirror.control.control_table.DRRepository")
    @patch("devmirror.refresh.refresh_engine.refresh_dr")
    def test_refresh_selective_mode(
        self, mock_refresh, MockDRRepo, MockObjRepo, MockAuditRepo, client, mock_db
    ):
        MockDRRepo.return_value.get.return_value = _make_dr_control_row(status="EXPIRING_SOON")
        mock_refresh.return_value = _mock_refresh_result(mode="selective")

        resp = client.post(
            "/api/drs/DR-1042/refresh",
            json={"mode": "selective", "selected_objects": ["prod.schema.table1"]},
        )
        assert resp.status_code == 202

    @patch("devmirror.control.audit.AuditRepository")
    @patch("devmirror.control.control_table.DrObjectRepository")
    @patch("devmirror.control.control_table.DRRepository")
    @patch("devmirror.refresh.refresh_engine.refresh_dr")
    def test_refresh_default_mode(
        self, mock_refresh, MockDRRepo, MockObjRepo, MockAuditRepo, client, mock_db
    ):
        """Test that the default mode (no body) uses incremental."""
        MockDRRepo.return_value.get.return_value = _make_dr_control_row()
        mock_refresh.return_value = _mock_refresh_result()

        resp = client.post("/api/drs/DR-1042/refresh")
        assert resp.status_code == 202
        data = resp.json()
        assert "incremental" in data["message"]


# ---- Re-provision endpoint tests ----


class TestReprovisionDr:
    @patch("devmirror.scan.lineage.query_table_sizes")
    @patch("devmirror.scan.manifest.build_manifest")
    @patch("devmirror.scan.dependency_classifier.classify_dependencies")
    @patch("devmirror.scan.lineage.query_lineage")
    @patch("devmirror.scan.stream_resolver.resolve_streams")
    @patch("devmirror.provision.runner.provision_dr")
    @patch("devmirror.control.control_table.DRRepository")
    def test_reprovision_returns_202(
        self,
        MockDRRepo,
        mock_provision,
        mock_resolve,
        mock_lineage,
        mock_classify,
        mock_build_manifest,
        mock_table_sizes,
        client,
        mock_db,
    ):
        mock_db.sql.return_value = [_make_db_row(status="provisioned")]
        mock_db.client = MagicMock()
        MockDRRepo.return_value.get.return_value = _make_dr_control_row()
        mock_provision.return_value = _mock_provision_result()
        _mock_scan_pipeline(mock_resolve, mock_lineage, mock_classify, mock_build_manifest, mock_table_sizes)

        resp = client.post("/api/drs/DR-1042/reprovision")
        assert resp.status_code == 202
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["task_id"].startswith("task-")
        assert data["status"] == "reprovisioning"

    @patch("devmirror.control.control_table.DRRepository")
    def test_reprovision_config_not_found(self, MockDRRepo, client, mock_db):
        mock_db.sql.return_value = []
        resp = client.post("/api/drs/DR-9999/reprovision")
        assert resp.status_code == 404

    @patch("devmirror.control.control_table.DRRepository")
    def test_reprovision_wrong_dr_status(self, MockDRRepo, client, mock_db):
        mock_db.sql.return_value = [_make_db_row(status="provisioned")]
        MockDRRepo.return_value.get.return_value = _make_dr_control_row(status="CLEANED_UP")
        resp = client.post("/api/drs/DR-1042/reprovision")
        assert resp.status_code == 409

    @patch("devmirror.control.control_table.DRRepository")
    def test_reprovision_dr_not_in_control_table(self, MockDRRepo, client, mock_db):
        mock_db.sql.return_value = [_make_db_row(status="provisioned")]
        MockDRRepo.return_value.get.return_value = None
        resp = client.post("/api/drs/DR-1042/reprovision")
        assert resp.status_code == 404


# ---- Update config for provisioned configs ----


class TestUpdateProvisionedConfig:
    def test_update_provisioned_config_allowed(self, client, mock_db):
        """Updating a provisioned config should now be allowed."""
        row = _make_db_row(status="provisioned")
        mock_db.sql.return_value = [row]

        payload = _valid_config_payload(description="Updated description")
        resp = client.put("/api/configs/DR-1042", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        # The status should remain provisioned after updating
        assert data["status"] == "provisioned"
