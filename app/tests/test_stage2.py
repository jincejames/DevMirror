"""Stage 2 endpoint tests: scan, provision, task status, DR status/list, cleanup, refresh, reprovision."""

from __future__ import annotations

import json
import time
from unittest.mock import MagicMock, patch

import pytest
from backend.tasks import TaskTracker

from .conftest import (
    make_db_row,
    make_dr_control_row,
    mock_provision_result,
    mock_refresh_result,
    mock_scan_pipeline,
    valid_config_payload,
)

# Decorator stacks for scan-pipeline patches (avoids repetition)
_SCAN_PATCHES = [
    "devmirror.scan.lineage.query_table_sizes",
    "devmirror.scan.manifest.build_manifest",
    "devmirror.scan.dependency_classifier.classify_dependencies",
    "devmirror.scan.lineage.query_lineage",
    "devmirror.scan.stream_resolver.resolve_streams",
]

# Decorator stacks for control-repo patches (DR status / list / refresh)
_CONTROL_REPO_PATCHES = [
    "devmirror.control.audit.AuditRepository",
    "devmirror.control.control_table.DrObjectRepository",
    "devmirror.control.control_table.DRRepository",
]


# ---- Scan endpoint tests ----


class TestScanConfig:
    @patch(_SCAN_PATCHES[0])
    @patch(_SCAN_PATCHES[1])
    @patch(_SCAN_PATCHES[2])
    @patch(_SCAN_PATCHES[3])
    @patch(_SCAN_PATCHES[4])
    def test_scan_success(
        self, mock_resolve, mock_lineage, mock_classify, mock_build_manifest, mock_table_sizes,
        client, mock_db,
    ):
        mock_db.sql.return_value = [make_db_row(status="valid")]
        mock_db.client = MagicMock()
        mock_scan_pipeline(mock_resolve, mock_lineage, mock_classify, mock_build_manifest, mock_table_sizes)

        resp = client.post("/api/configs/DR-1042/scan")
        assert resp.status_code == 200
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["status"] == "scanned"
        assert "scan_result" in data["manifest"]

    @patch("devmirror.scan.stream_resolver.resolve_streams")
    def test_scan_unresolved_streams(self, mock_resolve, client, mock_db):
        mock_db.sql.return_value = [make_db_row()]
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
        row = make_db_row(manifest_json=json.dumps(manifest), scanned_at="2026-04-15T10:00:00+00:00")
        mock_db.sql.return_value = [row]

        resp = client.get("/api/configs/DR-1042/manifest")
        assert resp.status_code == 200
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert "scan_result" in data["manifest"]
        assert data["scanned_at"] == "2026-04-15T10:00:00+00:00"

    @pytest.mark.parametrize("rows,expected_status", [
        ([make_db_row(manifest_json=None)], 404),   # not scanned
        ([], 404),                                   # config not found
    ])
    def test_get_manifest_not_found(self, client, mock_db, rows, expected_status):
        mock_db.sql.return_value = rows
        resp = client.get("/api/configs/DR-1042/manifest")
        assert resp.status_code == expected_status


class TestUpdateManifest:
    def test_update_manifest_success(self, client, mock_db):
        mock_db.sql.return_value = [make_db_row()]
        new_manifest = {"scan_result": {"dr_id": "DR-1042", "objects": [{"fqn": "a.b.c"}]}}

        resp = client.put("/api/configs/DR-1042/manifest", json=new_manifest)
        assert resp.status_code == 200
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["manifest"] == new_manifest
        assert data["scanned_at"] is not None

    def test_update_manifest_invalid_structure(self, client, mock_db):
        mock_db.sql.return_value = [make_db_row()]
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
        row = make_db_row(status="scanned", manifest_json=json.dumps(manifest))
        mock_db.sql.return_value = [row]
        mock_prov.return_value = mock_provision_result()

        resp = client.post("/api/configs/DR-1042/provision")
        assert resp.status_code == 202
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["task_id"].startswith("task-")
        assert data["status"] == "provisioning"
        assert "Poll GET" in data["message"]

    def test_provision_no_manifest_400(self, client, mock_db):
        mock_db.sql.return_value = [make_db_row(manifest_json=None)]
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
        task_id = task_tracker.submit("DR-1042", "provision", lambda: {"done": True})
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
        task_id = task_tracker.submit("DR-1042", "provision", lambda: (_ for _ in ()).throw(RuntimeError("something broke")))
        time.sleep(0.1)

        resp = client.get(f"/api/tasks/{task_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "failed"
        assert "something broke" in data["error"]


# ---- DR status endpoint tests ----


class TestGetDrStatus:
    @patch(_CONTROL_REPO_PATCHES[0])
    @patch(_CONTROL_REPO_PATCHES[1])
    @patch(_CONTROL_REPO_PATCHES[2])
    def test_dr_status_success(self, MockDRRepo, MockObjRepo, MockAuditRepo, client, mock_db):
        dr_repo = MockDRRepo.return_value
        dr_repo.get.return_value = {
            "dr_id": "DR-1042", "status": "ACTIVE", "description": "Test DR",
            "expiration_date": "2026-06-01", "created_at": "2026-04-01T00:00:00",
            "last_refreshed_at": None,
        }
        MockObjRepo.return_value.list_by_dr_id.return_value = [
            {"source_fqn": "prod.schema.t1", "target_fqn": "dev.dr_1042.t1", "status": "PROVISIONED"},
            {"source_fqn": "prod.schema.t2", "target_fqn": "dev.dr_1042.t2", "status": "PROVISIONED"},
        ]
        MockAuditRepo.return_value.list_by_dr_id.return_value = [
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
        MockDRRepo.return_value.get.return_value = None
        resp = client.get("/api/drs/DR-9999/status")
        assert resp.status_code == 404


# ---- DR list endpoint tests ----


class TestListDrs:
    @patch("devmirror.control.control_table.DrObjectRepository")
    @patch("devmirror.control.control_table.DRRepository")
    def test_list_drs(self, MockDRRepo, MockObjRepo, client, mock_db):
        MockDRRepo.return_value.list_active.return_value = [{
            "dr_id": "DR-1042", "status": "ACTIVE", "description": "Test",
            "expiration_date": "2026-06-01", "created_at": "2026-04-01T00:00:00",
            "created_by": "dev@example.com",
        }]

        resp = client.get("/api/drs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert data["drs"][0]["dr_id"] == "DR-1042"
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
        assert data["final_status"] == "CLEANED_UP"
        assert data["objects_dropped"] == 5

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
        task_id = tracker.submit("DR-1", "provision", lambda: (_ for _ in ()).throw(ValueError("test error")))
        time.sleep(0.1)
        task = tracker.get(task_id)
        assert task is not None
        assert task.status == "failed"
        assert task.error == "test error"
        assert task.completed_at is not None

    def test_get_unknown_returns_none(self):
        assert TaskTracker().get("task-nonexistent") is None

    def test_list_for_dr(self):
        tracker = TaskTracker()
        tracker.submit("DR-1", "scan", lambda: None)
        tracker.submit("DR-1", "provision", lambda: None)
        tracker.submit("DR-2", "scan", lambda: None)
        time.sleep(0.1)
        assert len(tracker.list_for_dr("DR-1")) == 2
        assert len(tracker.list_for_dr("DR-2")) == 1


# ---- Refresh endpoint tests ----


class TestRefreshDr:
    @patch(_CONTROL_REPO_PATCHES[0])
    @patch(_CONTROL_REPO_PATCHES[1])
    @patch(_CONTROL_REPO_PATCHES[2])
    @patch("devmirror.refresh.refresh_engine.refresh_dr")
    def test_refresh_returns_202(self, mock_refresh, MockDRRepo, MockObjRepo, MockAuditRepo, client, mock_db):
        MockDRRepo.return_value.get.return_value = make_dr_control_row()
        result = mock_refresh_result()
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
        MockDRRepo.return_value.get.return_value = make_dr_control_row(status="CLEANED_UP")
        resp = client.post("/api/drs/DR-1042/refresh", json={"mode": "incremental"})
        assert resp.status_code == 409

    @patch(_CONTROL_REPO_PATCHES[0])
    @patch(_CONTROL_REPO_PATCHES[1])
    @patch(_CONTROL_REPO_PATCHES[2])
    @patch("devmirror.refresh.refresh_engine.refresh_dr")
    def test_refresh_selective_mode(self, mock_refresh, MockDRRepo, MockObjRepo, MockAuditRepo, client, mock_db):
        MockDRRepo.return_value.get.return_value = make_dr_control_row(status="EXPIRING_SOON")
        mock_refresh.return_value = mock_refresh_result(mode="selective")

        resp = client.post(
            "/api/drs/DR-1042/refresh",
            json={"mode": "selective", "selected_objects": ["prod.schema.table1"]},
        )
        assert resp.status_code == 202

    @patch(_CONTROL_REPO_PATCHES[0])
    @patch(_CONTROL_REPO_PATCHES[1])
    @patch(_CONTROL_REPO_PATCHES[2])
    @patch("devmirror.refresh.refresh_engine.refresh_dr")
    def test_refresh_default_mode(self, mock_refresh, MockDRRepo, MockObjRepo, MockAuditRepo, client, mock_db):
        """Test that the default mode (no body) uses incremental."""
        MockDRRepo.return_value.get.return_value = make_dr_control_row()
        mock_refresh.return_value = mock_refresh_result()

        resp = client.post("/api/drs/DR-1042/refresh")
        assert resp.status_code == 202
        assert "incremental" in resp.json()["message"]


# ---- Re-provision endpoint tests ----


class TestReprovisionDr:
    @patch(_SCAN_PATCHES[0])
    @patch(_SCAN_PATCHES[1])
    @patch(_SCAN_PATCHES[2])
    @patch(_SCAN_PATCHES[3])
    @patch(_SCAN_PATCHES[4])
    @patch("devmirror.provision.runner.provision_dr")
    @patch("devmirror.control.control_table.DRRepository")
    def test_reprovision_returns_202(
        self, MockDRRepo, mock_provision,
        mock_resolve, mock_lineage, mock_classify, mock_build_manifest, mock_table_sizes,
        client, mock_db,
    ):
        mock_db.sql.return_value = [make_db_row(status="provisioned")]
        mock_db.client = MagicMock()
        MockDRRepo.return_value.get.return_value = make_dr_control_row()
        mock_provision.return_value = mock_provision_result()
        mock_scan_pipeline(mock_resolve, mock_lineage, mock_classify, mock_build_manifest, mock_table_sizes)

        resp = client.post("/api/drs/DR-1042/reprovision")
        assert resp.status_code == 202
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["task_id"].startswith("task-")
        assert data["status"] == "reprovisioning"

    @pytest.mark.parametrize("config_rows,dr_row,expected_status", [
        ([], None, 404),                                                    # config not found
        ([make_db_row(status="provisioned")], None, 404),                   # DR not in control table
        ([make_db_row(status="provisioned")], make_dr_control_row(status="CLEANED_UP"), 409),  # wrong DR status
    ])
    @patch("devmirror.control.control_table.DRRepository")
    def test_reprovision_failures(self, MockDRRepo, client, mock_db, config_rows, dr_row, expected_status):
        mock_db.sql.return_value = config_rows
        MockDRRepo.return_value.get.return_value = dr_row
        resp = client.post("/api/drs/DR-1042/reprovision")
        assert resp.status_code == expected_status


# ---- Update config for provisioned configs ----


class TestUpdateProvisionedConfig:
    def test_update_provisioned_config_allowed(self, client, mock_db):
        """Updating a provisioned config should keep the provisioned status."""
        mock_db.sql.return_value = [make_db_row(status="provisioned")]

        payload = valid_config_payload(description="Updated description")
        resp = client.put("/api/configs/DR-1042", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["status"] == "provisioned"
