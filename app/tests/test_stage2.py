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

    @patch("devmirror.scan.lineage.query_table_sizes")
    @patch("devmirror.scan.dependency_classifier.classify_dependencies")
    @patch("devmirror.scan.lineage.query_lineage")
    @patch("devmirror.scan.stream_resolver.resolve_streams")
    def test_scan_flags_non_prod_additional_objects(
        self, mock_resolve, mock_lineage, mock_classify, mock_table_sizes,
        client, mock_db,
    ):
        """An additional_object whose catalog differs from the streams' baseline
        catalog must surface in manifest.scan_result.non_prod_additional_objects."""
        from devmirror.scan.dependency_classifier import (
            ClassificationResult,
            ClassifiedObject,
        )

        # Config has additional_objects from a non-prod catalog.
        config_in = valid_config_payload(
            dr_id="DR-1042",
            additional_objects=["dev_analytics.scratch.foo"],
        )
        row = make_db_row(
            status="valid",
            config_json=json.dumps(config_in),
        )
        mock_db.sql.return_value = [row]
        mock_db.client = MagicMock()

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

        # Streams resolved into prod_core; additional object is in dev_analytics.
        mock_classify.return_value = ClassificationResult(
            objects=[
                ClassifiedObject(
                    fqn="prod_core.schema.table_a",
                    object_type="table",
                    access_mode="READ_ONLY",
                    format="delta",
                ),
                ClassifiedObject(
                    fqn="dev_analytics.scratch.foo",
                    object_type="table",
                    access_mode="READ_ONLY",
                    format="delta",
                ),
            ],
            review_required=True,
        )
        mock_table_sizes.return_value = {}

        resp = client.post("/api/configs/DR-1042/scan")
        assert resp.status_code == 200, resp.text
        sr = resp.json()["manifest"]["scan_result"]
        assert sr["non_prod_additional_objects"] == ["dev_analytics.scratch.foo"]


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


# ---- US-26: Ownership filtering on stage2 endpoints ----


class TestOwnershipStage2:
    """US-26: ownership / 403 checks on stage2 endpoints.

    ``user_client`` authenticates as ``testuser@example.com`` with role ``"user"``.
    ``client`` authenticates as ``testuser@example.com`` with role ``"admin"``.
    """

    # -- POST /api/configs/{dr_id}/scan  (non-owner => 403) ----------------

    def test_scan_config_non_owner_403(self, user_client, mock_db):
        """Non-owner user gets 403 when scanning another user's config."""
        mock_db.sql.return_value = [make_db_row(created_by="other@example.com")]
        resp = user_client.post("/api/configs/DR-1042/scan")
        assert resp.status_code == 403

    # -- GET /api/configs/{dr_id}/manifest (non-owner => 403) --------------

    def test_get_manifest_non_owner_403(self, user_client, mock_db):
        """Non-owner user gets 403 when reading another user's manifest."""
        mock_db.sql.return_value = [make_db_row(created_by="other@example.com")]
        resp = user_client.get("/api/configs/DR-1042/manifest")
        assert resp.status_code == 403

    # -- PUT /api/configs/{dr_id}/manifest (non-owner => 403) --------------

    def test_update_manifest_non_owner_403(self, user_client, mock_db):
        """Non-owner user gets 403 when updating another user's manifest."""
        mock_db.sql.return_value = [make_db_row(created_by="other@example.com")]
        new_manifest = {"scan_result": {"dr_id": "DR-1042", "objects": []}}
        resp = user_client.put("/api/configs/DR-1042/manifest", json=new_manifest)
        assert resp.status_code == 403

    # -- POST /api/configs/{dr_id}/provision (non-owner => 403) ------------

    def test_provision_non_owner_403(self, user_client, mock_db):
        """Non-owner user gets 403 when provisioning another user's config."""
        mock_db.sql.return_value = [make_db_row(created_by="other@example.com")]
        resp = user_client.post("/api/configs/DR-1042/provision")
        assert resp.status_code == 403

    # -- GET /api/drs/{dr_id}/status (non-owner => 403) --------------------

    @patch(_CONTROL_REPO_PATCHES[0])
    @patch(_CONTROL_REPO_PATCHES[1])
    @patch(_CONTROL_REPO_PATCHES[2])
    def test_dr_status_non_owner_403(self, MockDRRepo, MockObjRepo, MockAuditRepo, user_client, mock_db):
        """Non-owner user gets 403 when checking DR status owned by another user."""
        MockDRRepo.return_value.get.return_value = make_dr_control_row(
            created_by="other@example.com",
        )
        resp = user_client.get("/api/drs/DR-1042/status")
        assert resp.status_code == 403

    # -- GET /api/drs (list: user sees own only) ---------------------------

    @patch("devmirror.control.control_table.DrObjectRepository")
    @patch("devmirror.control.control_table.DRRepository")
    def test_list_drs_user_sees_own_only(self, MockDRRepo, MockObjRepo, user_client, mock_db):
        """Non-admin user only sees DRs they created."""
        MockDRRepo.return_value.list_active.return_value = [
            {
                "dr_id": "DR-1042", "status": "ACTIVE", "description": "Own DR",
                "expiration_date": "2026-06-01", "created_at": "2026-04-01T00:00:00",
                "created_by": "testuser@example.com",
            },
            {
                "dr_id": "DR-2000", "status": "ACTIVE", "description": "Other DR",
                "expiration_date": "2026-06-01", "created_at": "2026-04-01T00:00:00",
                "created_by": "other@example.com",
            },
        ]
        resp = user_client.get("/api/drs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 1
        assert len(data["drs"]) == 1
        assert data["drs"][0]["dr_id"] == "DR-1042"

    @patch("devmirror.control.control_table.DrObjectRepository")
    @patch("devmirror.control.control_table.DRRepository")
    def test_list_drs_admin_sees_all(self, MockDRRepo, MockObjRepo, client, mock_db):
        """Admin user sees all DRs regardless of owner."""
        MockDRRepo.return_value.list_active.return_value = [
            {
                "dr_id": "DR-1042", "status": "ACTIVE", "description": "Own DR",
                "expiration_date": "2026-06-01", "created_at": "2026-04-01T00:00:00",
                "created_by": "testuser@example.com",
            },
            {
                "dr_id": "DR-2000", "status": "ACTIVE", "description": "Other DR",
                "expiration_date": "2026-06-01", "created_at": "2026-04-01T00:00:00",
                "created_by": "other@example.com",
            },
        ]
        resp = client.get("/api/drs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["drs"]) == 2

    # -- POST /api/drs/{dr_id}/cleanup (non-owner => 403) -----------------

    @patch(_CONTROL_REPO_PATCHES[0])
    @patch(_CONTROL_REPO_PATCHES[1])
    @patch(_CONTROL_REPO_PATCHES[2])
    def test_cleanup_non_owner_403(self, MockDRRepo, MockObjRepo, MockAuditRepo, user_client, mock_db):
        """Non-owner user gets 403 when cleaning up another user's DR."""
        MockDRRepo.return_value.get.return_value = make_dr_control_row(
            created_by="other@example.com",
        )
        resp = user_client.post("/api/drs/DR-1042/cleanup")
        assert resp.status_code == 403

    # -- POST /api/drs/{dr_id}/refresh (non-owner => 403) -----------------

    @patch(_CONTROL_REPO_PATCHES[0])
    @patch(_CONTROL_REPO_PATCHES[1])
    @patch(_CONTROL_REPO_PATCHES[2])
    def test_refresh_non_owner_403(self, MockDRRepo, MockObjRepo, MockAuditRepo, user_client, mock_db):
        """Non-owner user gets 403 when refreshing another user's DR."""
        MockDRRepo.return_value.get.return_value = make_dr_control_row(
            created_by="other@example.com",
        )
        resp = user_client.post("/api/drs/DR-1042/refresh", json={"mode": "incremental"})
        assert resp.status_code == 403

    # -- POST /api/drs/{dr_id}/reprovision (non-owner => 403) -------------

    @patch("devmirror.control.control_table.DRRepository")
    def test_reprovision_non_owner_403(self, MockDRRepo, user_client, mock_db):
        """Non-owner user gets 403 when reprovisioning another user's config."""
        mock_db.sql.return_value = [make_db_row(created_by="other@example.com")]
        MockDRRepo.return_value.get.return_value = make_dr_control_row(
            created_by="other@example.com",
        )
        resp = user_client.post("/api/drs/DR-1042/reprovision")
        assert resp.status_code == 403


# ---- Modify endpoint tests (US-27) ----

# Decorator stacks for modify patches: 4 control repos + modify_dr engine
_MODIFY_CONTROL_PATCHES = [
    "devmirror.control.audit.AuditRepository",
    "devmirror.control.control_table.DrAccessRepository",
    "devmirror.control.control_table.DrObjectRepository",
    "devmirror.control.control_table.DRRepository",
]


class TestModifyDr:
    """Tests for POST /api/drs/{dr_id}/modify (US-27)."""

    @patch("devmirror.modify.modification_engine.modify_dr")
    @patch(_MODIFY_CONTROL_PATCHES[0])
    @patch(_MODIFY_CONTROL_PATCHES[1])
    @patch(_MODIFY_CONTROL_PATCHES[2])
    @patch(_MODIFY_CONTROL_PATCHES[3])
    def test_modify_dr_success(
        self, MockDRRepo, MockObjRepo, MockAccessRepo, MockAuditRepo,
        mock_modify, client, mock_db,
    ):
        """Admin client modifies a DR successfully."""
        MockDRRepo.return_value.get.return_value = make_dr_control_row(
            created_by="testuser@example.com",
        )

        # Build a realistic modify_dr return value
        action = MagicMock()
        action.action = "EXTEND_EXPIRATION"
        action.detail = "Extended expiration to 2026-07-01"
        result = MagicMock()
        result.audit_status = "SUCCESS"
        result.actions = [action]
        mock_modify.return_value = result

        body = {"new_expiration_date": "2026-07-01"}
        resp = client.post("/api/drs/DR-1042/modify", json=body)
        assert resp.status_code == 200
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["status"] == "SUCCESS"
        assert "Extended expiration" in data["message"]

    @patch(_MODIFY_CONTROL_PATCHES[0])
    @patch(_MODIFY_CONTROL_PATCHES[1])
    @patch(_MODIFY_CONTROL_PATCHES[2])
    @patch(_MODIFY_CONTROL_PATCHES[3])
    def test_modify_dr_not_found(
        self, MockDRRepo, MockObjRepo, MockAccessRepo, MockAuditRepo,
        client, mock_db,
    ):
        """DR doesn't exist in control table => 404."""
        MockDRRepo.return_value.get.return_value = None

        body = {"new_expiration_date": "2026-07-01"}
        resp = client.post("/api/drs/DR-9999/modify", json=body)
        assert resp.status_code == 404

    @patch(_MODIFY_CONTROL_PATCHES[0])
    @patch(_MODIFY_CONTROL_PATCHES[1])
    @patch(_MODIFY_CONTROL_PATCHES[2])
    @patch(_MODIFY_CONTROL_PATCHES[3])
    def test_modify_dr_wrong_status(
        self, MockDRRepo, MockObjRepo, MockAccessRepo, MockAuditRepo,
        client, mock_db,
    ):
        """DR has status CLEANED_UP => 409 conflict."""
        MockDRRepo.return_value.get.return_value = make_dr_control_row(status="CLEANED_UP")

        body = {"new_expiration_date": "2026-07-01"}
        resp = client.post("/api/drs/DR-1042/modify", json=body)
        assert resp.status_code == 409

    @patch(_MODIFY_CONTROL_PATCHES[0])
    @patch(_MODIFY_CONTROL_PATCHES[1])
    @patch(_MODIFY_CONTROL_PATCHES[2])
    @patch(_MODIFY_CONTROL_PATCHES[3])
    def test_modify_dr_non_owner_403(
        self, MockDRRepo, MockObjRepo, MockAccessRepo, MockAuditRepo,
        user_client, mock_db,
    ):
        """Non-owner user gets 403 when modifying another user's DR."""
        MockDRRepo.return_value.get.return_value = make_dr_control_row(
            created_by="other@example.com",
        )

        body = {"new_expiration_date": "2026-07-01"}
        resp = user_client.post("/api/drs/DR-1042/modify", json=body)
        assert resp.status_code == 403

    @patch("devmirror.modify.modification_engine.modify_dr")
    @patch(_MODIFY_CONTROL_PATCHES[0])
    @patch(_MODIFY_CONTROL_PATCHES[1])
    @patch(_MODIFY_CONTROL_PATCHES[2])
    @patch(_MODIFY_CONTROL_PATCHES[3])
    def test_modify_dr_owner_user_change_stages_pending(
        self, MockDRRepo, MockObjRepo, MockAccessRepo, MockAuditRepo,
        mock_modify, user_client, mock_db,
    ):
        """Phase 2: owner (non-admin) requesting an add_developers change
        on their own provisioned DR is staged for admin approval (HTTP 202)
        rather than being applied immediately."""
        MockDRRepo.return_value.get.return_value = make_dr_control_row(
            created_by="testuser@example.com",
        )
        # Mock the configs row so the staging path can build the proposed config.
        cfg_row = make_db_row(
            dr_id="DR-1042",
            status="provisioned",
            created_by="testuser@example.com",
        )
        # Override config_json so developers/qa_users are deterministic.
        cfg_row["config_json"] = json.dumps(valid_config_payload(
            dr_id="DR-1042",
            developers=["alice@co.com"],
        ))
        mock_db.sql.return_value = [cfg_row]
        mock_db.sql_with_params.side_effect = lambda stmt, params: [cfg_row]

        body = {"add_developers": ["dev2@example.com"]}
        resp = user_client.post("/api/drs/DR-1042/modify", json=body)
        assert resp.status_code == 202
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["status"] == "pending_review"
        assert data["pending_edit_id"].startswith("pe-")
        # Engine should NOT be invoked when staging.
        mock_modify.assert_not_called()

    @patch("devmirror.modify.modification_engine.modify_dr")
    @patch(_MODIFY_CONTROL_PATCHES[0])
    @patch(_MODIFY_CONTROL_PATCHES[1])
    @patch(_MODIFY_CONTROL_PATCHES[2])
    @patch(_MODIFY_CONTROL_PATCHES[3])
    def test_modify_dr_expiration_only_applies_immediately(
        self, MockDRRepo, MockObjRepo, MockAccessRepo, MockAuditRepo,
        mock_modify, client, mock_db,
    ):
        """An expiration-only modify request goes through the engine
        (immediate path), not the staging path."""
        MockDRRepo.return_value.get.return_value = make_dr_control_row(
            created_by="testuser@example.com",
        )

        action = MagicMock()
        action.action = "EXTEND_EXPIRATION"
        action.detail = "Extended expiration to 2026-08-01"
        result = MagicMock()
        result.audit_status = "SUCCESS"
        result.actions = [action]
        mock_modify.return_value = result

        body = {"new_expiration_date": "2026-08-01"}
        resp = client.post("/api/drs/DR-1042/modify", json=body)
        assert resp.status_code == 200
        # Engine WAS invoked (immediate path).
        mock_modify.assert_called_once()
        # Audit repo should NOT have a CONFIG_EDIT_PENDING row from staging.
        pending_calls = [
            c for c in MockAuditRepo.return_value.append.call_args_list
            if c.kwargs.get("action") == "CONFIG_EDIT_PENDING"
        ]
        assert pending_calls == []

    @patch("devmirror.modify.modification_engine.modify_dr")
    @patch(_MODIFY_CONTROL_PATCHES[0])
    @patch(_MODIFY_CONTROL_PATCHES[1])
    @patch(_MODIFY_CONTROL_PATCHES[2])
    @patch(_MODIFY_CONTROL_PATCHES[3])
    def test_modify_dr_mixed_user_and_expiration_stages_pending(
        self, MockDRRepo, MockObjRepo, MockAccessRepo, MockAuditRepo,
        mock_modify, client, mock_db,
    ):
        """Body has both add_developers and new_expiration_date -> staged
        (sensitive change present)."""
        MockDRRepo.return_value.get.return_value = make_dr_control_row(
            created_by="testuser@example.com",
        )
        cfg_row = make_db_row(
            dr_id="DR-1042", status="provisioned", created_by="testuser@example.com",
        )
        cfg_row["config_json"] = json.dumps(valid_config_payload(
            dr_id="DR-1042",
            developers=["alice@co.com"],
        ))
        mock_db.sql.return_value = [cfg_row]
        mock_db.sql_with_params.side_effect = lambda stmt, params: [cfg_row]

        body = {
            "add_developers": ["dev2@example.com"],
            "new_expiration_date": "2026-08-01",
        }
        resp = client.post("/api/drs/DR-1042/modify", json=body)
        assert resp.status_code == 202
        data = resp.json()
        assert data["status"] == "pending_review"
        assert data["pending_edit_id"].startswith("pe-")
        # Engine NOT invoked for the staged path.
        mock_modify.assert_not_called()

    @patch("devmirror.modify.modification_engine.modify_dr")
    @patch(_MODIFY_CONTROL_PATCHES[0])
    @patch(_MODIFY_CONTROL_PATCHES[1])
    @patch(_MODIFY_CONTROL_PATCHES[2])
    @patch(_MODIFY_CONTROL_PATCHES[3])
    def test_modify_dr_engine_error(
        self, MockDRRepo, MockObjRepo, MockAccessRepo, MockAuditRepo,
        mock_modify, client, mock_db,
    ):
        """modify_dr raises ModificationError => 400."""
        from devmirror.modify.modification_engine import ModificationError

        MockDRRepo.return_value.get.return_value = make_dr_control_row(
            created_by="testuser@example.com",
        )
        mock_modify.side_effect = ModificationError("Invalid expiration date")

        body = {"new_expiration_date": "2020-01-01"}
        resp = client.post("/api/drs/DR-1042/modify", json=body)
        assert resp.status_code == 400
        assert "Invalid expiration date" in resp.json()["detail"]


# ---- US-29: Admin-only gates on scan/manifest edit/provision/cleanup/reprovision ----


class TestAdminOnlyEndpoints:
    """US-29: Even resource owners (non-admin) get 403 on admin-only endpoints.

    ``user_client`` authenticates as ``testuser@example.com`` with role ``"user"``.
    All test configs/DRs are owned by that same user to prove that ownership
    alone is insufficient -- admin role is required.
    """

    # -- POST /api/configs/{dr_id}/scan  (admin-only => 403 for user) ----------

    def test_scan_requires_admin(self, user_client, mock_db):
        """Owner with role=user gets 403 when scanning own config."""
        mock_db.sql.return_value = [make_db_row(created_by="testuser@example.com")]
        resp = user_client.post("/api/configs/DR-1042/scan")
        assert resp.status_code == 403

    # -- PUT /api/configs/{dr_id}/manifest (admin-only => 403 for user) --------

    def test_update_manifest_requires_admin(self, user_client, mock_db):
        """Owner with role=user gets 403 when updating own config's manifest."""
        mock_db.sql.return_value = [make_db_row(created_by="testuser@example.com")]
        new_manifest = {"scan_result": {"dr_id": "DR-1042", "objects": []}}
        resp = user_client.put("/api/configs/DR-1042/manifest", json=new_manifest)
        assert resp.status_code == 403

    # -- POST /api/configs/{dr_id}/provision (admin-only => 403 for user) ------

    def test_provision_requires_admin(self, user_client, mock_db):
        """Owner with role=user gets 403 when provisioning own config."""
        mock_db.sql.return_value = [make_db_row(created_by="testuser@example.com")]
        resp = user_client.post("/api/configs/DR-1042/provision")
        assert resp.status_code == 403

    # -- POST /api/drs/{dr_id}/cleanup (admin-only => 403 for user) -----------

    def test_cleanup_requires_admin(self, user_client, mock_db):
        """Owner with role=user gets 403 when cleaning up own DR."""
        resp = user_client.post("/api/drs/DR-1042/cleanup")
        assert resp.status_code == 403

    # -- POST /api/drs/{dr_id}/reprovision (admin-only => 403 for user) -------

    def test_reprovision_requires_admin(self, user_client, mock_db):
        """Owner with role=user gets 403 when reprovisioning own config."""
        mock_db.sql.return_value = [make_db_row(created_by="testuser@example.com")]
        resp = user_client.post("/api/drs/DR-1042/reprovision")
        assert resp.status_code == 403

    # -- POST /api/drs/{dr_id}/refresh (NOT admin-only => 202 for owner) ------

    @patch("devmirror.refresh.refresh_engine.refresh_dr")
    @patch("devmirror.control.audit.AuditRepository")
    @patch("devmirror.control.control_table.DrObjectRepository")
    @patch("devmirror.control.control_table.DRRepository")
    def test_refresh_does_not_require_admin(
        self, MockDRRepo, MockObjRepo, MockAuditRepo, mock_refresh,
        user_client, mock_db,
    ):
        """Owner with role=user can refresh their own DR (202, not 403)."""
        MockDRRepo.return_value.get.return_value = make_dr_control_row(
            created_by="testuser@example.com",
        )
        mock_refresh.return_value = mock_refresh_result()

        resp = user_client.post("/api/drs/DR-1042/refresh", json={"mode": "incremental"})
        assert resp.status_code == 202
