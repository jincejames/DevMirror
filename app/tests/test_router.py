"""Stage 1 endpoint tests with FastAPI TestClient and mocked dependencies."""

from __future__ import annotations

import json
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

from .conftest import make_db_row, valid_config_payload


class TestHealthCheck:
    def test_health_returns_ok(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


class TestCreateConfig:
    def test_create_valid_config(self, client, mock_db):
        payload = valid_config_payload()
        mock_db.sql.return_value = [make_db_row()]

        resp = client.post("/api/configs", json=payload)
        assert resp.status_code == 201
        data = resp.json()
        assert data["dr_id"] == "DR-1042"
        assert data["status"] == "valid"

    def test_create_rejects_supplied_dr_id(self, client, mock_db):
        """US-34 AC4: callers cannot supply dr_id -- server must return 400
        with detail ``DR ID is auto-generated; do not supply``.
        """
        payload = valid_config_payload(dr_id="DR-9999")

        resp = client.post("/api/configs", json=payload)
        assert resp.status_code == 400
        # The detail must include the exact phrasing required by the spec
        # so the UI / API clients can surface a consistent message.
        assert resp.json()["detail"] == "DR ID is auto-generated; do not supply"

    def test_create_uses_server_assigned_dr_id(self, client, mock_db, monkeypatch):
        """US-34: the server-generated dr_id is returned in the response."""
        from devmirror.utils import id_generator as idg

        monkeypatch.setattr(idg, "next_dr_id", lambda db_client, settings: "DR00007")
        payload = valid_config_payload()  # no dr_id supplied
        mock_db.sql.return_value = [make_db_row(dr_id="DR00007")]

        resp = client.post("/api/configs", json=payload)
        assert resp.status_code == 201
        data = resp.json()
        assert data["dr_id"] == "DR00007"

    def test_create_with_empty_streams_422(self, client, mock_db):
        payload = valid_config_payload(streams=[])
        resp = client.post("/api/configs", json=payload)
        assert resp.status_code == 422


class TestListConfigs:
    def test_list_returns_configs(self, client, mock_db):
        mock_db.sql.return_value = [
            make_db_row(dr_id="DR-1"),
            make_db_row(dr_id="DR-2"),
        ]
        resp = client.get("/api/configs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["configs"]) == 2


class TestGetConfig:
    def test_get_existing_config(self, client, mock_db):
        mock_db.sql.return_value = [make_db_row()]
        resp = client.get("/api/configs/DR-1042")
        assert resp.status_code == 200
        assert resp.json()["dr_id"] == "DR-1042"

    def test_get_not_found(self, client, mock_db):
        mock_db.sql.return_value = []
        resp = client.get("/api/configs/DR-9999")
        assert resp.status_code == 404


class TestDeleteConfig:
    def test_delete_success(self, client, mock_db):
        mock_db.sql.return_value = [make_db_row(status="valid")]
        resp = client.delete("/api/configs/DR-1042")
        assert resp.status_code == 204

    def test_delete_provisioned_409(self, client, mock_db):
        mock_db.sql.return_value = [make_db_row(status="provisioned")]
        resp = client.delete("/api/configs/DR-1042")
        assert resp.status_code == 409

    def test_delete_not_found(self, client, mock_db):
        mock_db.sql.return_value = []
        resp = client.delete("/api/configs/DR-9999")
        assert resp.status_code == 404


class TestRejectConfig:
    """Admin-only POST /api/configs/{dr_id}/reject -- terminal rejection of
    a submitted config with a mandatory comment.  Replaces the old
    DR-level reject -- a config row is what the admin actually reviews."""

    def test_reject_scanned_config_succeeds(self, client, mock_db):
        # First get() reads 'scanned'; the post-reject re-read returns the
        # 'rejected' row (models the CAS UPDATE landing).
        mock_db.sql.side_effect = [
            [make_db_row(status="scanned")],
            [make_db_row(status="rejected")],
        ]
        resp = client.post(
            "/api/configs/DR-1042/reject",
            json={"comment": "Naming convention off"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["dr_id"] == "DR-1042"
        assert body["status"] == "rejected"
        assert body["rejection_comment"] == "Naming convention off"
        # The UPDATE must set status='rejected' AND carry the CAS guard.
        reject_updates = [
            call.args[0]
            for call in mock_db.sql_exec_with_params.call_args_list
            if call.args and "status = 'rejected'" in call.args[0]
        ]
        assert reject_updates, "expected UPDATE setting status='rejected'"
        assert any("AND status = :current_status" in s for s in reject_updates), \
            "reject UPDATE must carry the compare-and-set status guard"

    def test_reject_valid_config_also_succeeds(self, client, mock_db):
        # `valid` is also an acceptable source status -- the admin might
        # reject before the user even runs Scan.
        mock_db.sql.side_effect = [
            [make_db_row(status="valid")],
            [make_db_row(status="rejected")],
        ]
        resp = client.post(
            "/api/configs/DR-1042/reject",
            json={"comment": "Out of scope for this team"},
        )
        assert resp.status_code == 200

    def test_reject_concurrent_status_change_returns_409(self, client, mock_db):
        # TOCTOU (finding #6): the row reads 'valid' at guard time, but a
        # concurrent provision flips it before the reject UPDATE.  The CAS
        # guard makes the UPDATE a no-op; the post-write re-read still shows
        # 'provisioned' (not 'rejected'), so the endpoint returns 409.
        mock_db.sql.side_effect = [
            [make_db_row(status="valid")],        # initial guard read
            [make_db_row(status="provisioned")],  # re-read after CAS no-op
        ]
        resp = client.post(
            "/api/configs/DR-1042/reject", json={"comment": "race"},
        )
        assert resp.status_code == 409
        assert "concurrent" in resp.json()["detail"].lower()

    def test_reject_provisioned_returns_409(self, client, mock_db):
        # Already-provisioned configs are out of scope -- the admin should
        # use cleanup to drop the infrastructure, not reject the request.
        mock_db.sql.return_value = [make_db_row(status="provisioned")]
        resp = client.post(
            "/api/configs/DR-1042/reject", json={"comment": "n/a"},
        )
        assert resp.status_code == 409
        assert "provisioned" in resp.json()["detail"]

    def test_reject_already_rejected_returns_409(self, client, mock_db):
        mock_db.sql.return_value = [make_db_row(status="rejected")]
        resp = client.post(
            "/api/configs/DR-1042/reject", json={"comment": "duplicate"},
        )
        assert resp.status_code == 409
        assert "already rejected" in resp.json()["detail"].lower()

    def test_reject_unknown_config_returns_404(self, client, mock_db):
        mock_db.sql.return_value = []
        resp = client.post(
            "/api/configs/DR-9999/reject", json={"comment": "n/a"},
        )
        assert resp.status_code == 404

    def test_reject_empty_comment_returns_422(self, client, mock_db):
        # Mandatory non-empty comment is enforced at the Pydantic layer
        # so admins can't bury a rejection with no rationale.
        resp = client.post(
            "/api/configs/DR-1042/reject", json={"comment": "   "},
        )
        assert resp.status_code == 422


class TestRevalidateConfig:
    def test_revalidate_updates_status(self, client, mock_db):
        mock_db.sql.return_value = [make_db_row(status="invalid")]

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
        row = make_db_row()
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


class TestListConfigsFiltering:
    """US-26: list_configs filters by created_by for non-admin users."""

    def test_list_configs_user_sees_own_only(self, user_client, mock_db):
        """Non-admin user only sees configs they created."""
        mock_db.sql.return_value = [
            make_db_row(dr_id="DR-1", created_by="testuser@example.com"),
            make_db_row(dr_id="DR-2", created_by="testuser@example.com"),
            make_db_row(dr_id="DR-3", created_by="other@example.com"),
        ]
        resp = user_client.get("/api/configs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 2
        assert len(data["configs"]) == 2
        dr_ids = {c["dr_id"] for c in data["configs"]}
        assert dr_ids == {"DR-1", "DR-2"}

    def test_list_configs_admin_sees_all(self, client, mock_db):
        """Admin user sees all configs regardless of owner."""
        mock_db.sql.return_value = [
            make_db_row(dr_id="DR-1", created_by="testuser@example.com"),
            make_db_row(dr_id="DR-2", created_by="other@example.com"),
            make_db_row(dr_id="DR-3", created_by="another@example.com"),
        ]
        resp = client.get("/api/configs")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 3
        assert len(data["configs"]) == 3


class TestOwnershipChecks:
    """US-24: ownership checks on config endpoints.

    ``user_client`` authenticates as ``testuser@example.com`` with role ``"user"``.
    ``client`` authenticates as ``testuser@example.com`` with role ``"admin"``.
    """

    # -- GET /api/configs/{dr_id} ------------------------------------------

    def test_get_config_owner_allowed(self, user_client, mock_db):
        """User can read their own config."""
        mock_db.sql.return_value = [make_db_row(created_by="testuser@example.com")]
        resp = user_client.get("/api/configs/DR-1042")
        assert resp.status_code == 200
        assert resp.json()["dr_id"] == "DR-1042"

    def test_get_config_non_owner_403(self, user_client, mock_db):
        """User gets 403 when accessing another user's config."""
        mock_db.sql.return_value = [make_db_row(created_by="otheruser@example.com")]
        resp = user_client.get("/api/configs/DR-1042")
        assert resp.status_code == 403

    def test_get_config_admin_can_access_any(self, client, mock_db):
        """Admin can access any config regardless of owner."""
        mock_db.sql.return_value = [make_db_row(created_by="otheruser@example.com")]
        resp = client.get("/api/configs/DR-1042")
        assert resp.status_code == 200
        assert resp.json()["dr_id"] == "DR-1042"

    # -- PUT /api/configs/{dr_id} ------------------------------------------

    def test_update_config_owner_allowed(self, user_client, mock_db):
        """User can update their own config."""
        own_row = make_db_row(created_by="testuser@example.com")
        mock_db.sql.return_value = [own_row]
        payload = valid_config_payload()
        resp = user_client.put("/api/configs/DR-1042", json=payload)
        assert resp.status_code == 200

    def test_update_config_non_owner_403(self, user_client, mock_db):
        """User gets 403 when updating another user's config."""
        mock_db.sql.return_value = [make_db_row(created_by="otheruser@example.com")]
        resp = user_client.put("/api/configs/DR-1042", json=valid_config_payload())
        assert resp.status_code == 403

    # -- DELETE /api/configs/{dr_id} ---------------------------------------

    def test_delete_config_owner_allowed(self, user_client, mock_db):
        """User can delete their own non-provisioned config."""
        own_row = make_db_row(status="valid", created_by="testuser@example.com")
        mock_db.sql.side_effect = [[own_row], None]
        resp = user_client.delete("/api/configs/DR-1042")
        assert resp.status_code == 204

    def test_delete_config_non_owner_403(self, user_client, mock_db):
        """User gets 403 when deleting another user's config."""
        mock_db.sql.return_value = [make_db_row(created_by="otheruser@example.com")]
        resp = user_client.delete("/api/configs/DR-1042")
        assert resp.status_code == 403

    # -- POST /api/configs/{dr_id}/validate --------------------------------

    def test_validate_config_owner_allowed(self, user_client, mock_db):
        """User can re-validate their own config."""
        own_row = make_db_row(status="invalid", created_by="testuser@example.com")
        mock_db.sql.side_effect = [[own_row], None]
        resp = user_client.post("/api/configs/DR-1042/validate")
        assert resp.status_code == 200
        assert resp.json()["status"] in ("valid", "invalid")

    def test_validate_config_non_owner_403(self, user_client, mock_db):
        """User gets 403 when validating another user's config."""
        mock_db.sql.return_value = [make_db_row(created_by="otheruser@example.com")]
        resp = user_client.post("/api/configs/DR-1042/validate")
        assert resp.status_code == 403

    # -- GET /api/configs/{dr_id}/yaml -------------------------------------

    def test_yaml_export_owner_allowed(self, user_client, mock_db):
        """User can export YAML for their own config."""
        row = make_db_row(created_by="testuser@example.com")
        row["config_yaml"] = "version: '1.0'\n"
        mock_db.sql.return_value = [row]
        resp = user_client.get("/api/configs/DR-1042/yaml")
        assert resp.status_code == 200
        assert "text/yaml" in resp.headers.get("content-type", "")

    def test_yaml_export_non_owner_403(self, user_client, mock_db):
        """User gets 403 when exporting another user's config."""
        mock_db.sql.return_value = [make_db_row(created_by="otheruser@example.com")]
        resp = user_client.get("/api/configs/DR-1042/yaml")
        assert resp.status_code == 403

    # -- POST /api/configs (create — no ownership check) -------------------

    def test_create_config_any_user_allowed(self, user_client, mock_db):
        """Any user (non-admin) can create a new config."""
        mock_db.sql.return_value = [make_db_row(created_by="testuser@example.com")]
        payload = valid_config_payload()
        resp = user_client.post("/api/configs", json=payload)
        assert resp.status_code == 201
        assert resp.json()["dr_id"] == "DR-1042"


class TestUpdateProvisionedSensitiveEditStagesPending:
    """Phase 2: PUT on a provisioned DR with sensitive-field changes
    (access.developers, access.uat_users, additional_objects) must NOT
    apply grants directly. Instead it stages a CONFIG_EDIT_PENDING audit
    row and returns HTTP 202. Grants are applied only when an admin
    approves via the approval endpoints (covered in Phase 2 tests)."""

    @staticmethod
    def _provisioned_row(developers, uat_users=None, **overrides) -> dict:
        """Build a provisioned-status DB row with a specific access list."""
        config = valid_config_payload(
            dr_id="DR-1042",
            developers=developers,
            uat_users=uat_users or [],
        )
        row = make_db_row(dr_id="DR-1042", status="provisioned", **overrides)
        row["config_json"] = json.dumps(config)
        return row

    @staticmethod
    def _patch_control_repos():
        """Patch the four per-repo factories to return mocks.

        Returns a context manager that, when entered, patches each
        factory on backend.router.  The returned tuple matches the old
        4-repo shape so existing call sites keep working.
        """
        mock_dr = MagicMock()
        mock_obj = MagicMock()
        mock_access = MagicMock()
        mock_audit = MagicMock()

        class _PatchCtx:
            def __enter__(self_inner):
                self_inner._stack = ExitStack().__enter__()
                # `create=True` so the patch works even when the test target
                # didn't import a specific factory (e.g. router.py never
                # touches the DR repo directly -- it stages instead).
                self_inner._stack.enter_context(patch("backend.router._dr_repo", return_value=mock_dr, create=True))
                self_inner._stack.enter_context(patch("backend.router._obj_repo", return_value=mock_obj, create=True))
                self_inner._stack.enter_context(patch("backend.router._access_repo", return_value=mock_access, create=True))
                self_inner._stack.enter_context(patch("backend.router._audit_repo", return_value=mock_audit))
                return self_inner

            def __exit__(self_inner, *exc):
                return self_inner._stack.__exit__(*exc)

        return _PatchCtx(), mock_dr, mock_obj, mock_access, mock_audit

    def test_add_developer_stages_pending_not_grants(self, client, mock_db):
        """Adding a developer stages a pending edit; no grants run on PUT."""
        row = self._provisioned_row(["alice@co.com"])
        mock_db.sql.return_value = [row]

        payload = valid_config_payload(developers=["alice@co.com", "bob@co.com"])
        ctx, _dr, _obj, _access, _audit = self._patch_control_repos()
        with ctx, patch("backend.router._manage_users", create=True) as mock_manage:
            resp = client.put("/api/configs/DR-1042", json=payload)

        assert resp.status_code == 202
        body = resp.json()
        assert body["dr_id"] == "DR-1042"
        assert body["status"] == "pending_review"
        assert body["pending_edit_id"].startswith("pe-")
        # Grants must NOT be applied at staging time.
        mock_manage.assert_not_called()

    def test_remove_developer_stages_pending(self, client, mock_db):
        """Removing a developer stages pending; no grants run on PUT."""
        row = self._provisioned_row(["alice@co.com", "bob@co.com"])
        mock_db.sql.return_value = [row]

        payload = valid_config_payload(developers=["alice@co.com"])
        ctx, _dr, _obj, _access, _audit = self._patch_control_repos()
        with ctx, patch("backend.router._manage_users", create=True) as mock_manage:
            resp = client.put("/api/configs/DR-1042", json=payload)

        assert resp.status_code == 202
        mock_manage.assert_not_called()

    def test_uat_change_stages_pending(self, client, mock_db):
        """UAT-list additions/removals stage a pending edit."""
        row = self._provisioned_row(
            ["alice@co.com"], uat_users=["uat1@co.com", "uat2@co.com"]
        )
        mock_db.sql.return_value = [row]

        payload = valid_config_payload(
            developers=["alice@co.com"],
            uat_users=["uat1@co.com", "uat3@co.com"],
        )
        ctx, _dr, _obj, _access, _audit = self._patch_control_repos()
        with ctx, patch("backend.router._manage_users", create=True) as mock_manage:
            resp = client.put("/api/configs/DR-1042", json=payload)

        assert resp.status_code == 202
        mock_manage.assert_not_called()

    def test_no_sensitive_change_applies_immediately(self, client, mock_db):
        """A PUT that changes only the description applies immediately
        (no staging, no grants)."""
        row = self._provisioned_row(["alice@co.com"])
        mock_db.sql.return_value = [row]

        payload = valid_config_payload(
            developers=["alice@co.com"],
            description="Just updating the description",
        )
        ctx, _dr, _obj, _access, _audit = self._patch_control_repos()
        with ctx, patch("backend.router._manage_users", create=True) as mock_manage:
            resp = client.put("/api/configs/DR-1042", json=payload)

        assert resp.status_code == 200
        mock_manage.assert_not_called()

    def test_unprovisioned_sensitive_edit_applies_immediately(self, client, mock_db):
        """A PUT on a 'valid' (non-provisioned) config applies immediately
        even when the developer list changes -- staging is only for
        provisioned DRs."""
        config = valid_config_payload(
            dr_id="DR-1042", developers=["alice@co.com"]
        )
        row = make_db_row(dr_id="DR-1042", status="valid")
        row["config_json"] = json.dumps(config)
        mock_db.sql.return_value = [row]

        payload = valid_config_payload(
            developers=["alice@co.com", "bob@co.com"]
        )
        with patch("backend.router._manage_users", create=True) as mock_manage:
            resp = client.put("/api/configs/DR-1042", json=payload)

        assert resp.status_code == 200
        # Non-provisioned config: no grant logic runs.
        mock_manage.assert_not_called()

    def test_sensitive_edit_writes_pending_audit(self, client, mock_db):
        """A sensitive PUT writes a CONFIG_EDIT_PENDING audit entry containing
        the diff and a pending_edit_id."""
        row = self._provisioned_row(["alice@co.com"])
        mock_db.sql.return_value = [row]

        payload = valid_config_payload(
            developers=["alice@co.com", "bob@co.com"]
        )
        ctx, _dr, _obj, _access, mock_audit = self._patch_control_repos()
        with ctx, patch("backend.router._manage_users", create=True):
            resp = client.put("/api/configs/DR-1042", json=payload)

        assert resp.status_code == 202
        assert mock_audit.append.call_count == 1
        _args, kwargs = mock_audit.append.call_args
        assert kwargs["dr_id"] == "DR-1042"
        assert kwargs["action"] == "CONFIG_EDIT_PENDING"
        assert kwargs["performed_by"] == "testuser@example.com"
        assert kwargs["status"] == "PENDING"
        detail = json.loads(kwargs["action_detail"])
        assert "pending_edit_id" in detail
        assert "changes" in detail
        assert "proposed_config_json" in detail
        # Expect a developers-field diff entry.
        dev_changes = [c for c in detail["changes"] if c["field"] == "access.developers"]
        assert len(dev_changes) == 1
        assert dev_changes[0]["before"] == ["alice@co.com"]
        assert dev_changes[0]["after"] == ["alice@co.com", "bob@co.com"]

    def test_pending_audit_includes_proposed_config_json(self, client, mock_db):
        """The staged audit row's action_detail must include
        ``proposed_config_json`` whose value parses to a JSON object with the
        new developer list."""
        row = self._provisioned_row(["alice@co.com"])
        mock_db.sql.return_value = [row]

        payload = valid_config_payload(
            developers=["alice@co.com", "bob@co.com"]
        )
        ctx, _dr, _obj, _access, mock_audit = self._patch_control_repos()
        with ctx, patch("backend.router._manage_users", create=True):
            resp = client.put("/api/configs/DR-1042", json=payload)

        assert resp.status_code == 202
        _args, kwargs = mock_audit.append.call_args
        detail = json.loads(kwargs["action_detail"])
        assert "proposed_config_json" in detail
        proposed = json.loads(detail["proposed_config_json"])
        assert isinstance(proposed, dict)
        assert "bob@co.com" in proposed["developers"]
        assert "alice@co.com" in proposed["developers"]

    def test_additional_objects_change_stages_pending(self, client, mock_db):
        """A sensitive edit changing only ``additional_objects`` stages a
        pending edit (not just dev/qa users)."""
        row = self._provisioned_row(["alice@co.com"])
        # Pre-existing config has no additional_objects.
        mock_db.sql.return_value = [row]

        payload = valid_config_payload(
            developers=["alice@co.com"],
            additional_objects=["prod.schema.extra_table"],
        )
        ctx, _dr, _obj, _access, mock_audit = self._patch_control_repos()
        with ctx, patch("backend.router._manage_users", create=True) as mock_manage:
            resp = client.put("/api/configs/DR-1042", json=payload)

        assert resp.status_code == 202
        body = resp.json()
        assert body["status"] == "pending_review"
        assert body["pending_edit_id"].startswith("pe-")
        # No grants applied at staging time.
        mock_manage.assert_not_called()
        # CONFIG_EDIT_PENDING audit row written.
        pending_calls = [
            c for c in mock_audit.append.call_args_list
            if c.kwargs.get("action") == "CONFIG_EDIT_PENDING"
        ]
        assert len(pending_calls) == 1
        detail = json.loads(pending_calls[0].kwargs["action_detail"])
        ao_changes = [c for c in detail["changes"] if c["field"] == "additional_objects"]
        assert len(ao_changes) == 1

    def test_description_only_change_does_not_stage(self, client, mock_db):
        """A PUT changing only description -> 200 + no CONFIG_EDIT_PENDING row."""
        row = self._provisioned_row(["alice@co.com"])
        mock_db.sql.return_value = [row]

        payload = valid_config_payload(
            developers=["alice@co.com"],
            description="Just a description tweak",
        )
        ctx, _dr, _obj, _access, mock_audit = self._patch_control_repos()
        with ctx, patch("backend.router._manage_users", create=True):
            resp = client.put("/api/configs/DR-1042", json=payload)

        assert resp.status_code == 200
        pending_calls = [
            c for c in mock_audit.append.call_args_list
            if c.kwargs.get("action") == "CONFIG_EDIT_PENDING"
        ]
        assert pending_calls == []

    def test_unprovisioned_sensitive_edit_does_not_stage(self, client, mock_db):
        """For a `valid` (not provisioned) config, even sensitive changes
        apply immediately. No CONFIG_EDIT_PENDING audit row is written."""
        row = make_db_row(dr_id="DR-1042", status="valid")
        row["config_json"] = json.dumps(
            valid_config_payload(dr_id="DR-1042", developers=["alice@co.com"])
        )
        mock_db.sql.return_value = [row]

        payload = valid_config_payload(
            developers=["alice@co.com", "bob@co.com"]
        )
        ctx, _dr, _obj, _access, mock_audit = self._patch_control_repos()
        with ctx, patch("backend.router._manage_users", create=True):
            resp = client.put("/api/configs/DR-1042", json=payload)

        assert resp.status_code == 200
        pending_calls = [
            c for c in mock_audit.append.call_args_list
            if c.kwargs.get("action") == "CONFIG_EDIT_PENDING"
        ]
        assert pending_calls == []
