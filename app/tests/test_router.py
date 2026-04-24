"""Stage 1 endpoint tests with FastAPI TestClient and mocked dependencies."""

from __future__ import annotations

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
