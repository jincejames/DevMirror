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

    def test_create_invalid_dr_id(self, client, mock_db):
        payload = valid_config_payload(dr_id="INVALID")
        mock_db.sql.return_value = [make_db_row(dr_id="INVALID", status="invalid")]

        resp = client.post("/api/configs", json=payload)
        assert resp.status_code == 201
        data = resp.json()
        assert data["status"] == "invalid"

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
