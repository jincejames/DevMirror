"""Tests for GET /api/streams/search (multi-workspace)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _make_job(name: str) -> MagicMock:
    j = MagicMock()
    j.settings.name = name
    return j


def _make_pipeline(name: str) -> MagicMock:
    p = MagicMock()
    p.name = name
    return p


def _wire_local_ws(mock_db, *, jobs: list[str] | None = None, pipelines: list[str] | None = None):
    """Wire up the local workspace's jobs.list and pipelines.list_pipelines mocks."""
    mock_db.client.jobs.list.return_value = [_make_job(n) for n in (jobs or [])]
    mock_db.client.pipelines.list_pipelines.return_value = [
        _make_pipeline(n) for n in (pipelines or [])
    ]


class TestSearchStreamsLocalOnly:
    """When the remote-workspace env vars are unset, only the local
    WorkspaceClient is queried and every result is tagged with
    ``workspace='local'`` (or whatever DEVMIRROR_LOCAL_WORKSPACE_LABEL says).
    """

    def test_local_only_jobs_and_pipelines(self, client, mock_db, monkeypatch):
        # Explicitly clear remote env so the remote branch doesn't fire
        # even if the host's environment has stale values.
        monkeypatch.delenv("DEVMIRROR_REMOTE_WORKSPACE_HOST", raising=False)
        monkeypatch.delenv("DEVMIRROR_REMOTE_WORKSPACE_TOKEN", raising=False)
        monkeypatch.delenv("DEVMIRROR_LOCAL_WORKSPACE_LABEL", raising=False)

        _wire_local_ws(
            mock_db,
            jobs=["devmirror-lifecycle", "devmirror-cleanup"],
            pipelines=["devmirror-pipeline"],
        )

        with patch("backend.router.WorkspaceClient") as mock_remote_ws:
            resp = client.get("/api/streams/search?q=devmirror")

        assert resp.status_code == 200
        data = resp.json()
        names = sorted(r["name"] for r in data["results"])
        assert names == ["devmirror-cleanup", "devmirror-lifecycle", "devmirror-pipeline"]
        # Every result tagged "local" (the default label).
        assert {r["workspace"] for r in data["results"]} == {"local"}
        # Remote WorkspaceClient was NOT constructed.
        mock_remote_ws.assert_not_called()

    def test_local_label_override(self, client, mock_db, monkeypatch):
        monkeypatch.setenv("DEVMIRROR_LOCAL_WORKSPACE_LABEL", "lh-support")
        monkeypatch.delenv("DEVMIRROR_REMOTE_WORKSPACE_HOST", raising=False)
        monkeypatch.delenv("DEVMIRROR_REMOTE_WORKSPACE_TOKEN", raising=False)

        _wire_local_ws(mock_db, jobs=["my-job"])

        resp = client.get("/api/streams/search?q=my")
        assert resp.status_code == 200
        data = resp.json()
        assert data["results"][0]["workspace"] == "lh-support"


class TestSearchStreamsLocalAndRemote:
    """When all four env vars are set, both workspaces are searched and
    results carry their respective workspace labels.
    """

    def test_local_and_remote_results_merged(self, client, mock_db, monkeypatch):
        monkeypatch.setenv("DEVMIRROR_LOCAL_WORKSPACE_LABEL", "local")
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_HOST", "https://prod.cloud.databricks.com")
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_LABEL", "prod")
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_TOKEN", "fake-pat")

        _wire_local_ws(mock_db, jobs=["local-only-job"])

        # Remote WorkspaceClient mock -- has its own jobs/pipelines
        remote_ws = MagicMock()
        remote_ws.jobs.list.return_value = [_make_job("remote-only-job")]
        remote_ws.pipelines.list_pipelines.return_value = [
            _make_pipeline("remote-only-pipeline"),
        ]

        with patch("backend.router.WorkspaceClient") as mock_remote_cls:
            mock_remote_cls.return_value = remote_ws
            resp = client.get("/api/streams/search?q=anything")

        assert resp.status_code == 200
        data = resp.json()
        results_by_name = {r["name"]: r["workspace"] for r in data["results"]}
        assert results_by_name == {
            "local-only-job": "local",
            "remote-only-job": "prod",
            "remote-only-pipeline": "prod",
        }
        mock_remote_cls.assert_called_once()
        # Verify the remote WorkspaceClient was built with PAT auth and the
        # configured host.
        kwargs = mock_remote_cls.call_args.kwargs
        assert kwargs.get("host") == "https://prod.cloud.databricks.com"
        assert kwargs.get("token") == "fake-pat"
        assert kwargs.get("auth_type") == "pat"

    def test_remote_label_defaults_to_host(self, client, mock_db, monkeypatch):
        # When DEVMIRROR_REMOTE_WORKSPACE_LABEL is empty, the host is
        # used as the workspace label so results are still distinguishable.
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_HOST", "https://prod.example.net")
        monkeypatch.delenv("DEVMIRROR_REMOTE_WORKSPACE_LABEL", raising=False)
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_TOKEN", "fake-pat")

        _wire_local_ws(mock_db, jobs=[])
        remote_ws = MagicMock()
        remote_ws.jobs.list.return_value = [_make_job("foo")]
        remote_ws.pipelines.list_pipelines.return_value = []

        with patch("backend.router.WorkspaceClient") as mock_remote_cls:
            mock_remote_cls.return_value = remote_ws
            resp = client.get("/api/streams/search?q=foo")

        data = resp.json()
        assert data["results"][0]["workspace"] == "https://prod.example.net"


class TestSearchStreamsRemoteFailureIsNonFatal:
    """If the remote workspace search raises (network issue, bad token,
    etc.) the local results are still returned -- the search degrades
    gracefully rather than 500ing.
    """

    def test_remote_jobs_list_raises(self, client, mock_db, monkeypatch, caplog):
        import logging

        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_HOST", "https://prod.cloud.databricks.com")
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_LABEL", "prod")
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_TOKEN", "fake-pat")

        _wire_local_ws(mock_db, jobs=["local-job"])

        remote_ws = MagicMock()
        remote_ws.jobs.list.side_effect = RuntimeError("token rejected")
        remote_ws.pipelines.list_pipelines.side_effect = RuntimeError("token rejected")

        with patch("backend.router.WorkspaceClient") as mock_remote_cls, \
                caplog.at_level(logging.WARNING, logger="backend.router"):
            mock_remote_cls.return_value = remote_ws
            resp = client.get("/api/streams/search?q=local-job")

        assert resp.status_code == 200
        data = resp.json()
        # Local result still returned despite remote failure.
        names = [r["name"] for r in data["results"]]
        assert "local-job" in names
        # And a WARNING was logged for the remote failure.
        assert any("workspace" in r.message and "prod" in r.message
                   for r in caplog.records)

    def test_remote_client_construction_raises(self, client, mock_db, monkeypatch):
        # If WorkspaceClient(...) itself raises (e.g. invalid host) the
        # endpoint must still return local results.
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_HOST", "not-a-url")
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_TOKEN", "fake-pat")
        _wire_local_ws(mock_db, jobs=["local-job"])

        with patch("backend.router.WorkspaceClient") as mock_remote_cls:
            mock_remote_cls.side_effect = RuntimeError("bad host")
            resp = client.get("/api/streams/search?q=anything")

        assert resp.status_code == 200
        data = resp.json()
        assert any(r["name"] == "local-job" for r in data["results"])
