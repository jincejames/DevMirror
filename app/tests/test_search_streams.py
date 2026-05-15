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
            # "only" is a substring of all three names -- jobs are filtered
            # client-side, pipelines server-side via LIKE (mocked, so the
            # filter arg is just passed through).
            resp = client.get("/api/streams/search?q=only")

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


class TestSearchStreamsJobsSubstringMatch:
    """Regression: the Databricks Jobs 2.2 list API only supports exact
    (case-insensitive) name match via the ``name`` query param, with no
    LIKE/contains filter.  We now do client-side substring filtering so
    typing a prefix or partial name returns matches.  Originally the search
    only returned a result when the FULL job name was typed.
    """

    def test_prefix_substring_and_case_insensitive_match(self, client, mock_db, monkeypatch):
        monkeypatch.delenv("DEVMIRROR_REMOTE_WORKSPACE_HOST", raising=False)
        monkeypatch.delenv("DEVMIRROR_REMOTE_WORKSPACE_TOKEN", raising=False)

        # Mimic the LH workspace's job naming.
        _wire_local_ws(
            mock_db,
            jobs=[
                "[dev ashish_sinha_sp]b2b_data_load_job",
                "[dev ashish_sinha_sp]other_job",
                "[prod]unrelated_job",
            ],
        )

        with patch("backend.router.WorkspaceClient"):
            # Prefix match -- only the start of the name typed.
            resp = client.get("/api/streams/search?q=%5Bdev%20ashish")
        names = sorted(r["name"] for r in resp.json()["results"])
        assert names == [
            "[dev ashish_sinha_sp]b2b_data_load_job",
            "[dev ashish_sinha_sp]other_job",
        ]

        with patch("backend.router.WorkspaceClient"):
            # Mid-string substring.
            resp = client.get("/api/streams/search?q=b2b_data")
        names = [r["name"] for r in resp.json()["results"]]
        assert names == ["[dev ashish_sinha_sp]b2b_data_load_job"]

        with patch("backend.router.WorkspaceClient"):
            # Case-insensitive.
            resp = client.get("/api/streams/search?q=ASHISH")
        names = sorted(r["name"] for r in resp.json()["results"])
        assert names == [
            "[dev ashish_sinha_sp]b2b_data_load_job",
            "[dev ashish_sinha_sp]other_job",
        ]

    def test_jobs_list_called_without_name_filter(self, client, mock_db, monkeypatch):
        """The fix removes the server-side ``name=q`` param -- otherwise
        the SDK would still do an exact-match against the workspace and
        return nothing for partial queries.
        """
        monkeypatch.delenv("DEVMIRROR_REMOTE_WORKSPACE_HOST", raising=False)
        monkeypatch.delenv("DEVMIRROR_REMOTE_WORKSPACE_TOKEN", raising=False)

        _wire_local_ws(mock_db, jobs=["my-job"])

        with patch("backend.router.WorkspaceClient"):
            client.get("/api/streams/search?q=my")

        # SDK must NOT receive name=<q> -- it would gate on exact match.
        kwargs = mock_db.client.jobs.list.call_args.kwargs
        assert "name" not in kwargs


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
            resp = client.get("/api/streams/search?q=local")

        assert resp.status_code == 200
        data = resp.json()
        assert any(r["name"] == "local-job" for r in data["results"])


class TestRemoteAuthDispatch:
    """The remote WorkspaceClient construction dispatches between auth
    methods in this precedence order:

    1. DATABRICKS_CLIENT_ID + DATABRICKS_CLIENT_SECRET  -> OAuth M2M with
       the app's own auto-injected account-level credentials.
    2. DEVMIRROR_REMOTE_WORKSPACE_CLIENT_ID + _SECRET  -> OAuth M2M override
       (used only if path 1 is unset).
    3. DEVMIRROR_REMOTE_WORKSPACE_TOKEN  -> PAT (legacy).
    4. none of the above  -> remote search silently skipped.
    """

    def test_oauth_m2m_when_client_id_and_secret_set(
        self, client, mock_db, monkeypatch,
    ):
        # Path 2 (override): auto-injected DATABRICKS_CLIENT_* unset, so the
        # dispatcher falls through to the DEVMIRROR_REMOTE_WORKSPACE_CLIENT_*
        # override pair.
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.setenv(
            "DEVMIRROR_REMOTE_WORKSPACE_HOST", "https://preprod.example.net",
        )
        monkeypatch.setenv(
            "DEVMIRROR_REMOTE_WORKSPACE_ID", "1234567890",
        )
        monkeypatch.setenv(
            "DEVMIRROR_REMOTE_WORKSPACE_CLIENT_ID", "sp-app-id-guid",
        )
        monkeypatch.setenv(
            "DEVMIRROR_REMOTE_WORKSPACE_CLIENT_SECRET", "oauth-secret-xyz",
        )
        # PAT also set -- OAuth M2M must take precedence and the PAT-env
        # must be ignored entirely (no token kwarg in the call).
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_TOKEN", "should-be-ignored")

        _wire_local_ws(mock_db, jobs=[])
        remote_ws = MagicMock()
        remote_ws.jobs.list.return_value = []
        remote_ws.pipelines.list_pipelines.return_value = []

        # Patch Config in the dispatcher's import site so we can assert on
        # what was passed to it without actually contacting Databricks.
        with patch("backend.router.WorkspaceClient") as mock_remote_cls, \
                patch(
                    "databricks.sdk.config.Config",
                ) as mock_config_cls:
            mock_remote_cls.return_value = remote_ws
            client.get("/api/streams/search?q=anything")

        # Config built with the cross-workspace OAuth M2M kwargs.
        cfg_kwargs = mock_config_cls.call_args.kwargs
        assert cfg_kwargs.get("host") == "https://preprod.example.net"
        assert cfg_kwargs.get("client_id") == "sp-app-id-guid"
        assert cfg_kwargs.get("client_secret") == "oauth-secret-xyz"
        assert cfg_kwargs.get("auth_type") == "oauth-m2m"
        assert cfg_kwargs.get("workspace_id") == "1234567890"
        # WorkspaceClient receives the pre-built Config (not raw kwargs).
        ws_kwargs = mock_remote_cls.call_args.kwargs
        assert "config" in ws_kwargs
        # PAT token must not leak into the call.
        assert "token" not in ws_kwargs

    def test_oauth_m2m_prefers_auto_injected_app_creds(
        self, client, mock_db, monkeypatch,
    ):
        # Path 1: when DATABRICKS_CLIENT_ID/_SECRET are set (auto-injected
        # by the Apps runtime), they win even if the DEVMIRROR_* override
        # pair is also set.  The auto-injected ones are account-level and
        # work cross-workspace; the override is for the edge case of a
        # different SP.
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-own-sp-id")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "account-level-secret")
        monkeypatch.setenv(
            "DEVMIRROR_REMOTE_WORKSPACE_HOST", "https://preprod.example.net",
        )
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_ID", "1234567890")
        monkeypatch.setenv(
            "DEVMIRROR_REMOTE_WORKSPACE_CLIENT_ID", "override-sp-id",
        )
        monkeypatch.setenv(
            "DEVMIRROR_REMOTE_WORKSPACE_CLIENT_SECRET", "override-secret",
        )

        _wire_local_ws(mock_db, jobs=[])
        remote_ws = MagicMock()
        remote_ws.jobs.list.return_value = []
        remote_ws.pipelines.list_pipelines.return_value = []

        with patch("backend.router.WorkspaceClient") as mock_remote_cls, \
                patch("databricks.sdk.config.Config") as mock_config_cls:
            mock_remote_cls.return_value = remote_ws
            client.get("/api/streams/search?q=anything")

        cfg_kwargs = mock_config_cls.call_args.kwargs
        assert cfg_kwargs.get("client_id") == "app-own-sp-id"
        assert cfg_kwargs.get("client_secret") == "account-level-secret"
        assert cfg_kwargs.get("host") == "https://preprod.example.net"
        assert cfg_kwargs.get("auth_type") == "oauth-m2m"
        assert cfg_kwargs.get("workspace_id") == "1234567890"

    def test_oauth_m2m_with_empty_workspace_id_env(
        self, client, mock_db, monkeypatch,
    ):
        # When DEVMIRROR_REMOTE_WORKSPACE_ID is unset, the dispatcher passes
        # an empty string -- still better than letting the auto-injected
        # DATABRICKS_WORKSPACE_ID leak in, since empty is treated as
        # "explicitly unset" by Config._set_inner_config (the env-load step
        # is skipped because the attr is already in _inner).
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.setenv(
            "DEVMIRROR_REMOTE_WORKSPACE_HOST", "https://preprod.example.net",
        )
        monkeypatch.delenv("DEVMIRROR_REMOTE_WORKSPACE_ID", raising=False)
        monkeypatch.setenv(
            "DEVMIRROR_REMOTE_WORKSPACE_CLIENT_ID", "sp-app-id-guid",
        )
        monkeypatch.setenv(
            "DEVMIRROR_REMOTE_WORKSPACE_CLIENT_SECRET", "oauth-secret-xyz",
        )
        monkeypatch.delenv("DEVMIRROR_REMOTE_WORKSPACE_TOKEN", raising=False)

        _wire_local_ws(mock_db, jobs=[])
        remote_ws = MagicMock()
        remote_ws.jobs.list.return_value = []
        remote_ws.pipelines.list_pipelines.return_value = []

        with patch("backend.router.WorkspaceClient") as mock_remote_cls, \
                patch("databricks.sdk.config.Config") as mock_config_cls:
            mock_remote_cls.return_value = remote_ws
            client.get("/api/streams/search?q=anything")

        assert mock_config_cls.call_args.kwargs.get("workspace_id") == ""

    def test_pat_fallback_when_only_token_set(
        self, client, mock_db, monkeypatch,
    ):
        monkeypatch.setenv(
            "DEVMIRROR_REMOTE_WORKSPACE_HOST", "https://dev.example.net",
        )
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_TOKEN", "fake-pat")
        # Ensure all OAuth pairs are unset so the dispatcher falls to PAT.
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.delenv(
            "DEVMIRROR_REMOTE_WORKSPACE_CLIENT_ID", raising=False,
        )
        monkeypatch.delenv(
            "DEVMIRROR_REMOTE_WORKSPACE_CLIENT_SECRET", raising=False,
        )

        _wire_local_ws(mock_db, jobs=[])
        remote_ws = MagicMock()
        remote_ws.jobs.list.return_value = []
        remote_ws.pipelines.list_pipelines.return_value = []

        with patch("backend.router.WorkspaceClient") as mock_remote_cls:
            mock_remote_cls.return_value = remote_ws
            client.get("/api/streams/search?q=anything")

        kwargs = mock_remote_cls.call_args.kwargs
        assert kwargs.get("host") == "https://dev.example.net"
        assert kwargs.get("token") == "fake-pat"
        assert kwargs.get("auth_type") == "pat"
        # OAuth fields must not have leaked.
        assert "client_id" not in kwargs
        assert "client_secret" not in kwargs

    def test_no_auth_skips_remote_silently(
        self, client, mock_db, monkeypatch, caplog,
    ):
        # Host set but no auth -> dispatcher returns None.  Remote search
        # is skipped with a WARNING; local results still come back.
        import logging

        monkeypatch.setenv(
            "DEVMIRROR_REMOTE_WORKSPACE_HOST", "https://x.example.net",
        )
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.delenv(
            "DEVMIRROR_REMOTE_WORKSPACE_CLIENT_ID", raising=False,
        )
        monkeypatch.delenv(
            "DEVMIRROR_REMOTE_WORKSPACE_CLIENT_SECRET", raising=False,
        )
        monkeypatch.delenv("DEVMIRROR_REMOTE_WORKSPACE_TOKEN", raising=False)

        _wire_local_ws(mock_db, jobs=["local-only"])

        with patch("backend.router.WorkspaceClient") as mock_remote_cls, \
                caplog.at_level(logging.WARNING, logger="backend.router"):
            resp = client.get("/api/streams/search?q=local")

        assert resp.status_code == 200
        # WorkspaceClient must NOT have been constructed -- dispatcher
        # short-circuited at "no creds".
        mock_remote_cls.assert_not_called()
        assert any(
            "no auth credentials" in r.message for r in caplog.records
        )

    def test_empty_strings_treated_as_unset(
        self, client, mock_db, monkeypatch,
    ):
        # Bundle / app.yaml commonly use `value: ""` for "leave unset".
        # The dispatcher must NOT treat empty strings as configured.
        monkeypatch.setenv(
            "DEVMIRROR_REMOTE_WORKSPACE_HOST", "https://x.example.net",
        )
        monkeypatch.setenv("DATABRICKS_CLIENT_ID", "")
        monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "")
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_CLIENT_ID", "")
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_CLIENT_SECRET", "")
        monkeypatch.setenv("DEVMIRROR_REMOTE_WORKSPACE_TOKEN", "")

        _wire_local_ws(mock_db, jobs=["local-only"])

        with patch("backend.router.WorkspaceClient") as mock_remote_cls:
            client.get("/api/streams/search?q=local")

        mock_remote_cls.assert_not_called()
