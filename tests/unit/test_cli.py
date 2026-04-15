"""Tests for the devmirror CLI entrypoint and subcommands."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from devmirror.cli import main

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.fixture()
def runner():
    return CliRunner()


@pytest.fixture()
def mock_settings():
    settings = MagicMock()
    settings.warehouse_id = "test-warehouse"
    settings.control_fqn_prefix = "dev_analytics.devmirror_admin"
    settings.control_catalog = "dev_analytics"
    settings.control_schema = "devmirror_admin"
    return settings


# ===========================================================================
# Basic CLI
# ===========================================================================

class TestCLIEntrypoint:
    def test_help(self, runner) -> None:
        result = runner.invoke(main, ["--help"])
        assert result.exit_code == 0
        assert "DevMirror" in result.output

    def test_version(self, runner) -> None:
        result = runner.invoke(main, ["--version"])
        assert result.exit_code == 0
        assert "0.1.0" in result.output

    def test_validate_valid_config(self, runner) -> None:
        result = runner.invoke(main, ["validate", "--config", str(FIXTURES / "valid_minimal.yaml")])
        assert result.exit_code == 0
        assert "DR-1042" in result.output

    def test_validate_invalid_config(self, runner, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text("version: '1.0'\ndevelopment_request:\n  dr_id: 'bad'\n", encoding="utf-8")
        result = runner.invoke(main, ["validate", "--config", str(bad)])
        assert result.exit_code != 0


# ===========================================================================
# Scan CLI
# ===========================================================================

def _mock_resolve_streams(client, stream_names):
    from devmirror.scan.stream_resolver import ResolvedStream
    resolved = [
        ResolvedStream(name=name, resource_type="job", resource_id=str(i + 100), task_keys=["task_a"])
        for i, name in enumerate(stream_names)
    ]
    return resolved, []


def _mock_query_lineage(sql_executor, streams, lineage_table=None, row_limit=10000):
    from devmirror.scan.lineage import LineageEdge, LineageResult
    edges = [LineageEdge(source_table_fqn="prod.sch.src", target_table_fqn="prod.sch.tgt", source_type="TABLE", target_type="TABLE", entity_id="100")]
    return LineageResult(edges=edges, row_limit_hit=False)


class TestScanCLI:
    @patch("devmirror.scan.lineage.get_enrichment_table", return_value=None)
    @patch("devmirror.scan.lineage.query_lineage", side_effect=_mock_query_lineage)
    @patch("devmirror.scan.stream_resolver.resolve_streams", side_effect=_mock_resolve_streams)
    @patch("devmirror.settings.load_settings")
    @patch("databricks.sdk.WorkspaceClient")
    def test_scan_success(self, mock_ws, mock_settings_fn, mock_resolve, mock_lineage, mock_enrichment, tmp_path: Path) -> None:
        from devmirror.settings import Settings
        mock_settings_fn.return_value = Settings()
        output = tmp_path / "manifest.yaml"
        result = runner_invoke(["scan", "--config", str(FIXTURES / "valid_minimal.yaml"), "--output", str(output)])
        assert result.exit_code == 0
        assert output.exists()

    @patch("devmirror.scan.stream_resolver.resolve_streams", return_value=([], ["stream_a"]))
    @patch("devmirror.settings.load_settings")
    @patch("databricks.sdk.WorkspaceClient")
    def test_scan_unresolved_streams(self, mock_ws, mock_settings_fn, mock_resolve, tmp_path: Path) -> None:
        from devmirror.settings import Settings
        mock_settings_fn.return_value = Settings()
        result = runner_invoke(["scan", "--config", str(FIXTURES / "valid_minimal.yaml"), "--output", str(tmp_path / "out.yaml")])
        assert result.exit_code == 1

    def test_scan_help(self, runner) -> None:
        result = runner.invoke(main, ["scan", "--help"])
        assert result.exit_code == 0
        assert "--config" in result.output


# ===========================================================================
# Cleanup CLI
# ===========================================================================

def _cleanup_result(final_status="CLEANED_UP", dropped=5, skipped=0, failed=None,
                    revokes=3, rev_failed=None, schemas=2, sch_failed=None):
    m = MagicMock()
    m.dr_id, m.final_status = "DR-1042", final_status
    m.objects_dropped, m.objects_skipped = dropped, skipped
    m.objects_failed = failed or []
    m.revokes_succeeded, m.revokes_failed = revokes, rev_failed or []
    m.schemas_dropped, m.schemas_failed = schemas, sch_failed or []
    return m


class TestCleanupCommand:
    def test_cleanup_success(self, runner, mock_settings):
        mock_result = _cleanup_result()
        with (
            patch("devmirror.settings.load_settings", return_value=mock_settings),
            patch("databricks.sdk.WorkspaceClient"),
            patch("devmirror.utils.db_client.DbClient"),
            patch("devmirror.cleanup.cleanup_engine.cleanup_dr", return_value=mock_result),
        ):
            result = runner.invoke(main, ["cleanup", "--dr-id", "DR-1042"])
            assert result.exit_code == 0
            assert "CLEANED_UP" in result.output

    def test_cleanup_partial_failure_exits_1(self, runner, mock_settings):
        mock_result = _cleanup_result(
            final_status="CLEANUP_IN_PROGRESS", dropped=3, revokes=1,
            failed=[("dev.s.t1", "error")], schemas=0,
        )
        with (
            patch("devmirror.settings.load_settings", return_value=mock_settings),
            patch("databricks.sdk.WorkspaceClient"),
            patch("devmirror.utils.db_client.DbClient"),
            patch("devmirror.cleanup.cleanup_engine.cleanup_dr", return_value=mock_result),
        ):
            result = runner.invoke(main, ["cleanup", "--dr-id", "DR-1042"])
            assert result.exit_code == 1

    def test_cleanup_requires_dr_id(self, runner):
        result = runner.invoke(main, ["cleanup"])
        assert result.exit_code != 0


# ===========================================================================
# Status CLI
# ===========================================================================

class TestStatusCommand:
    def _setup_patches(self, mock_settings, dr_row, objects, audits):
        mock_dr_repo = MagicMock()
        mock_dr_repo.get.return_value = dr_row
        mock_obj_repo = MagicMock()
        mock_obj_repo.list_by_dr_id.return_value = objects
        mock_audit_repo = MagicMock()
        mock_audit_repo.list_by_dr_id.return_value = audits
        return (
            patch("devmirror.settings.load_settings", return_value=mock_settings),
            patch("databricks.sdk.WorkspaceClient"),
            patch("devmirror.utils.db_client.DbClient"),
            patch("devmirror.control.control_table.DRRepository", return_value=mock_dr_repo),
            patch("devmirror.control.control_table.DrObjectRepository", return_value=mock_obj_repo),
            patch("devmirror.control.audit.AuditRepository", return_value=mock_audit_repo),
        )

    def test_status_human_output(self, runner, mock_settings):
        dr_row = {"dr_id": "DR-100", "status": "ACTIVE", "description": "Test", "expiration_date": "2026-05-01", "created_at": "2026-04-01", "last_refreshed_at": "2026-04-10", "last_modified_at": "2026-04-10", "notification_sent_at": None}
        objects = [{"status": "PROVISIONED"}, {"status": "FAILED"}]
        audits = [{"action": "PROVISION", "status": "SUCCESS", "performed_at": "2026-04-01"}]
        patches = self._setup_patches(mock_settings, dr_row, objects, audits)
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = runner.invoke(main, ["status", "--dr-id", "DR-100"])
            assert result.exit_code == 0
            assert "ACTIVE" in result.output

    def test_status_json_output(self, runner, mock_settings):
        import json
        dr_row = {"dr_id": "DR-100", "status": "ACTIVE", "description": "Test", "expiration_date": "2026-05-01", "created_at": "2026-04-01", "last_refreshed_at": None, "last_modified_at": None, "notification_sent_at": None}
        patches = self._setup_patches(mock_settings, dr_row, [], [])
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = runner.invoke(main, ["status", "--dr-id", "DR-100", "--json"])
            assert result.exit_code == 0
            payload = json.loads(result.output)
            assert payload["dr_id"] == "DR-100"

    def test_status_not_found(self, runner, mock_settings):
        patches = self._setup_patches(mock_settings, None, [], [])
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            result = runner.invoke(main, ["status", "--dr-id", "DR-9999"])
            assert result.exit_code != 0


# ===========================================================================
# List CLI
# ===========================================================================

class TestListCommand:
    def test_list_shows_active_drs(self, runner, mock_settings):
        mock_dr_repo = MagicMock()
        mock_dr_repo.list_active.return_value = [
            {"dr_id": "DR-100", "status": "ACTIVE", "expiration_date": "2026-05-01", "description": "First"},
            {"dr_id": "DR-200", "status": "PROVISIONING", "expiration_date": "2026-06-01", "description": "Second with long desc that is truncated"},
        ]
        with (
            patch("devmirror.settings.load_settings", return_value=mock_settings),
            patch("databricks.sdk.WorkspaceClient"),
            patch("devmirror.utils.db_client.DbClient"),
            patch("devmirror.control.control_table.DRRepository", return_value=mock_dr_repo),
        ):
            result = runner.invoke(main, ["list"])
            assert result.exit_code == 0
            assert "DR-100" in result.output

    def test_list_empty(self, runner, mock_settings):
        mock_dr_repo = MagicMock()
        mock_dr_repo.list_active.return_value = []
        with (
            patch("devmirror.settings.load_settings", return_value=mock_settings),
            patch("databricks.sdk.WorkspaceClient"),
            patch("devmirror.utils.db_client.DbClient"),
            patch("devmirror.control.control_table.DRRepository", return_value=mock_dr_repo),
        ):
            result = runner.invoke(main, ["list"])
            assert "No active development requests found" in result.output


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def runner_invoke(args):
    return CliRunner().invoke(main, args)
