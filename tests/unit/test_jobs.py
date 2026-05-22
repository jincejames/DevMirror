"""Tests for devmirror.jobs entry-point helpers."""

from __future__ import annotations

import logging
import os
import sys
from types import SimpleNamespace
from unittest import mock

import pytest

from devmirror.jobs import (
    _apply_overrides_from_argv,
    _configure_logging,
    run_audit_purge,
    run_cleanup,
    run_notifications,
)
from devmirror.settings import load_settings


@pytest.fixture
def clean_env(monkeypatch):
    """Strip DEVMIRROR_* env vars so each test starts from a known baseline."""
    for key in list(os.environ):
        if key.startswith("DEVMIRROR_"):
            monkeypatch.delenv(key, raising=False)
    yield monkeypatch


class TestApplyOverridesFromArgv:
    def test_named_parameters_promoted_to_env_vars(self, clean_env) -> None:
        with mock.patch.object(
            sys, "argv",
            [
                "devmirror-cleanup",
                "--catalog=odp_adw_support_n",
                "--schema=devmirror",
                "--warehouse-id=bda66ae121230af9",
            ],
        ):
            _apply_overrides_from_argv()

        assert os.environ["DEVMIRROR_CONTROL_CATALOG"] == "odp_adw_support_n"
        assert os.environ["DEVMIRROR_CONTROL_SCHEMA"] == "devmirror"
        assert os.environ["DEVMIRROR_WAREHOUSE_ID"] == "bda66ae121230af9"

        # And load_settings() picks them up as the control FQN.
        s = load_settings()
        assert s.control_fqn_prefix == "odp_adw_support_n.devmirror"
        assert s.warehouse_id == "bda66ae121230af9"

    def test_no_args_is_a_noop(self, clean_env) -> None:
        with mock.patch.object(sys, "argv", ["devmirror-cleanup"]):
            _apply_overrides_from_argv()

        # Defaults remain unchanged.
        assert "DEVMIRROR_CONTROL_CATALOG" not in os.environ
        assert "DEVMIRROR_CONTROL_SCHEMA" not in os.environ
        assert "DEVMIRROR_WAREHOUSE_ID" not in os.environ

        s = load_settings()
        assert s.control_catalog == "dev_analytics"
        assert s.control_schema == "devmirror_admin"

    def test_preexisting_env_var_overridden_by_cli(self, clean_env) -> None:
        # If both env var AND CLI flag are set, CLI wins (the shim writes
        # AFTER reading argv, so the most-explicit signal -- the one
        # passed to this exact run -- always lands).
        clean_env.setenv("DEVMIRROR_CONTROL_CATALOG", "from_env")
        clean_env.setenv("DEVMIRROR_CONTROL_SCHEMA", "from_env")
        with mock.patch.object(
            sys, "argv",
            ["devmirror-cleanup", "--catalog=from_cli", "--schema=from_cli"],
        ):
            _apply_overrides_from_argv()

        assert os.environ["DEVMIRROR_CONTROL_CATALOG"] == "from_cli"
        assert os.environ["DEVMIRROR_CONTROL_SCHEMA"] == "from_cli"

    def test_unknown_args_ignored(self, clean_env) -> None:
        # parse_known_args swallows unrelated flags so existing CLI calls
        # that pass other flags still work.
        with mock.patch.object(
            sys, "argv",
            ["devmirror-cleanup", "--catalog=foo", "--unknown=bar", "positional"],
        ):
            _apply_overrides_from_argv()

        assert os.environ["DEVMIRROR_CONTROL_CATALOG"] == "foo"


# ---------------------------------------------------------------------------
# Lifecycle-job logging
# ---------------------------------------------------------------------------


@pytest.fixture
def reset_root_logger():
    """Snapshot + restore the root logger so test runs that exercise
    _configure_logging() don't leak handlers into other tests."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level
    yield
    root.handlers = saved_handlers
    root.level = saved_level


class TestConfigureLogging:
    def test_attaches_stream_handler_at_info_when_root_is_bare(
        self, reset_root_logger,
    ):
        root = logging.getLogger()
        root.handlers = []
        root.setLevel(logging.WARNING)

        _configure_logging()

        assert len(root.handlers) >= 1
        assert root.level <= logging.INFO

    def test_does_not_attach_duplicate_handler_on_second_call(
        self, reset_root_logger,
    ):
        root = logging.getLogger()
        root.handlers = []
        root.setLevel(logging.WARNING)

        _configure_logging()
        n_after_first = len(root.handlers)
        _configure_logging()
        assert len(root.handlers) == n_after_first

    def test_lowers_level_to_info_if_existing_handler_is_higher(
        self, reset_root_logger,
    ):
        root = logging.getLogger()
        root.handlers = [logging.NullHandler()]
        root.setLevel(logging.ERROR)

        _configure_logging()

        assert root.level <= logging.INFO


def _patch_jobs_internals(monkeypatch, **overrides):
    """Patch jobs.* helpers so the entry points can run end-to-end without a
    real Databricks workspace, control catalog, or repos.

    Returns the MagicMock for the engine function so callers can shape the
    return value (e.g. set the cleanup result).
    """
    import devmirror.jobs as jobs_mod

    # _build_context returns a 6-tuple; we just need stable mocks.
    dummy_db = mock.MagicMock()
    dummy_settings = SimpleNamespace(
        default_notification_days=[1, 7],
        audit_retention_days=90,
    )
    dummy_repos = tuple(mock.MagicMock() for _ in range(4))
    monkeypatch.setattr(
        jobs_mod, "_build_context",
        lambda: (dummy_db, dummy_settings, *dummy_repos),
    )
    return jobs_mod


class TestRunCleanupLogging:
    def test_no_expired_drs_logs_zero_eligible(
        self, monkeypatch, reset_root_logger, caplog,
    ):
        jobs_mod = _patch_jobs_internals(monkeypatch)
        monkeypatch.setattr(
            "devmirror.cleanup.cleanup_engine.find_expired_drs",
            lambda *a, **kw: [],
        )
        with caplog.at_level(logging.INFO, logger="devmirror.jobs"):
            run_cleanup()
        messages = [r.message for r in caplog.records]
        assert any("Cleanup run starting" in m for m in messages)
        assert any("0 DRs eligible" in m for m in messages)

    def test_per_dr_and_aggregate_summary_emitted(
        self, monkeypatch, reset_root_logger, caplog,
    ):
        _patch_jobs_internals(monkeypatch)
        monkeypatch.setattr(
            "devmirror.cleanup.cleanup_engine.find_expired_drs",
            lambda *a, **kw: [
                {"dr_id": "DR-00001", "status": "ACTIVE"},
                {"dr_id": "DR-00002", "status": "ACTIVE"},
            ],
        )
        # Two DRs, two outcomes: one fully cleaned, one partial.
        results = [
            SimpleNamespace(
                fully_cleaned=True, objects_dropped=8, schemas_dropped=2,
                revokes_succeeded=4, objects_failed=[], schemas_failed=[],
                revokes_failed=[],
            ),
            SimpleNamespace(
                fully_cleaned=False, objects_dropped=5, schemas_dropped=1,
                revokes_succeeded=2, objects_failed=[("x", "e")],
                schemas_failed=[("y", "e"), ("z", "e")],
                revokes_failed=[],
            ),
        ]
        monkeypatch.setattr(
            "devmirror.cleanup.cleanup_engine.cleanup_dr",
            mock.MagicMock(side_effect=results),
        )
        # Bypass DRStatus.__call__ on the raw "ACTIVE" string -- pretend it
        # parses cleanly.  The real class accepts ACTIVE; this is just to
        # avoid pulling enum import into the test.
        monkeypatch.setattr(
            "devmirror.control.control_table.DRStatus",
            lambda v: v,
        )
        with caplog.at_level(logging.INFO, logger="devmirror.jobs"):
            run_cleanup()
        msgs = "\n".join(r.message for r in caplog.records)
        # Per-DR lines surface object / schema / revoke counts.
        assert "DR-00001: fully cleaned -- 8 objects, 2 schemas, 4 revokes" in msgs
        assert "DR-00002: partial -- 5 objects / 1 schemas / 2 revokes" in msgs
        # Aggregate sums and DR-bucket counts in the final summary.
        assert "1/2 DRs fully cleaned" in msgs
        assert "1 partial" in msgs
        assert "13 objects dropped" in msgs   # 8 + 5
        assert "3 schemas dropped" in msgs    # 2 + 1
        assert "6 revokes" in msgs            # 4 + 2


class TestRunNotificationsLogging:
    def test_aggregate_summary_emitted(
        self, monkeypatch, reset_root_logger, caplog,
    ):
        _patch_jobs_internals(monkeypatch)
        result = SimpleNamespace(notified=3, failed=[], skipped=1)
        monkeypatch.setattr(
            "devmirror.cleanup.notifier.notify_expiring_drs",
            mock.MagicMock(return_value=result),
        )
        monkeypatch.setattr(
            "devmirror.cleanup.notifier.LoggingBackend",
            mock.MagicMock(),
        )
        with caplog.at_level(logging.INFO, logger="devmirror.jobs"):
            run_notifications()
        msgs = "\n".join(r.message for r in caplog.records)
        assert "Notification run starting" in msgs
        assert "3 sent, 0 failed, 1 skipped" in msgs


class TestRunAuditPurgeLogging:
    def test_summary_emits_deleted_count_and_retention(
        self, monkeypatch, reset_root_logger, caplog,
    ):
        jobs_mod = _patch_jobs_internals(monkeypatch)
        # The audit_repo is the LAST entry in the 6-tuple from _build_context;
        # rebind it with a MagicMock that returns a known purge count.
        import devmirror.jobs as jm
        original_build = jm._build_context
        purger = mock.MagicMock(return_value=42)

        def patched_build_context():
            tup = original_build()
            audit_repo = mock.MagicMock()
            audit_repo.purge_old_entries = purger
            return (*tup[:-1], audit_repo)

        monkeypatch.setattr(jobs_mod, "_build_context", patched_build_context)
        with caplog.at_level(logging.INFO, logger="devmirror.jobs"):
            run_audit_purge()
        msgs = "\n".join(r.message for r in caplog.records)
        assert "Audit purge starting" in msgs
        assert "42 entries removed" in msgs
        assert "retention=90 days" in msgs
