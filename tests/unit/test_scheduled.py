"""Tests for devmirror.jobs -- scheduled entrypoints."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture()
def mock_context():
    mock_sql = MagicMock()
    mock_settings = MagicMock()
    mock_settings.default_notification_days = 7
    mock_settings.audit_retention_days = 365
    mock_dr_repo = MagicMock()
    mock_obj_repo = MagicMock()
    mock_access_repo = MagicMock()
    mock_audit_repo = MagicMock()
    mock_audit_repo.purge_old_entries.return_value = 0
    return (mock_sql, mock_settings, mock_dr_repo, mock_obj_repo, mock_access_repo, mock_audit_repo)


class TestRunNotifications:
    @patch("devmirror.jobs._build_context")
    def test_calls_notifier(self, mock_build, mock_context):
        mock_build.return_value = mock_context
        with patch("devmirror.cleanup.notifier.notify_expiring_drs") as mock_notify:
            mock_notify.return_value = MagicMock(notified=2, failed=[], skipped=0)
            from devmirror.jobs import run_notifications
            run_notifications()
            mock_notify.assert_called_once()

    @patch("devmirror.jobs._build_context")
    def test_propagates_exception(self, mock_build):
        mock_build.side_effect = RuntimeError("Settings missing")
        from devmirror.jobs import run_notifications
        with pytest.raises(RuntimeError, match="Settings missing"):
            run_notifications()


class TestRunCleanup:
    @patch("devmirror.jobs._build_context")
    def test_no_expired_drs(self, mock_build, mock_context):
        mock_build.return_value = mock_context
        with patch("devmirror.cleanup.cleanup_engine.find_expired_drs", return_value=[]):
            from devmirror.jobs import run_cleanup
            run_cleanup()

    @patch("devmirror.jobs._build_context")
    def test_processes_expired_drs(self, mock_build, mock_context):
        mock_build.return_value = mock_context
        expired = [{"dr_id": "DR-100", "status": "ACTIVE"}, {"dr_id": "DR-200", "status": "CLEANUP_IN_PROGRESS"}]
        with (
            patch("devmirror.cleanup.cleanup_engine.find_expired_drs", return_value=expired),
            patch("devmirror.cleanup.cleanup_engine.cleanup_dr", return_value=MagicMock(fully_cleaned=True)) as mock_cleanup,
        ):
            from devmirror.jobs import run_cleanup
            run_cleanup()
            assert mock_cleanup.call_count == 2

    @patch("devmirror.jobs._build_context")
    def test_continues_on_per_dr_failure(self, mock_build, mock_context):
        mock_build.return_value = mock_context
        expired = [{"dr_id": "DR-100", "status": "ACTIVE"}, {"dr_id": "DR-200", "status": "ACTIVE"}]
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("First DR fails")
            return MagicMock(fully_cleaned=True)

        with (
            patch("devmirror.cleanup.cleanup_engine.find_expired_drs", return_value=expired),
            patch("devmirror.cleanup.cleanup_engine.cleanup_dr", side_effect=side_effect) as mock_cleanup,
        ):
            from devmirror.jobs import run_cleanup
            run_cleanup()
            assert mock_cleanup.call_count == 2


class TestRunAuditPurge:
    @patch("devmirror.jobs._build_context")
    def test_calls_purge(self, mock_build, mock_context):
        mock_context[5].purge_old_entries.return_value = 5  # audit_repo
        mock_build.return_value = mock_context
        from devmirror.jobs import run_audit_purge
        run_audit_purge()
        mock_context[5].purge_old_entries.assert_called_once()
