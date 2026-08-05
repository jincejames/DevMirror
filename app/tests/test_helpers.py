"""Tests for app.backend.helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import backend.helpers as helpers
from backend.helpers import _get_repo


def _reset_flag():
    helpers._table_ensured = False


def test_bootstrap_flag_stays_false_on_failure():
    """A transient apply_control_ddl failure must NOT latch _table_ensured,
    so the next request retries the bootstrap instead of skipping it for the
    life of the process."""
    _reset_flag()
    settings = MagicMock()
    settings.control_fqn_prefix = "c.s"
    db = MagicMock()

    with patch(
        "devmirror.control.control_table.apply_control_ddl",
        side_effect=RuntimeError("transient warehouse error"),
    ) as mock_ddl:
        _get_repo(settings, db)  # must not raise (best-effort)

    assert mock_ddl.called
    assert helpers._table_ensured is False  # not latched on failure

    # A subsequent call retries the DDL (flag still False), and on success latches.
    with patch(
        "devmirror.control.control_table.apply_control_ddl",
    ) as mock_ddl2:
        _get_repo(settings, db)
        assert mock_ddl2.called  # retried
    assert helpers._table_ensured is True


def test_bootstrap_runs_once_on_success():
    """On success the flag latches and apply_control_ddl is not re-run."""
    _reset_flag()
    settings = MagicMock()
    settings.control_fqn_prefix = "c.s"
    db = MagicMock()

    with patch("devmirror.control.control_table.apply_control_ddl") as mock_ddl:
        _get_repo(settings, db)
        assert helpers._table_ensured is True
        assert mock_ddl.call_count == 1
        # Second call: flag latched, DDL not re-run.
        _get_repo(settings, db)
        assert mock_ddl.call_count == 1

    _reset_flag()
