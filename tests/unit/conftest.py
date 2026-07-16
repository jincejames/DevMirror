"""Shared fixtures for unit tests.

The historical pattern was a per-file ``_mock_db()`` function that built
a MagicMock and wired up SQL-client methods.  Five+ near-identical copies
existed before this consolidation.  ``make_mock_db()`` here provides the
canonical SQL surface; tests that need extras (``create_schema``,
``grant``, ``delete_table``, ...) attach them locally on top.
"""

from __future__ import annotations

from unittest.mock import MagicMock


def make_mock_db(rows: list | None = None) -> MagicMock:
    """Return a MagicMock with the standard DbClient SQL surface wired.

    ``sql_with_params`` delegates to ``sql`` so tests that set
    ``mock_db.sql.return_value`` / ``side_effect`` continue to work after
    the repository switched from ``sql()`` to ``sql_with_params()``.
    """
    m = MagicMock()
    m.sql_exec = MagicMock()
    m.sql_exec_with_params = MagicMock()
    m.sql = MagicMock(return_value=rows if rows is not None else [])
    m.sql_with_params.side_effect = lambda stmt, params=None: m.sql(stmt, params)
    return m
