"""Tests for devmirror.utils.db_client.DbClient targeted behaviors."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from devmirror.utils.db_client import DbClient


def _client() -> tuple[DbClient, MagicMock]:
    """Build a DbClient with its underlying SDK WorkspaceClient mocked.

    DbClient's __init__ accepts a WorkspaceClient; here we bypass it
    and set the attribute directly so the SDK isn't actually constructed.
    """
    db = DbClient.__new__(DbClient)
    sdk = MagicMock()
    db._client = sdk
    return db, sdk


class TestDeleteSchema:
    """delete_schema must pass force=True so non-empty schemas drop
    instead of silently no-opping, and exceptions must propagate so
    cleanup_engine can record schemas_failed."""

    def test_passes_force_true(self) -> None:
        db, sdk = _client()
        db.delete_schema("cat", "sch")
        sdk.schemas.delete.assert_called_once_with("cat.sch", force=True)

    def test_propagates_exceptions(self) -> None:
        db, sdk = _client()
        sdk.schemas.delete.side_effect = RuntimeError("permission denied")
        with pytest.raises(RuntimeError, match="permission denied"):
            db.delete_schema("cat", "sch")
