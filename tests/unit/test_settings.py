"""Tests for devmirror.settings.

All tests manipulate environment variables only -- no network calls.
"""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from devmirror.settings import SettingsError, load_settings


def _env(**overrides: str) -> dict[str, str]:
    """Build a minimal valid env dict, applying overrides."""
    base: dict[str, str] = {}
    base.update(overrides)
    return base


# ===========================================================================
# load_settings - happy path
# ===========================================================================

class TestLoadSettingsHappy:
    def test_minimal_env(self) -> None:
        with patch.dict(os.environ, _env(), clear=True):
            s = load_settings()
        assert s.warehouse_id is None
        assert s.control_catalog == "dev_analytics"
        assert s.control_schema == "devmirror_admin"
        assert s.workspace_profile is None
        assert s.max_dr_duration_days == 90
        assert s.default_notification_days == 7
        assert s.max_parallel_clones == 10
        assert s.shallow_clone_threshold_gb == 50
        assert s.audit_retention_days == 365
        assert s.lineage_system_table == "system.access.table_lineage"

    def test_with_warehouse_id(self) -> None:
        with patch.dict(os.environ, {"DEVMIRROR_WAREHOUSE_ID": "wh-test-123"}, clear=True):
            s = load_settings()
        assert s.warehouse_id == "wh-test-123"

    def test_all_overrides(self) -> None:
        env = _env(
            DEVMIRROR_WAREHOUSE_ID="wh-override",
            DEVMIRROR_CONTROL_CATALOG="custom_cat",
            DEVMIRROR_CONTROL_SCHEMA="custom_schema",
            DATABRICKS_CONFIG_PROFILE="my-profile",
            DEVMIRROR_MAX_DR_DURATION_DAYS="60",
            DEVMIRROR_DEFAULT_NOTIFICATION_DAYS="14",
            DEVMIRROR_MAX_PARALLEL_CLONES="5",
            DEVMIRROR_SHALLOW_CLONE_THRESHOLD_GB="100",
            DEVMIRROR_AUDIT_RETENTION_DAYS="180",
            DEVMIRROR_LINEAGE_SYSTEM_TABLE="custom.lineage.table",
        )
        with patch.dict(os.environ, env, clear=True):
            s = load_settings()
        assert s.warehouse_id == "wh-override"
        assert s.control_catalog == "custom_cat"
        assert s.control_schema == "custom_schema"
        assert s.workspace_profile == "my-profile"
        assert s.max_dr_duration_days == 60
        assert s.default_notification_days == 14
        assert s.max_parallel_clones == 5
        assert s.shallow_clone_threshold_gb == 100
        assert s.audit_retention_days == 180
        assert s.lineage_system_table == "custom.lineage.table"

    def test_control_fqn_prefix(self) -> None:
        with patch.dict(os.environ, _env(), clear=True):
            s = load_settings()
        assert s.control_fqn_prefix == "dev_analytics.devmirror_admin"


# ===========================================================================
# load_settings - error cases
# ===========================================================================

class TestLoadSettingsErrors:
    def test_missing_warehouse_id_succeeds(self) -> None:
        """Warehouse ID is now optional -- missing is OK."""
        with patch.dict(os.environ, {}, clear=True):
            s = load_settings()
        assert s.warehouse_id is None

    def test_empty_warehouse_id_succeeds(self) -> None:
        """Warehouse ID is now optional -- empty string yields None."""
        with patch.dict(os.environ, {"DEVMIRROR_WAREHOUSE_ID": "  "}, clear=True):
            s = load_settings()
        assert s.warehouse_id is None

    def test_non_integer_max_duration(self) -> None:
        env = _env(DEVMIRROR_MAX_DR_DURATION_DAYS="not-a-number")
        with patch.dict(os.environ, env, clear=True), pytest.raises(
            SettingsError, match="must be an integer"
        ):
            load_settings()

    def test_non_integer_max_parallel(self) -> None:
        env = _env(DEVMIRROR_MAX_PARALLEL_CLONES="abc")
        with patch.dict(os.environ, env, clear=True), pytest.raises(
            SettingsError, match="must be an integer"
        ):
            load_settings()


# ===========================================================================
# Settings frozen
# ===========================================================================

class TestSettingsImmutable:
    def test_frozen(self) -> None:
        with patch.dict(os.environ, _env(), clear=True):
            s = load_settings()
        with pytest.raises(AttributeError):
            s.warehouse_id = "changed"  # type: ignore[misc]
