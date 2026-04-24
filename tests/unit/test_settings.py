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
        # Stage 4 US-34: defaults for auto-generated DR IDs.
        assert s.dr_id_prefix == "DR"
        assert s.dr_id_padding == 5

    def test_dr_id_env_overrides(self) -> None:
        env = _env(
            DEVMIRROR_DR_ID_PREFIX="PROJ",
            DEVMIRROR_DR_ID_PADDING="6",
        )
        with patch.dict(os.environ, env, clear=True):
            s = load_settings()
        assert s.dr_id_prefix == "PROJ"
        assert s.dr_id_padding == 6

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
# Stage 4 US-35: DR ID prefix / padding validation
# ===========================================================================

class TestDrIdPrefixValidation:
    @pytest.mark.parametrize(
        "prefix",
        [
            "DR",           # default
            "A",            # single letter is allowed
            "PROJ",
            "Ab1",          # mixed case + digits
            "abcdefgh",     # max 8 characters
            "Z9",
            # US-35 extra boundary / coverage cases:
            "AB345678",     # exact 8-char boundary with digits + uppercase
            "dr",           # lowercase-only (regex allows [A-Za-z])
            "d",            # lowercase single letter
            "aA1",          # lowercase + uppercase + digit
        ],
    )
    def test_valid_prefix(self, prefix: str) -> None:
        env = _env(DEVMIRROR_DR_ID_PREFIX=prefix)
        with patch.dict(os.environ, env, clear=True):
            s = load_settings()
        assert s.dr_id_prefix == prefix

    @pytest.mark.parametrize(
        "prefix",
        [
            "1bad",          # must start with a letter
            "9DR",           # starts with a digit
            "",              # explicitly blank (distinct from unset)
            "   ",           # whitespace-only is also blank after strip
            "DR-1",          # hyphen is not alphanumeric
            "DR_1",          # underscore is not alphanumeric
            "DR 1",          # space is not alphanumeric
            "abcdefghi",     # 9 characters exceeds max of 8
            "LONGPREFIX",    # 10 characters exceeds max of 8
            # US-35 extra boundary / coverage cases:
            "ABC456789",     # exact 9-char boundary (one over max)
            "DRé",     # non-ASCII letter (unicode) is not permitted
            "éDR",     # starts with a non-ASCII letter
            "DR!",           # punctuation other than alnum
            "DR.1",          # dot is not alphanumeric
        ],
    )
    def test_invalid_prefix_raises(self, prefix: str) -> None:
        env = _env(DEVMIRROR_DR_ID_PREFIX=prefix)
        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(
                SettingsError,
                match=r"DEVMIRROR_DR_ID_PREFIX must be alphanumeric",
            ):
                load_settings()

    def test_unset_prefix_falls_back_to_default(self) -> None:
        """When DEVMIRROR_DR_ID_PREFIX is not set at all, default 'DR' is used."""
        env = _env()
        env.pop("DEVMIRROR_DR_ID_PREFIX", None)
        with patch.dict(os.environ, env, clear=True):
            s = load_settings()
            assert s.dr_id_prefix == "DR"


class TestDrIdPaddingValidation:
    @pytest.mark.parametrize("padding", [3, 4, 5, 6, 10, 12])
    def test_valid_padding(self, padding: int) -> None:
        env = _env(DEVMIRROR_DR_ID_PADDING=str(padding))
        with patch.dict(os.environ, env, clear=True):
            s = load_settings()
        assert s.dr_id_padding == padding

    @pytest.mark.parametrize("padding", [0, 1, 2, 13, 20, 99, -1])
    def test_out_of_range_padding_raises(self, padding: int) -> None:
        env = _env(DEVMIRROR_DR_ID_PADDING=str(padding))
        with patch.dict(os.environ, env, clear=True), pytest.raises(
            SettingsError,
            match=r"DEVMIRROR_DR_ID_PADDING must be between 3 and 12 inclusive",
        ):
            load_settings()

    def test_non_integer_padding_raises(self) -> None:
        env = _env(DEVMIRROR_DR_ID_PADDING="not-a-number")
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
