"""DevMirror runtime settings loaded from DEVMIRROR_* environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


class SettingsError(Exception):
    """Raised when a required setting is missing or invalid."""


@dataclass(frozen=True)
class Settings:
    """Immutable runtime settings for a DevMirror session."""

    warehouse_id: str | None = None
    control_catalog: str = "dev_analytics"
    control_schema: str = "devmirror_admin"
    workspace_profile: str | None = None

    # System-level policy defaults (SPECIFICATION.md section 8)
    max_dr_duration_days: int = 90
    default_notification_days: int = 7
    shallow_clone_threshold_gb: int = 50
    max_parallel_clones: int = 10
    audit_retention_days: int = 365
    lineage_system_table: str = "system.access.table_lineage"

    @property
    def control_fqn_prefix(self) -> str:
        """Fully qualified prefix for control tables: ``catalog.schema``."""
        return f"{self.control_catalog}.{self.control_schema}"


def _str_env(key: str, default: str) -> str:
    """Read a string env var, falling back to *default* if unset or blank."""
    return os.environ.get(key, "").strip() or default


def _int_env(key: str, default: int) -> int:
    """Read an integer env var, falling back to *default* if unset or blank."""
    raw = os.environ.get(key, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise SettingsError(f"{key} must be an integer, got: {raw!r}") from exc


def load_settings() -> Settings:
    """Load settings from environment variables."""
    warehouse_id = os.environ.get("DEVMIRROR_WAREHOUSE_ID", "").strip() or None

    return Settings(
        warehouse_id=warehouse_id,
        control_catalog=_str_env("DEVMIRROR_CONTROL_CATALOG", "dev_analytics"),
        control_schema=_str_env("DEVMIRROR_CONTROL_SCHEMA", "devmirror_admin"),
        workspace_profile=os.environ.get("DATABRICKS_CONFIG_PROFILE", "").strip() or None,
        max_dr_duration_days=_int_env("DEVMIRROR_MAX_DR_DURATION_DAYS", 90),
        default_notification_days=_int_env("DEVMIRROR_DEFAULT_NOTIFICATION_DAYS", 7),
        shallow_clone_threshold_gb=_int_env("DEVMIRROR_SHALLOW_CLONE_THRESHOLD_GB", 50),
        max_parallel_clones=_int_env("DEVMIRROR_MAX_PARALLEL_CLONES", 10),
        audit_retention_days=_int_env("DEVMIRROR_AUDIT_RETENTION_DAYS", 365),
        lineage_system_table=_str_env(
            "DEVMIRROR_LINEAGE_SYSTEM_TABLE", "system.access.table_lineage"
        ),
    )
