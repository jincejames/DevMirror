"""DevMirror runtime settings loaded from DEVMIRROR_* environment variables."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass


class SettingsError(Exception):
    """Raised when a required setting is missing or invalid."""


# Stage 4 US-35: DR ID prefix must be alphanumeric, start with a letter,
# and be at most 8 characters long.
_DR_ID_PREFIX_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9]{0,7}$")

# Stage 4 US-35: DR ID padding width must fall within [3, 12].
_DR_ID_PADDING_MIN = 3
_DR_ID_PADDING_MAX = 12


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

    # DR ID generation (Stage 4 US-34 / US-35)
    dr_id_prefix: str = "DR"
    dr_id_padding: int = 5

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
    """Load settings from environment variables.

    Reads ``DEVMIRROR_*`` environment variables once and returns an immutable
    :class:`Settings` instance. Values are only consumed at process startup
    (CLI invocation or app ``lifespan``); changes require a redeploy.

    Stage 4 US-35 validation:

    * ``DEVMIRROR_DR_ID_PREFIX`` (default ``"DR"``) must match
      ``^[A-Za-z][A-Za-z0-9]{0,7}$`` -- alphanumeric, starting with a letter,
      at most 8 characters.
    * ``DEVMIRROR_DR_ID_PADDING`` (default ``5``) must satisfy
      ``3 <= padding <= 12``.

    Raises:
        SettingsError: If a value is malformed (e.g. a non-integer padding,
            a prefix that violates the pattern, or a padding outside the
            accepted range). Raised before the :class:`Settings` object is
            constructed so the error surfaces at startup with a clear stack
            trace.
    """
    warehouse_id = os.environ.get("DEVMIRROR_WAREHOUSE_ID", "").strip() or None

    # Distinguish "unset" (apply default) from "explicitly blank" (reject).
    # Operators who set the var to "" or "   " likely intend to override the
    # default and should get a loud error instead of silently falling back.
    raw_prefix = os.environ.get("DEVMIRROR_DR_ID_PREFIX")
    if raw_prefix is None:
        dr_id_prefix = "DR"
    else:
        dr_id_prefix = raw_prefix.strip()
        if not _DR_ID_PREFIX_PATTERN.match(dr_id_prefix):
            raise SettingsError(
                "DEVMIRROR_DR_ID_PREFIX must be alphanumeric starting with a "
                f"letter, max 8 characters (got: {dr_id_prefix!r})"
            )

    dr_id_padding = _int_env("DEVMIRROR_DR_ID_PADDING", 5)
    if not (_DR_ID_PADDING_MIN <= dr_id_padding <= _DR_ID_PADDING_MAX):
        raise SettingsError(
            "DEVMIRROR_DR_ID_PADDING must be between 3 and 12 inclusive "
            f"(got: {dr_id_padding})"
        )

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
        dr_id_prefix=dr_id_prefix,
        dr_id_padding=dr_id_padding,
    )
