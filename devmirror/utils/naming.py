"""Deterministic naming conventions for DevMirror schema isolation."""

from __future__ import annotations

import re
from typing import Literal

_DR_NUMBER_RE = re.compile(r"^DR-(\d+)$")


class NamingError(Exception):
    """Raised when a naming operation receives invalid input."""


def extract_dr_number(dr_id: str) -> str:
    """Extract the numeric portion from a DR id (e.g. 'DR-1042' -> '1042')."""
    m = _DR_NUMBER_RE.match(dr_id)
    if not m:
        raise NamingError(
            f"Invalid dr_id format: {dr_id!r}. Expected DR-<digits> (e.g. 'DR-1042')."
        )
    return m.group(1)


def dev_schema_prefix(dr_id: str) -> str:
    """Return the dev schema prefix for a DR."""
    return f"dr_{extract_dr_number(dr_id)}"


def qa_schema_prefix(dr_id: str) -> str:
    """Return the QA schema prefix for a DR."""
    return f"qa_{extract_dr_number(dr_id)}"


def schema_prefix(dr_id: str, env: Literal["dev", "qa"]) -> str:
    """Return the schema prefix for a given environment."""
    if env == "dev":
        return dev_schema_prefix(dr_id)
    if env == "qa":
        return qa_schema_prefix(dr_id)
    raise NamingError(f"Unknown environment: {env!r}. Must be 'dev' or 'qa'.")


def target_schema_fqn(
    target_catalog: str,
    prod_schema_fqn: str,
    dr_id: str,
    env: Literal["dev", "qa"],
) -> str:
    """Build a two-part target schema FQN from a production schema FQN."""
    parts = prod_schema_fqn.split(".")
    if len(parts) < 2:
        raise NamingError(
            f"prod_schema_fqn must have at least 2 dot-separated parts "
            f"(catalog.schema), got: {prod_schema_fqn!r}"
        )
    original_schema = parts[1]
    prefix = schema_prefix(dr_id, env)
    return f"{target_catalog}.{prefix}_{original_schema}"


def target_object_fqn(
    target_catalog: str,
    prod_object_fqn: str,
    dr_id: str,
    env: Literal["dev", "qa"],
) -> str:
    """Build a three-part target object FQN from a production object FQN."""
    parts = prod_object_fqn.split(".")
    if len(parts) != 3:
        raise NamingError(
            f"prod_object_fqn must be three-part (catalog.schema.object), "
            f"got: {prod_object_fqn!r}"
        )
    original_schema = parts[1]
    object_name = parts[2]
    prefix = schema_prefix(dr_id, env)
    return f"{target_catalog}.{prefix}_{original_schema}.{object_name}"


def required_target_schemas(
    target_catalog: str,
    prod_schema_fqns: list[str],
    dr_id: str,
    env: Literal["dev", "qa"],
) -> list[str]:
    """Derive deduplicated sorted list of target schemas from prod schemas."""
    seen: set[str] = set()
    result: list[str] = []
    for fqn in prod_schema_fqns:
        target = target_schema_fqn(target_catalog, fqn, dr_id, env)
        if target not in seen:
            seen.add(target)
            result.append(target)
    result.sort()
    return result


def resolve_target_catalog(source_catalog: str, env: str) -> str:
    """Derive the target catalog name from a source catalog.

    If DEVMIRROR_TARGET_CATALOG is set, always use that catalog
    regardless of source catalog name.
    """
    import os
    override = os.environ.get("DEVMIRROR_TARGET_CATALOG", "").strip()
    if override:
        return override
    if source_catalog.startswith("prod_"):
        return source_catalog.replace("prod_", "dev_", 1)
    if source_catalog.startswith("prod"):
        return source_catalog.replace("prod", "dev", 1)
    return f"{source_catalog}_{env}"
