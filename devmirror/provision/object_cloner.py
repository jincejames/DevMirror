"""SQL generation for object cloning and schema provisioning."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from devmirror.config.schema import DataRevision
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)

# Safe identifier pattern for three-part FQNs.
_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_]+$")

VALID_STRATEGIES = frozenset({"shallow_clone", "deep_clone", "view", "schema_only"})


class ClonerError(Exception):
    """Raised when clone SQL generation or execution fails."""


def _validate_fqn(fqn: str, label: str) -> None:
    """Validate a three-part FQN for safe SQL interpolation."""
    parts = fqn.split(".")
    if len(parts) != 3:
        raise ClonerError(
            f"{label} must be three-part (catalog.schema.object), got: {fqn!r}"
        )
    for part in parts:
        if not _SAFE_IDENTIFIER.match(part):
            raise ClonerError(
                f"Unsafe identifier in {label}: {part!r}. "
                "Only alphanumeric characters and underscores are allowed."
            )


def _revision_clause(data_revision: DataRevision | None) -> str:
    """Build the VERSION/TIMESTAMP AS OF clause, or empty string for latest."""
    if data_revision is None or data_revision.mode == "latest":
        return ""
    if data_revision.mode == "version" and data_revision.version is not None:
        return f" VERSION AS OF {data_revision.version}"
    if data_revision.mode == "timestamp" and data_revision.timestamp is not None:
        ts = data_revision.timestamp.replace("'", "''")
        return f" TIMESTAMP AS OF '{ts}'"
    return ""


def create_shallow_clone_sql(
    source_fqn: str,
    target_fqn: str,
    data_revision: DataRevision | None = None,
) -> str:
    """Generate SQL for a shallow clone."""
    _validate_fqn(source_fqn, "source_fqn")
    _validate_fqn(target_fqn, "target_fqn")
    rev = _revision_clause(data_revision)
    return f"CREATE TABLE {target_fqn} SHALLOW CLONE {source_fqn}{rev}"


def create_deep_clone_sql(
    source_fqn: str,
    target_fqn: str,
    data_revision: DataRevision | None = None,
) -> str:
    """Generate SQL for a deep clone."""
    _validate_fqn(source_fqn, "source_fqn")
    _validate_fqn(target_fqn, "target_fqn")
    rev = _revision_clause(data_revision)
    return f"CREATE TABLE {target_fqn} DEEP CLONE {source_fqn}{rev}"


def create_view_sql(
    source_fqn: str,
    target_fqn: str,
    data_revision: DataRevision | None = None,
) -> str:
    """Generate SQL for a view referencing the prod table."""
    _validate_fqn(source_fqn, "source_fqn")
    _validate_fqn(target_fqn, "target_fqn")
    rev = _revision_clause(data_revision)
    return f"CREATE VIEW {target_fqn} AS SELECT * FROM {source_fqn}{rev}"


def create_schema_only_sql(
    source_fqn: str,
    target_fqn: str,
) -> str:
    """Generate SQL for a schema-only (empty) table."""
    _validate_fqn(source_fqn, "source_fqn")
    _validate_fqn(target_fqn, "target_fqn")
    return f"CREATE TABLE {target_fqn} LIKE {source_fqn}"


def generate_clone_sql(
    source_fqn: str,
    target_fqn: str,
    strategy: str,
    data_revision: DataRevision | None = None,
) -> str:
    """Generate clone SQL for the given strategy."""
    if strategy not in VALID_STRATEGIES:
        raise ClonerError(
            f"Unknown clone strategy: {strategy!r}. "
            f"Valid strategies: {sorted(VALID_STRATEGIES)}"
        )

    if strategy == "shallow_clone":
        return create_shallow_clone_sql(source_fqn, target_fqn, data_revision)
    if strategy == "deep_clone":
        return create_deep_clone_sql(source_fqn, target_fqn, data_revision)
    if strategy == "view":
        return create_view_sql(source_fqn, target_fqn, data_revision)
    # schema_only
    return create_schema_only_sql(source_fqn, target_fqn)


@dataclass
class CloneResult:
    """Outcome of a single object clone operation."""

    source_fqn: str
    target_fqn: str
    strategy: str
    sql: str
    success: bool
    error: str | None = None


def execute_clone(
    db_client: DbClient,
    source_fqn: str,
    target_fqn: str,
    strategy: str,
    data_revision: DataRevision | None = None,
) -> CloneResult:
    """Generate and execute clone SQL for a single object."""
    try:
        sql = generate_clone_sql(source_fqn, target_fqn, strategy, data_revision)
    except ClonerError as exc:
        return CloneResult(
            source_fqn=source_fqn,
            target_fqn=target_fqn,
            strategy=strategy,
            sql="",
            success=False,
            error=str(exc),
        )

    try:
        logger.info("Cloning %s -> %s [%s]", source_fqn, target_fqn, strategy)
        db_client.sql_exec(sql)
        return CloneResult(
            source_fqn=source_fqn,
            target_fqn=target_fqn,
            strategy=strategy,
            sql=sql,
            success=True,
        )
    except Exception as exc:
        logger.error("Clone failed %s -> %s: %s", source_fqn, target_fqn, exc)
        return CloneResult(
            source_fqn=source_fqn,
            target_fqn=target_fqn,
            strategy=strategy,
            sql=sql,
            success=False,
            error=str(exc),
        )


def default_clone_strategy(
    object_type: str,
    access_mode: str,
) -> str:
    """Determine the default clone strategy for an object."""
    if object_type == "view":
        return "view"
    return "shallow_clone"


# ---------------------------------------------------------------------------
# Schema provisioning (merged from schema_provisioner.py)
# ---------------------------------------------------------------------------


class SchemaProvisioningError(Exception):
    """Raised when schema provisioning fails."""


def _validate_schema_identifier(part: str, label: str) -> None:
    """Validate that an identifier part is safe for SQL interpolation."""
    if not _SAFE_IDENTIFIER.match(part):
        raise SchemaProvisioningError(
            f"Unsafe {label} identifier: {part!r}. "
            "Only alphanumeric characters and underscores are allowed."
        )


def create_schema_sql(schema_fqn: str) -> str:
    """Generate a ``CREATE SCHEMA IF NOT EXISTS`` statement."""
    parts = schema_fqn.split(".")
    if len(parts) != 2:
        raise SchemaProvisioningError(
            f"Schema FQN must be two-part (catalog.schema), got: {schema_fqn!r}"
        )
    for part in parts:
        _validate_schema_identifier(part, "schema FQN part")
    return f"CREATE SCHEMA IF NOT EXISTS {schema_fqn}"


@dataclass
class SchemaProvisionResult:
    """Result of provisioning schemas."""

    created: list[str]
    failed: dict[str, str]

    @property
    def all_succeeded(self) -> bool:
        return len(self.failed) == 0


def provision_schemas(
    db_client: DbClient,
    schema_fqns: list[str],
) -> SchemaProvisionResult:
    """Create all required schemas via the SDK."""
    created: list[str] = []
    failed: dict[str, str] = {}

    for fqn in schema_fqns:
        parts = fqn.split(".")
        if len(parts) != 2:
            failed[fqn] = f"Schema FQN must be two-part (catalog.schema), got: {fqn!r}"
            continue
        catalog, schema = parts
        try:
            logger.info("Creating schema: %s", fqn)
            db_client.create_schema(catalog, schema)
            created.append(fqn)
        except Exception as exc:
            logger.error("Failed to create schema %s: %s", fqn, exc)
            failed[fqn] = str(exc)

    return SchemaProvisionResult(created=created, failed=failed)
