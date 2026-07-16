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

# Delta table feature that marks a clone target catalog-managed. Table-level
# feature only -- NOT valid on views, volumes, or UC schemas.
_CATALOG_MANAGED_PROP = "'delta.feature.catalogManaged' = 'supported'"

# Inline clause appended to the SHALLOW/DEEP CLONE builders. Verified against
# the LH pre-prod workspace (both interactively and via the Statement Execution
# API the app uses): `CREATE [OR REPLACE] TABLE ... CLONE ... TBLPROPERTIES(...)`
# honors the clause and the resulting table is catalog-managed. NOTE:
# `CREATE TABLE ... LIKE` does NOT -- it silently drops the clause -- so the
# schema_only path applies the feature via a follow-up ALTER (see
# set_catalog_managed_sql) instead of this inline form.
CATALOG_MANAGED_TBLPROPERTIES = f" TBLPROPERTIES ({_CATALOG_MANAGED_PROP})"

VALID_STRATEGIES = frozenset(
    {"shallow_clone", "deep_clone", "view", "schema_only", "create_volume"}
)

# Fixed directory tree carved out inside every provisioned import volume.
# Order matters: parents before children so ``files.create_directory`` calls
# don't race ahead of a missing intermediate path.
IMPORT_VOLUME_SUBDIRS = (
    "source",
    "source/data",
    "source/archive",
    "source/ready",
)


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
    """Generate SQL for a shallow clone.

    ``CREATE OR REPLACE`` so re-provisioning the same target_fqn
    refreshes in place instead of failing with "table exists".  Combined
    with the runner's force_replace path (which drops v1 targets that v2
    no longer plans to recreate), this guarantees provisioning is a
    true replacement, not an additive layer.
    """
    _validate_fqn(source_fqn, "source_fqn")
    _validate_fqn(target_fqn, "target_fqn")
    rev = _revision_clause(data_revision)
    return (
        f"CREATE OR REPLACE TABLE {target_fqn} SHALLOW CLONE {source_fqn}{rev}"
        f"{CATALOG_MANAGED_TBLPROPERTIES}"
    )


def create_deep_clone_sql(
    source_fqn: str,
    target_fqn: str,
    data_revision: DataRevision | None = None,
) -> str:
    """Generate SQL for a deep clone (CREATE OR REPLACE -- see shallow)."""
    _validate_fqn(source_fqn, "source_fqn")
    _validate_fqn(target_fqn, "target_fqn")
    rev = _revision_clause(data_revision)
    return (
        f"CREATE OR REPLACE TABLE {target_fqn} DEEP CLONE {source_fqn}{rev}"
        f"{CATALOG_MANAGED_TBLPROPERTIES}"
    )


def create_view_sql(
    source_fqn: str,
    target_fqn: str,
    data_revision: DataRevision | None = None,
) -> str:
    """Generate SQL for a view referencing the prod table (CREATE OR REPLACE)."""
    _validate_fqn(source_fqn, "source_fqn")
    _validate_fqn(target_fqn, "target_fqn")
    rev = _revision_clause(data_revision)
    return f"CREATE OR REPLACE VIEW {target_fqn} AS SELECT * FROM {source_fqn}{rev}"


def create_schema_only_sql(
    source_fqn: str,
    target_fqn: str,
) -> str:
    """Generate SQL for a schema-only (empty) table.

    ``CREATE TABLE ... LIKE`` silently ignores an inline ``TBLPROPERTIES``
    clause (verified on LH pre-prod), so catalog-managed is applied via a
    separate ``ALTER`` -- see ``set_catalog_managed_sql``.
    """
    _validate_fqn(source_fqn, "source_fqn")
    _validate_fqn(target_fqn, "target_fqn")
    return f"CREATE TABLE {target_fqn} LIKE {source_fqn}"


def set_catalog_managed_sql(target_fqn: str) -> str:
    """Generate an ALTER that marks an existing table catalog-managed.

    Used for the ``schema_only`` (``CREATE TABLE ... LIKE``) path, where an
    inline ``TBLPROPERTIES`` clause is silently dropped. A LIKE-created table
    is a fresh managed table (not a clone), so ``ALTER TABLE ... SET
    TBLPROPERTIES`` reliably sets the feature and is NOT affected by the
    shallow-clone ALTER limitation (SUP-32492); verified on LH pre-prod.
    """
    _validate_fqn(target_fqn, "target_fqn")
    return f"ALTER TABLE {target_fqn} SET TBLPROPERTIES ({_CATALOG_MANAGED_PROP})"


def create_volume_sql(target_fqn: str) -> str:
    """Generate SQL to create a managed Volume.

    ``target_fqn`` is the three-part volume FQN
    (``<catalog>.<schema>.<volume_name>``).  No source FQN involved --
    a volume is a side-channel for sideloaded files, not a clone of
    anything.  ``IF NOT EXISTS`` makes re-provisioning idempotent.
    """
    _validate_fqn(target_fqn, "target_fqn")
    return f"CREATE VOLUME IF NOT EXISTS {target_fqn}"


def _volume_path(target_fqn: str) -> str:
    """Convert a three-part volume FQN to its Files API root path."""
    catalog, schema, volume = target_fqn.split(".")
    return f"/Volumes/{catalog}/{schema}/{volume}"


def provision_volume_subdirs(db_client: DbClient, target_fqn: str) -> list[str]:
    """Create the fixed ``source/{data,archive,ready}`` tree inside a volume.

    Best-effort: each directory create is independent and idempotent at the
    SDK level (``create_directory`` is a no-op when the path already exists).
    Failures are logged but do NOT raise -- the volume itself is already
    provisioned, and a missing subdir is a recoverable annoyance, not a
    provisioning failure.

    Returns the list of directory paths that were created (or already existed).
    """
    root = _volume_path(target_fqn)
    created: list[str] = []
    for sub in IMPORT_VOLUME_SUBDIRS:
        path = f"{root}/{sub}"
        try:
            db_client.client.files.create_directory(directory_path=path)
            created.append(path)
        except Exception as exc:
            logger.warning("Failed to create volume directory %s: %s", path, exc)
    return created


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
    if strategy == "create_volume":
        # source_fqn is ignored -- volumes are not clones of anything.
        return create_volume_sql(target_fqn)
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
        if strategy == "create_volume":
            # Carve out the fixed sideload layout (source/{data,archive,ready}).
            # Best-effort: directory creation failures are warnings, not errors.
            provision_volume_subdirs(db_client, target_fqn)
        elif strategy == "schema_only":
            # `CREATE TABLE ... LIKE` drops an inline TBLPROPERTIES clause, so
            # mark the fresh (non-clone) table catalog-managed via a follow-up
            # ALTER.  Best-effort: a failure here does not fail the clone.
            try:
                db_client.sql_exec(set_catalog_managed_sql(target_fqn))
            except Exception as exc:
                logger.warning(
                    "Failed to set catalogManaged on schema_only table %s: %s",
                    target_fqn, exc,
                )
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
