"""Shared helpers used by both router.py (Stage 1) and router_stage2.py (Stage 2)."""

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import yaml
from fastapi import HTTPException
from pydantic import ValidationError

from devmirror.utils.validation import validate_config_for_submission

from .models import (
    ConfigIn,
    ConfigListItem,
    ConfigOut,
    FieldError,
)
from .repository import ConfigRepository

if TYPE_CHECKING:
    from devmirror.settings import Settings
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)


_table_ensured = False


def _get_repo(settings: Settings, db_client: DbClient | None = None) -> ConfigRepository:
    """Return a ConfigRepository, bootstrapping ALL control tables on
    first call.

    Previously this only ensured `devmirror_configs`; the rest of the
    control plane (devmirror_development_requests, devmirror_dr_objects,
    devmirror_dr_access, audit_log, devmirror_id_counter) was assumed
    to be created out-of-band via `apply_control_ddl()`.  In customer
    onboardings where that step is skipped (e.g. LH), the first
    provision call blows up with TABLE_OR_VIEW_NOT_FOUND on whichever
    table happens to be queried first.  Running the full DDL idempotently
    (CREATE TABLE IF NOT EXISTS) on first request is cheap and removes
    the foot-gun.
    """
    global _table_ensured  # noqa: PLW0603
    repo = ConfigRepository(settings.control_fqn_prefix)
    if not _table_ensured and db_client is not None:
        try:
            from devmirror.control.control_table import apply_control_ddl

            apply_control_ddl(db_client, settings)
            logger.info(
                "Control-plane DDL applied at %s", settings.control_fqn_prefix,
            )
        except Exception:
            logger.warning(
                "Could not bootstrap control-plane DDL at %s",
                settings.control_fqn_prefix,
                exc_info=True,
            )
        _table_ensured = True
    return repo


@contextmanager
def _target_catalog_override(target_catalog: str | None, dr_id: str | None = None):
    """Temporarily set DEVMIRROR_TARGET_CATALOG for a per-DR explicit
    override.  When set, ``resolve_target_catalog()`` returns this value
    for every object regardless of source catalog.  Use sparingly -- the
    default LH behaviour (base + suffix per object) is preferred.

    Emits an INFO log line when the override fires so operators can grep
    app logs for "target_catalog override" and correlate unexpected
    target FQNs with the request that set the override (the user-facing
    audit log only records target FQNs after the fact).

    NOTE: This is not thread-safe.  If two requests run concurrently with
    different target catalogs the env-var override will conflict.  This is
    acceptable for now because the app is deployed behind a single-worker
    Uvicorn and provisioning is serialised via the TaskTracker.
    """
    if not target_catalog:
        yield
        return
    logger.info(
        "target_catalog override active for DR %s: every object will be "
        "cloned into %s (per-object base+suffix routing bypassed)",
        dr_id or "<unknown>", target_catalog,
    )
    old = os.environ.get("DEVMIRROR_TARGET_CATALOG")
    os.environ["DEVMIRROR_TARGET_CATALOG"] = target_catalog
    try:
        yield
    finally:
        if old is None:
            os.environ.pop("DEVMIRROR_TARGET_CATALOG", None)
        else:
            os.environ["DEVMIRROR_TARGET_CATALOG"] = old


def _build_yaml(config_in: ConfigIn) -> str:
    """Generate YAML from ConfigIn."""
    return yaml.safe_dump(
        {"version": "1.0", "development_request": config_in.model_dump(exclude_none=True)},
        default_flow_style=False,
        sort_keys=False,
    )


def _parse_config_in(config_json: str) -> ConfigIn:
    """Parse a stored config_json string back into a ConfigIn."""
    return ConfigIn.model_validate_json(config_json)


def _field_errors_from_validation_error(exc: ValidationError) -> list[FieldError]:
    """Convert a Pydantic ValidationError to a list of FieldError."""
    return [
        FieldError(loc=[str(p) for p in e["loc"]], msg=e["msg"])
        for e in exc.errors()
    ]


def _field_errors_from_strings(errors: list[str]) -> list[FieldError]:
    """Convert policy error strings to FieldError objects."""
    return [FieldError(loc=["policy"], msg=msg) for msg in errors]


def _row_to_config_out(row: dict) -> ConfigOut:
    """Convert a DB row dict to a ConfigOut response model."""
    config_in = _parse_config_in(row["config_json"])
    raw_errors = row.get("validation_errors", "[]")
    try:
        error_dicts = json.loads(raw_errors) if raw_errors else []
    except (json.JSONDecodeError, TypeError):
        error_dicts = []
    errors = [FieldError(**e) for e in error_dicts]
    return ConfigOut(
        dr_id=row["dr_id"],
        description=row.get("description"),
        status=row["status"],
        config=config_in,
        validation_errors=errors,
        created_at=row["created_at"],
        created_by=row["created_by"],
        updated_at=row.get("updated_at"),
        expiration_date=row["expiration_date"],
    )


def _row_to_list_item(row: dict) -> ConfigListItem:
    """Convert a DB row dict to a ConfigListItem."""
    return ConfigListItem(
        dr_id=row["dr_id"],
        description=row.get("description"),
        status=row["status"],
        created_at=row["created_at"],
        created_by=row["created_by"],
        expiration_date=row["expiration_date"],
    )


def _validate_config(config_in: ConfigIn) -> tuple[str, list[FieldError], object | None]:
    """Parse ConfigIn -> validate -> collect errors -> determine status.

    Returns ``(status, errors, dm_config)``.  ``dm_config`` is ``None``
    when schema validation fails.
    """
    from devmirror.config.schema import DevMirrorConfig  # noqa: F811

    all_errors: list[FieldError] = []
    dm_config: DevMirrorConfig | None = None

    try:
        dm_config = config_in.to_devmirror_config()
    except ValidationError as exc:
        all_errors.extend(_field_errors_from_validation_error(exc))

    if dm_config is not None:
        policy_errors = validate_config_for_submission(dm_config)
        all_errors.extend(_field_errors_from_strings(policy_errors))

    status = "invalid" if all_errors else "valid"
    return status, all_errors, dm_config


def _control_repos(settings: Settings):
    """Build the four control-table repositories from *settings*."""
    from devmirror.control.audit import AuditRepository
    from devmirror.control.control_table import (
        DrAccessRepository,
        DRRepository,
        DrObjectRepository,
    )

    fqn = settings.control_fqn_prefix
    return DRRepository(fqn), DrObjectRepository(fqn), DrAccessRepository(fqn), AuditRepository(fqn)


def _run_scan(db_client: DbClient, settings: Settings, dm_config, target_catalog: str | None = None) -> dict:
    """Run the full scan pipeline and return the manifest dict."""
    from devmirror.scan.dependency_classifier import classify_dependencies
    from devmirror.scan.lineage import LineageResult, query_lineage, query_table_sizes
    from devmirror.scan.manifest import build_manifest
    from devmirror.scan.stream_resolver import resolve_streams

    dr = dm_config.development_request

    ws_client = db_client.client
    stream_names = [s.name for s in dr.streams]

    # Short-circuit when the DR has no streams -- the additional_objects
    # path doesn't need workflow resolution or lineage walking, and we
    # specifically don't want to query `system.access.table_lineage`
    # (which may not even be reachable in some workspaces).
    if stream_names:
        resolved, unresolved = resolve_streams(ws_client, stream_names)
        if unresolved:
            raise HTTPException(
                status_code=400,
                detail=f"Unresolved streams: {unresolved}",
            )
        lineage_result = query_lineage(
            db_client, resolved, lineage_table=settings.lineage_system_table,
        )
    else:
        resolved = []
        lineage_result = LineageResult(edges=[], row_limit_hit=False)

    classification = classify_dependencies(
        lineage_result.edges,
        additional_objects=dr.additional_objects,
    )

    table_fqns = [
        obj.fqn for obj in classification.objects if obj.object_type == "table"
    ]
    table_sizes = query_table_sizes(db_client, table_fqns) if table_fqns else {}

    # Determine baseline catalog(s) -- the catalogs the streams' resolved
    # objects live in. If additional_objects reference a different catalog,
    # flag them as non-prod for admin review.
    additional_set = set(dr.additional_objects or [])
    baseline_catalogs: set[str] = set()
    for obj in classification.objects:
        if obj.fqn in additional_set:
            # Skip objects that came from additional_objects; we don't want
            # to treat their own catalog as a baseline.
            continue
        parts = obj.fqn.split(".")
        if len(parts) == 3:
            baseline_catalogs.add(parts[0])

    non_prod_additional: list[str] = []
    if baseline_catalogs:
        for fqn in additional_set:
            parts = fqn.split(".")
            if len(parts) == 3 and parts[0] not in baseline_catalogs:
                non_prod_additional.append(fqn)

    manifest = build_manifest(
        dr_id=dr.dr_id,
        streams=resolved,
        classification=classification,
        lineage_row_limit_hit=lineage_result.row_limit_hit,
        table_sizes=table_sizes or None,
        non_prod_additional_objects=non_prod_additional,
    )
    return manifest


def _auto_scan(
    db_client: DbClient, settings: Settings, config_in: ConfigIn, dm_config: object, repo: ConfigRepository, dr_id: str,
) -> None:
    """Auto-scan after a valid config is saved. Silently skips on failure."""
    try:
        with _target_catalog_override(config_in.target_catalog, dr_id=dr_id):
            manifest = _run_scan(db_client, settings, dm_config)
        manifest_json = json.dumps(manifest)
        scanned_at = datetime.now(UTC).isoformat()
        repo.update_manifest(db_client, dr_id=dr_id, manifest_json=manifest_json, scanned_at=scanned_at)
        repo.update_status(db_client, dr_id=dr_id, status="scanned")
        logger.info("Auto-scan completed for %s: %d objects", dr_id, manifest.get("scan_result", {}).get("total_objects", 0))
    except Exception:
        logger.warning("Auto-scan failed for %s (config saved as valid)", dr_id, exc_info=True)
