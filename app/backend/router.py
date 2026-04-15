"""API router: DevMirror config + Stage 2 scan/provision/status/cleanup endpoints."""
# ruff: noqa: B008  -- Depends() in function signatures is standard FastAPI pattern

from __future__ import annotations

import json
import logging
import os
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import yaml
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response
from pydantic import ValidationError

from devmirror.utils.validation import validate_config_for_submission

from .config import get_current_user, get_db_client, get_settings, get_task_tracker
from .models import (
    CleanupResponse,
    ConfigIn,
    ConfigListItem,
    ConfigListResponse,
    ConfigOut,
    DrListItem,
    DrListResponse,
    DrStatusResponse,
    FieldError,
    ManifestResponse,
    ProvisionStartResponse,
    RefreshRequest,
    RefreshStartResponse,
    ScanResponse,
    StreamSearchResponse,
    StreamSearchResult,
    TaskStatusResponse,
    ValidationResult,
)
from .repository import ConfigRepository
from .tasks import TaskTracker

if TYPE_CHECKING:
    from devmirror.settings import Settings
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)

router = APIRouter()


def _get_repo(settings: Settings) -> ConfigRepository:
    return ConfigRepository(settings.control_fqn_prefix)


@contextmanager
def _target_catalog_override(target_catalog: str | None):
    """Temporarily set DEVMIRROR_TARGET_CATALOG for a per-DR override."""
    if not target_catalog:
        yield
        return
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


# ---- Shared helpers ----


def _validate_config(config_in: ConfigIn) -> tuple[str, list[FieldError], object | None]:
    """Parse ConfigIn -> validate -> collect errors -> determine status.

    Returns ``(status, errors, dm_config)``.  ``dm_config`` is ``None``
    when schema validation fails.
    """
    from devmirror.config.schema import DevMirrorConfig  # noqa: F811 – re-import is fine

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
    from devmirror.scan.lineage import query_lineage, query_table_sizes
    from devmirror.scan.manifest import build_manifest
    from devmirror.scan.stream_resolver import resolve_streams

    dr = dm_config.development_request

    ws_client = db_client.client
    stream_names = [s.name for s in dr.streams]
    resolved, unresolved = resolve_streams(ws_client, stream_names)
    if unresolved:
        raise HTTPException(
            status_code=400,
            detail=f"Unresolved streams: {unresolved}",
        )

    lineage_result = query_lineage(
        db_client, resolved, lineage_table=settings.lineage_system_table
    )

    classification = classify_dependencies(
        lineage_result.edges,
        additional_objects=dr.additional_objects,
    )

    table_fqns = [
        obj.fqn for obj in classification.objects if obj.object_type == "table"
    ]
    table_sizes = query_table_sizes(db_client, table_fqns) if table_fqns else {}

    manifest = build_manifest(
        dr_id=dr.dr_id,
        streams=resolved,
        classification=classification,
        lineage_row_limit_hit=lineage_result.row_limit_hit,
        table_sizes=table_sizes or None,
    )
    return manifest


def _auto_scan(
    db_client: DbClient, settings: Settings, config_in: ConfigIn, dm_config: object, repo: ConfigRepository, dr_id: str,
) -> None:
    """Auto-scan after a valid config is saved. Silently skips on failure."""
    try:
        with _target_catalog_override(config_in.target_catalog):
            manifest = _run_scan(db_client, settings, dm_config)
        manifest_json = json.dumps(manifest)
        scanned_at = datetime.now(UTC).isoformat()
        repo.update_manifest(db_client, dr_id=dr_id, manifest_json=manifest_json, scanned_at=scanned_at)
        repo.update_status(db_client, dr_id=dr_id, status="scanned")
        logger.info("Auto-scan completed for %s: %d objects", dr_id, manifest.get("scan_result", {}).get("total_objects", 0))
    except Exception:
        logger.warning("Auto-scan failed for %s (config saved as valid)", dr_id, exc_info=True)


# ---- 1. POST /api/configs (createConfig) ---------------------------------

@router.post(
    "/configs",
    response_model=ConfigOut,
    status_code=201,
    operation_id="createConfig",
)
def create_config(
    config_in: ConfigIn,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    current_user: str = Depends(get_current_user),
) -> ConfigOut:
    """Create a new DevMirror config."""
    status, all_errors, _dm_config = _validate_config(config_in)

    config_json = config_in.model_dump_json()
    config_yaml = _build_yaml(config_in)
    errors_json = json.dumps([e.model_dump() for e in all_errors])

    repo = _get_repo(settings)
    repo.insert(
        db_client,
        dr_id=config_in.dr_id,
        config_json=config_json,
        config_yaml=config_yaml,
        status=status,
        validation_errors=errors_json,
        created_by=current_user,
        expiration_date=config_in.expiration_date,
        description=config_in.description,
    )

    # Auto-scan if config is valid
    if status == "valid" and _dm_config is not None:
        _auto_scan(db_client, settings, config_in, _dm_config, repo, config_in.dr_id)

    row = repo.get(db_client, dr_id=config_in.dr_id)
    if row is None:
        raise HTTPException(status_code=500, detail="Failed to read back created config")
    return _row_to_config_out(row)


# ---- 2. GET /api/configs (listConfigs) ------------------------------------

@router.get(
    "/configs",
    response_model=ConfigListResponse,
    operation_id="listConfigs",
)
def list_configs(
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
) -> ConfigListResponse:
    """List all DevMirror configs."""
    repo = _get_repo(settings)
    rows = repo.list_all(db_client)
    items = [_row_to_list_item(r) for r in rows]
    return ConfigListResponse(configs=items, total=len(items))


# ---- 3. GET /api/configs/{dr_id} (getConfig) -----------------------------

@router.get(
    "/configs/{dr_id}",
    response_model=ConfigOut,
    operation_id="getConfig",
)
def get_config(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
) -> ConfigOut:
    """Get a single DevMirror config by DR ID."""
    repo = _get_repo(settings)
    row = repo.get(db_client, dr_id=dr_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")
    return _row_to_config_out(row)


# ---- 4. PUT /api/configs/{dr_id} (updateConfig) --------------------------

@router.put(
    "/configs/{dr_id}",
    response_model=ConfigOut,
    operation_id="updateConfig",
)
def update_config(
    dr_id: str,
    config_in: ConfigIn,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
) -> ConfigOut:
    """Update an existing DevMirror config."""
    repo = _get_repo(settings)
    existing = repo.get(db_client, dr_id=dr_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")

    was_provisioned = existing.get("status") == "provisioned"

    status, all_errors, _dm_config = _validate_config(config_in)

    # If the config was provisioned, keep the provisioned status after editing
    if was_provisioned and status == "valid":
        status = "provisioned"
    config_json = config_in.model_dump_json()
    config_yaml = _build_yaml(config_in)
    errors_json = json.dumps([e.model_dump() for e in all_errors])

    repo.update(
        db_client,
        dr_id=dr_id,
        config_json=config_json,
        config_yaml=config_yaml,
        status=status,
        validation_errors=errors_json,
        expiration_date=config_in.expiration_date,
        description=config_in.description,
    )

    # Auto-scan if config is valid (not provisioned -- provisioned configs use re-provision)
    if status == "valid" and _dm_config is not None:
        _auto_scan(db_client, settings, config_in, _dm_config, repo, dr_id)

    row = repo.get(db_client, dr_id=dr_id)
    if row is None:
        raise HTTPException(status_code=500, detail="Failed to read back updated config")
    return _row_to_config_out(row)


# ---- 5. DELETE /api/configs/{dr_id} (deleteConfig) -----------------------

@router.delete(
    "/configs/{dr_id}",
    status_code=204,
    operation_id="deleteConfig",
)
def delete_config(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
) -> Response:
    """Delete a DevMirror config."""
    repo = _get_repo(settings)
    existing = repo.get(db_client, dr_id=dr_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")
    if existing.get("status") == "provisioned":
        raise HTTPException(
            status_code=409,
            detail=f"Config {dr_id} is provisioned and cannot be deleted",
        )
    repo.delete(db_client, dr_id=dr_id)
    return Response(status_code=204)


# ---- 6. POST /api/configs/{dr_id}/validate (revalidateConfig) -----------

@router.post(
    "/configs/{dr_id}/validate",
    response_model=ValidationResult,
    operation_id="revalidateConfig",
)
def revalidate_config(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
) -> ValidationResult:
    """Re-validate an existing config and update its status/errors."""
    repo = _get_repo(settings)
    row = repo.get(db_client, dr_id=dr_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")

    config_in = _parse_config_in(row["config_json"])
    status, all_errors, _dm_config = _validate_config(config_in)
    errors_json = json.dumps([e.model_dump() for e in all_errors])
    config_yaml = _build_yaml(config_in)

    repo.update(
        db_client,
        dr_id=dr_id,
        config_json=row["config_json"],
        config_yaml=config_yaml,
        status=status,
        validation_errors=errors_json,
        expiration_date=row["expiration_date"],
        description=row.get("description"),
    )

    return ValidationResult(status=status, errors=all_errors)


# ---- 7. GET /api/configs/{dr_id}/yaml (exportConfigYaml) ----------------

@router.get(
    "/configs/{dr_id}/yaml",
    operation_id="exportConfigYaml",
)
def export_config_yaml(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
) -> Response:
    """Export config as a downloadable YAML file."""
    repo = _get_repo(settings)
    row = repo.get(db_client, dr_id=dr_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")

    yaml_content = row.get("config_yaml", "")
    return Response(
        content=yaml_content,
        media_type="text/yaml",
        headers={"Content-Disposition": f'attachment; filename="{dr_id}.yaml"'},
    )


# ---- 8. GET /api/streams/search (searchStreams) --------------------------

@router.get(
    "/streams/search",
    response_model=StreamSearchResponse,
    operation_id="searchStreams",
)
def search_streams(
    q: str = Query(..., min_length=1, description="Search term for stream names"),
    db_client: DbClient = Depends(get_db_client),
) -> StreamSearchResponse:
    """Search for Databricks jobs and pipelines by name."""
    results: list[StreamSearchResult] = []

    try:
        ws_client = db_client.client

        # Search jobs
        try:
            for job in ws_client.jobs.list(name=q):
                job_name = getattr(job.settings, "name", None) if job.settings else None
                if job_name:
                    results.append(StreamSearchResult(name=job_name, type="job"))
                if len(results) >= 20:
                    break
        except Exception:
            logger.warning("Failed to search jobs", exc_info=True)

        # Search pipelines
        if len(results) < 20:
            try:
                for pipeline in ws_client.pipelines.list_pipelines(
                    filter=f"name LIKE '%{q}%'"
                ):
                    pipeline_name = pipeline.name
                    if pipeline_name:
                        results.append(
                            StreamSearchResult(name=pipeline_name, type="pipeline")
                        )
                    if len(results) >= 20:
                        break
            except Exception:
                logger.warning("Failed to search pipelines", exc_info=True)

    except Exception:
        logger.warning("Failed to initialize workspace client for search", exc_info=True)

    return StreamSearchResponse(results=results[:20])


# ---- 9. POST /api/configs/{dr_id}/scan (scanConfig) -----------------------

@router.post(
    "/configs/{dr_id}/scan",
    response_model=ScanResponse,
    operation_id="scanConfig",
)
def scan_config(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
) -> ScanResponse:
    """Trigger an object discovery scan for a saved config."""
    repo = _get_repo(settings)
    row = repo.get(db_client, dr_id=dr_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")

    if row["status"] == "invalid":
        raise HTTPException(
            status_code=400,
            detail=f"Config {dr_id} has validation errors. Fix them before scanning.",
        )

    config_in = _parse_config_in(row["config_json"])
    dm_config = config_in.to_devmirror_config()

    with _target_catalog_override(config_in.target_catalog):
        manifest = _run_scan(db_client, settings, dm_config)

    # Store manifest in config row
    manifest_json = json.dumps(manifest)
    scanned_at = datetime.now(UTC).isoformat()
    repo.update_manifest(
        db_client,
        dr_id=dr_id,
        manifest_json=manifest_json,
        scanned_at=scanned_at,
    )

    # Update status to scanned
    repo.update_status(db_client, dr_id=dr_id, status="scanned")

    return ScanResponse(dr_id=dr_id, status="scanned", manifest=manifest)


# ---- 10. GET /api/configs/{dr_id}/manifest (getManifest) ------------------

@router.get(
    "/configs/{dr_id}/manifest",
    response_model=ManifestResponse,
    operation_id="getManifest",
)
def get_manifest(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
) -> ManifestResponse:
    """Retrieve the stored scan manifest for review."""
    repo = _get_repo(settings)
    result = repo.get_manifest(db_client, dr_id=dr_id)
    if result is None:
        raise HTTPException(
            status_code=404,
            detail=f"No manifest found for {dr_id}. Run a scan first.",
        )
    return ManifestResponse(
        dr_id=dr_id,
        manifest=result["manifest"],
        scanned_at=result.get("scanned_at"),
    )


# ---- 11. PUT /api/configs/{dr_id}/manifest (updateManifest) --------------

@router.put(
    "/configs/{dr_id}/manifest",
    response_model=ManifestResponse,
    operation_id="updateManifest",
)
def update_manifest(
    dr_id: str,
    manifest: dict,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
) -> ManifestResponse:
    """Save a modified manifest after human review."""
    repo = _get_repo(settings)
    row = repo.get(db_client, dr_id=dr_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")

    # Validate minimal manifest structure
    scan_result = manifest.get("scan_result")
    if not isinstance(scan_result, dict) or "objects" not in scan_result:
        raise HTTPException(
            status_code=400,
            detail="Invalid manifest: must contain scan_result.objects",
        )

    manifest_json = json.dumps(manifest)
    scanned_at = datetime.now(UTC).isoformat()
    repo.update_manifest(
        db_client,
        dr_id=dr_id,
        manifest_json=manifest_json,
        scanned_at=scanned_at,
    )

    return ManifestResponse(
        dr_id=dr_id,
        manifest=manifest,
        scanned_at=scanned_at,
    )


# ---- 12. POST /api/configs/{dr_id}/provision (provisionConfig) -----------

@router.post(
    "/configs/{dr_id}/provision",
    response_model=ProvisionStartResponse,
    status_code=202,
    operation_id="provisionConfig",
)
def provision_config(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    task_tracker: TaskTracker = Depends(get_task_tracker),
) -> ProvisionStartResponse:
    """Start provisioning of dev/qa objects from an approved manifest."""
    from devmirror.provision.runner import provision_dr

    repo = _get_repo(settings)
    row = repo.get(db_client, dr_id=dr_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")

    if row["status"] == "invalid":
        raise HTTPException(
            status_code=400,
            detail=f"Config {dr_id} is invalid. Fix validation errors before provisioning.",
        )

    manifest_raw = row.get("manifest_json")
    if not manifest_raw:
        raise HTTPException(
            status_code=400,
            detail=f"No manifest for {dr_id}. Run a scan first.",
        )

    config_in = _parse_config_in(row["config_json"])
    dm_config = config_in.to_devmirror_config()
    manifest = json.loads(manifest_raw)
    dr_repo, obj_repo, access_repo, audit_repo = _control_repos(settings)
    tc = config_in.target_catalog

    def do_provision() -> dict:
        with _target_catalog_override(tc):
            result = provision_dr(
                dm_config,
                manifest,
                db_client=db_client,
                dr_repo=dr_repo,
                obj_repo=obj_repo,
                access_repo=access_repo,
                audit_repo=audit_repo,
                max_parallel=settings.max_parallel_clones,
                force_replace=True,
            )
        repo.update_status(db_client, dr_id=dr_id, status="provisioned")
        return {
            "final_status": result.final_status,
            "objects_succeeded": len(result.objects_succeeded),
            "objects_failed": len(result.objects_failed),
            "schemas_created": len(result.schemas_created),
            "grants_applied": result.grants_applied,
        }

    task_id = task_tracker.submit(dr_id, "provision", do_provision)
    return ProvisionStartResponse(
        dr_id=dr_id,
        task_id=task_id,
        status="provisioning",
        message=f"Provisioning started. Poll GET /api/tasks/{task_id} for progress.",
    )


# ---- 13. GET /api/tasks/{task_id} (getTaskStatus) ------------------------

@router.get(
    "/tasks/{task_id}",
    response_model=TaskStatusResponse,
    operation_id="getTaskStatus",
)
def get_task_status(
    task_id: str,
    task_tracker: TaskTracker = Depends(get_task_tracker),
) -> TaskStatusResponse:
    """Poll for background task status."""
    task = task_tracker.get(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail=f"Task {task_id} not found")
    return TaskStatusResponse(
        task_id=task.task_id,
        dr_id=task.dr_id,
        task_type=task.task_type,
        status=task.status,
        progress=task.progress,
        result=task.result,
        error=task.error,
        started_at=task.started_at,
        completed_at=task.completed_at,
    )


# ---- 14. GET /api/drs/{dr_id}/status (getDrStatus) -----------------------

@router.get(
    "/drs/{dr_id}/status",
    response_model=DrStatusResponse,
    operation_id="getDrStatus",
)
def get_dr_status(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
) -> DrStatusResponse:
    """Get the full lifecycle status of a provisioned DR."""
    dr_repo, obj_repo, _access_repo, audit_repo = _control_repos(settings)

    dr_row = dr_repo.get(db_client, dr_id=dr_id)
    if dr_row is None:
        raise HTTPException(
            status_code=404, detail=f"DR {dr_id} not found in control tables"
        )

    objects = obj_repo.list_by_dr_id(db_client, dr_id=dr_id)
    audit_entries = audit_repo.list_by_dr_id(db_client, dr_id=dr_id, limit=20)

    # Build object breakdown by status
    breakdown: dict[str, int] = {}
    for obj in objects:
        status = obj.get("status", "UNKNOWN")
        breakdown[status] = breakdown.get(status, 0) + 1

    return DrStatusResponse(
        dr_id=dr_id,
        status=dr_row.get("status", "UNKNOWN"),
        description=dr_row.get("description"),
        expiration_date=dr_row.get("expiration_date", ""),
        created_at=dr_row.get("created_at", ""),
        last_refreshed_at=dr_row.get("last_refreshed_at"),
        objects=objects,
        total_objects=len(objects),
        object_breakdown=breakdown,
        recent_audit=audit_entries,
    )


# ---- 15. GET /api/drs (listDrs) ------------------------------------------

@router.get(
    "/drs",
    response_model=DrListResponse,
    operation_id="listDrs",
)
def list_drs(
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
) -> DrListResponse:
    """List all provisioned DRs from the control table."""
    dr_repo, _obj_repo, _access_repo, _audit_repo = _control_repos(settings)

    rows = dr_repo.list_active(db_client)
    items: list[DrListItem] = []
    for row in rows:
        items.append(
            DrListItem(
                dr_id=row.get("dr_id", ""),
                status=row.get("status", "UNKNOWN"),
                description=row.get("description"),
                expiration_date=row.get("expiration_date", ""),
                created_at=row.get("created_at", ""),
                created_by=row.get("created_by", ""),
                total_objects=0,
            )
        )

    return DrListResponse(drs=items, total=len(items))


# ---- 16. POST /api/drs/{dr_id}/cleanup (cleanupDr) -----------------------

@router.post(
    "/drs/{dr_id}/cleanup",
    response_model=CleanupResponse,
    operation_id="cleanupDr",
)
def cleanup_dr_endpoint(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    task_tracker: TaskTracker = Depends(get_task_tracker),
) -> CleanupResponse:
    """Manually trigger cleanup for a specific DR."""
    from devmirror.cleanup.cleanup_engine import cleanup_dr

    # Guard against concurrent cleanups for the same DR
    running = task_tracker.list_for_dr(dr_id)
    if any(t.task_type == "cleanup" and t.status == "running" for t in running):
        raise HTTPException(
            status_code=409,
            detail=f"A cleanup is already in progress for {dr_id}.",
        )

    dr_repo, obj_repo, access_repo, audit_repo = _control_repos(settings)
    result = cleanup_dr(
        dr_id,
        db_client=db_client,
        dr_repo=dr_repo,
        obj_repo=obj_repo,
        access_repo=access_repo,
        audit_repo=audit_repo,
    )

    if result.final_status == "NOT_FOUND":
        raise HTTPException(
            status_code=404,
            detail=f"DR {dr_id} not found in control tables",
        )

    return CleanupResponse(
        dr_id=dr_id,
        final_status=result.final_status,
        objects_dropped=result.objects_dropped,
        schemas_dropped=result.schemas_dropped,
        revokes_succeeded=result.revokes_succeeded,
    )


# ---- 17. POST /api/drs/{dr_id}/refresh (refreshDr) -------------------------

@router.post(
    "/drs/{dr_id}/refresh",
    response_model=RefreshStartResponse,
    status_code=202,
    operation_id="refreshDr",
)
def refresh_dr_endpoint(
    dr_id: str,
    body: RefreshRequest | None = None,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    task_tracker: TaskTracker = Depends(get_task_tracker),
) -> RefreshStartResponse:
    """Start a refresh (re-sync) of dev objects from production."""
    from devmirror.refresh.refresh_engine import refresh_dr

    dr_repo, obj_repo, _access_repo, audit_repo = _control_repos(settings)

    # Validate DR exists and is ACTIVE/EXPIRING_SOON
    dr_row = dr_repo.get(db_client, dr_id=dr_id)
    if dr_row is None:
        raise HTTPException(
            status_code=404,
            detail=f"DR {dr_id} not found in control tables",
        )
    dr_status = dr_row.get("status", "")
    if dr_status not in ("ACTIVE", "EXPIRING_SOON", "FAILED"):
        raise HTTPException(
            status_code=409,
            detail=f"DR {dr_id} has status {dr_status}. Refresh is only allowed on ACTIVE, EXPIRING_SOON, or FAILED DRs.",
        )

    req = body or RefreshRequest()
    mode = req.mode
    selected_fqns = req.selected_objects

    def do_refresh() -> dict:
        result = refresh_dr(
            dr_id,
            mode,  # type: ignore[arg-type]
            db_client=db_client,
            dr_repo=dr_repo,
            obj_repo=obj_repo,
            audit_repo=audit_repo,
            selected_fqns=selected_fqns,
            max_parallel=settings.max_parallel_clones,
        )
        return {
            "audit_status": result.audit_status,
            "mode": result.mode,
            "objects_succeeded": len(result.objects_succeeded),
            "objects_failed": len(result.objects_failed),
        }

    task_id = task_tracker.submit(dr_id, "refresh", do_refresh)
    return RefreshStartResponse(
        dr_id=dr_id,
        task_id=task_id,
        status="refreshing",
        message=f"Refresh started (mode={mode}). Poll GET /api/tasks/{task_id} for progress.",
    )


# ---- 18. POST /api/drs/{dr_id}/reprovision (reprovisionDr) -----------------

@router.post(
    "/drs/{dr_id}/reprovision",
    response_model=ProvisionStartResponse,
    status_code=202,
    operation_id="reprovisionDr",
)
def reprovision_dr_endpoint(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    task_tracker: TaskTracker = Depends(get_task_tracker),
) -> ProvisionStartResponse:
    """Re-scan and re-provision all objects for an already-provisioned DR."""
    from devmirror.provision.runner import provision_dr

    dr_repo, obj_repo, access_repo, audit_repo = _control_repos(settings)

    # 1. Get config from devmirror_configs table
    repo = _get_repo(settings)
    row = repo.get(db_client, dr_id=dr_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")

    # 2. Validate DR is ACTIVE or EXPIRING_SOON in control table
    dr_row = dr_repo.get(db_client, dr_id=dr_id)
    if dr_row is None:
        raise HTTPException(
            status_code=404,
            detail=f"DR {dr_id} not found in control tables",
        )
    dr_status = dr_row.get("status", "")
    if dr_status not in ("ACTIVE", "EXPIRING_SOON", "FAILED"):
        raise HTTPException(
            status_code=409,
            detail=f"DR {dr_id} has status {dr_status}. Re-provision is only allowed on ACTIVE, EXPIRING_SOON, or FAILED DRs.",
        )

    config_in = _parse_config_in(row["config_json"])
    dm_config = config_in.to_devmirror_config()
    tc = config_in.target_catalog

    def do_reprovision() -> dict:
        with _target_catalog_override(tc):
            # Re-run scan with current config
            try:
                manifest = _run_scan(db_client, settings, dm_config)
            except HTTPException as exc:
                raise RuntimeError(exc.detail) from exc

            # Update manifest
            manifest_json = json.dumps(manifest)
            scanned_at = datetime.now(UTC).isoformat()
            repo.update_manifest(
                db_client, dr_id=dr_id, manifest_json=manifest_json, scanned_at=scanned_at,
            )

            # Re-provision with force_replace=True
            result = provision_dr(
                dm_config,
                manifest,
                db_client=db_client,
                dr_repo=dr_repo,
                obj_repo=obj_repo,
                access_repo=access_repo,
                audit_repo=audit_repo,
                max_parallel=settings.max_parallel_clones,
                force_replace=True,
            )

        # 5. Update config status
        repo.update_status(db_client, dr_id=dr_id, status="provisioned")

        return {
            "final_status": result.final_status,
            "objects_succeeded": len(result.objects_succeeded),
            "objects_failed": len(result.objects_failed),
            "schemas_created": len(result.schemas_created),
            "grants_applied": result.grants_applied,
        }

    task_id = task_tracker.submit(dr_id, "reprovision", do_reprovision)
    return ProvisionStartResponse(
        dr_id=dr_id,
        task_id=task_id,
        status="reprovisioning",
        message=f"Re-provisioning started. Poll GET /api/tasks/{task_id} for progress.",
    )
