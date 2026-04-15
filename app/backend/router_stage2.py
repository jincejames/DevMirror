"""Stage 2 API router: scan, provision, task status, DR status/list, cleanup, refresh, reprovision."""
# ruff: noqa: B008  -- Depends() in function signatures is standard FastAPI pattern

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException

from .config import get_db_client, get_settings, get_task_tracker
from .helpers import (
    _control_repos,
    _get_repo,
    _parse_config_in,
    _run_scan,
    _target_catalog_override,
)
from .models import (
    CleanupResponse,
    DrListItem,
    DrListResponse,
    DrStatusResponse,
    ManifestResponse,
    ProvisionStartResponse,
    RefreshRequest,
    RefreshStartResponse,
    ScanResponse,
    TaskStatusResponse,
)
from .tasks import TaskTracker

if TYPE_CHECKING:
    from devmirror.settings import Settings
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)

router_stage2 = APIRouter()


# ---- 9. POST /api/configs/{dr_id}/scan (scanConfig) -----------------------

@router_stage2.post(
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

@router_stage2.get(
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

@router_stage2.put(
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

@router_stage2.post(
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
        # Only mark as "provisioned" if at least some objects succeeded
        if len(result.objects_succeeded) > 0:
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

@router_stage2.get(
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

@router_stage2.get(
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

@router_stage2.get(
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

@router_stage2.post(
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

@router_stage2.post(
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

@router_stage2.post(
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
                # Surface the scan error so the task status shows it
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

        # Only mark as "provisioned" if at least some objects succeeded
        if len(result.objects_succeeded) > 0:
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
