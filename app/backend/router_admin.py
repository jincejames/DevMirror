"""Admin-only endpoints for the approval queue."""
# ruff: noqa: B008  -- Depends() in function signatures is standard FastAPI pattern

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from .approvals import find_pending, list_pending
from .auth import require_admin
from .config import get_current_user, get_db_client, get_settings
from .helpers import _build_yaml, _control_repos, _get_repo

if TYPE_CHECKING:
    from devmirror.settings import Settings
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)
router_admin = APIRouter()


class PendingEditOut(BaseModel):
    pending_edit_id: str
    dr_id: str
    requested_by: str
    requested_at: str
    changes: list[dict]


class PendingProvisionOut(BaseModel):
    """A config that has been scanned and is awaiting admin provisioning."""

    dr_id: str
    description: str | None
    requested_by: str
    scanned_at: str | None
    total_objects: int
    total_schemas: int
    review_required: bool
    non_prod_additional_objects: list[str]


class ApprovalsListResponse(BaseModel):
    pending: list[PendingEditOut]
    pending_provisions: list[PendingProvisionOut]
    total: int


class ApproveResponse(BaseModel):
    pending_edit_id: str
    status: str
    message: str


class RejectRequest(BaseModel):
    reason: str | None = None


@router_admin.get(
    "/admin/approvals",
    response_model=ApprovalsListResponse,
    operation_id="listApprovals",
)
def list_approvals(
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    _: None = Depends(require_admin),
) -> ApprovalsListResponse:
    """Return all pending admin work: edits awaiting approval AND configs awaiting provisioning."""
    _, _, _, audit_repo = _control_repos(settings)

    # Pending edits (Phase 2 -- audit-log staging)
    raw = list_pending(audit_repo, db_client)
    edits = [
        PendingEditOut(
            pending_edit_id=r["pending_edit_id"],
            dr_id=r.get("dr_id", ""),
            requested_by=r.get("performed_by", ""),
            requested_at=r.get("performed_at", ""),
            changes=(r.get("detail") or {}).get("changes", []),
        )
        for r in raw
    ]

    # Pending provisions (configs in 'scanned' status awaiting "Approve & Provision")
    repo = _get_repo(settings, db_client)
    rows = repo.list_all(db_client)
    provisions: list[PendingProvisionOut] = []
    for row in rows:
        if row.get("status") != "scanned":
            continue
        manifest_raw = row.get("manifest_json")
        scan_result: dict = {}
        if manifest_raw:
            try:
                manifest = json.loads(manifest_raw)
                scan_result = manifest.get("scan_result") or manifest or {}
            except (json.JSONDecodeError, TypeError):
                scan_result = {}
        provisions.append(
            PendingProvisionOut(
                dr_id=row.get("dr_id", ""),
                description=row.get("description"),
                requested_by=row.get("created_by", ""),
                scanned_at=row.get("scanned_at"),
                total_objects=int(scan_result.get("total_objects") or 0),
                total_schemas=len(scan_result.get("schemas_required") or []),
                review_required=bool(scan_result.get("review_required")),
                non_prod_additional_objects=list(
                    scan_result.get("non_prod_additional_objects") or []
                ),
            )
        )

    return ApprovalsListResponse(
        pending=edits,
        pending_provisions=provisions,
        total=len(edits) + len(provisions),
    )


@router_admin.post(
    "/admin/approvals/{pending_edit_id}/approve",
    response_model=ApproveResponse,
    operation_id="approveEdit",
)
def approve_edit(
    pending_edit_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    current_user: str = Depends(get_current_user),
    _: None = Depends(require_admin),
) -> ApproveResponse:
    """Approve a pending edit: persist the proposed config and apply grants."""
    from devmirror.modify.modification_engine import _manage_users
    from devmirror.utils import now_iso

    from .models import ConfigIn

    _, obj_repo, access_repo, audit_repo = _control_repos(settings)
    pending = find_pending(audit_repo, db_client, pending_edit_id)
    if pending is None:
        raise HTTPException(
            status_code=404,
            detail=f"Pending edit {pending_edit_id} not found or already resolved",
        )

    detail = pending["detail"]
    proposed_json = detail.get("proposed_config_json")
    dr_id = pending["dr_id"]
    if not proposed_json:
        raise HTTPException(
            status_code=500, detail="Pending edit has no proposed config payload"
        )

    # Load current config row
    repo = _get_repo(settings, db_client)
    existing = repo.get(db_client, dr_id=dr_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")

    old_dict = json.loads(existing["config_json"])
    new_dict = json.loads(proposed_json)

    # Compute deltas to drive grant/revoke
    old_devs = set(old_dict.get("developers") or [])
    new_devs = set(new_dict.get("developers") or [])
    old_qa = set(old_dict.get("qa_users") or [])
    new_qa = set(new_dict.get("qa_users") or [])

    added_devs = sorted(new_devs - old_devs)
    removed_devs = sorted(old_devs - new_devs)
    added_qa = sorted(new_qa - old_qa)
    removed_qa = sorted(old_qa - new_qa)

    # Persist the new config row
    config_in = ConfigIn.model_validate(new_dict)
    repo.update(
        db_client,
        dr_id=dr_id,
        config_json=json.dumps(new_dict),
        config_yaml=_build_yaml(config_in),
        status=existing.get("status", "provisioned"),
        validation_errors="[]",
        expiration_date=new_dict.get("expiration_date", existing.get("expiration_date", "")),
        description=new_dict.get("description"),
    )

    # Apply grants
    try:
        if added_devs:
            _manage_users(
                "add_users", dr_id, added_devs, "dev",
                db_client, obj_repo, access_repo,
            )
        if removed_devs:
            _manage_users(
                "remove_users", dr_id, removed_devs, "dev",
                db_client, obj_repo, access_repo,
            )
        if added_qa:
            _manage_users(
                "add_users", dr_id, added_qa, "qa",
                db_client, obj_repo, access_repo,
            )
        if removed_qa:
            _manage_users(
                "remove_users", dr_id, removed_qa, "qa",
                db_client, obj_repo, access_repo,
            )
    except Exception as exc:
        logger.exception("Failed to apply grants on approval for %s", dr_id)
        # Still write the APPROVED audit (the config row is already updated).
        audit_repo.append(
            db_client,
            dr_id=dr_id,
            action="CONFIG_EDIT_APPROVED",
            performed_by=current_user,
            performed_at=now_iso(),
            status="PARTIAL",
            error_message=str(exc),
            action_detail=json.dumps({"pending_edit_id": pending_edit_id}),
        )
        return ApproveResponse(
            pending_edit_id=pending_edit_id,
            status="partial",
            message=f"Approved but grant application failed: {exc}",
        )

    audit_repo.append(
        db_client,
        dr_id=dr_id,
        action="CONFIG_EDIT_APPROVED",
        performed_by=current_user,
        performed_at=now_iso(),
        status="SUCCESS",
        action_detail=json.dumps({
            "pending_edit_id": pending_edit_id,
            "changes": detail.get("changes", []),
        }),
    )
    return ApproveResponse(
        pending_edit_id=pending_edit_id,
        status="approved",
        message="Edit applied.",
    )


@router_admin.post(
    "/admin/approvals/{pending_edit_id}/reject",
    response_model=ApproveResponse,
    operation_id="rejectEdit",
)
def reject_edit(
    pending_edit_id: str,
    body: RejectRequest | None = None,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    current_user: str = Depends(get_current_user),
    _: None = Depends(require_admin),
) -> ApproveResponse:
    """Reject a pending edit: write a CONFIG_EDIT_REJECTED audit row."""
    from devmirror.utils import now_iso

    _, _, _, audit_repo = _control_repos(settings)
    pending = find_pending(audit_repo, db_client, pending_edit_id)
    if pending is None:
        raise HTTPException(
            status_code=404,
            detail=f"Pending edit {pending_edit_id} not found or already resolved",
        )

    audit_repo.append(
        db_client,
        dr_id=pending["dr_id"],
        action="CONFIG_EDIT_REJECTED",
        performed_by=current_user,
        performed_at=now_iso(),
        status="REJECTED",
        action_detail=json.dumps({
            "pending_edit_id": pending_edit_id,
            "reason": (body.reason if body else None) or "",
        }),
    )
    return ApproveResponse(
        pending_edit_id=pending_edit_id,
        status="rejected",
        message="Edit rejected.",
    )
