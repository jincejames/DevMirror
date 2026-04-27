"""Phase 2 approval workflow helpers.

Sensitive edits to provisioned DRs are staged in the audit log as
``CONFIG_EDIT_PENDING`` rows. An admin approves or rejects via the new
admin endpoints, which write a paired ``CONFIG_EDIT_APPROVED`` /
``CONFIG_EDIT_REJECTED`` row referencing the same ``pending_edit_id``.
"""
from __future__ import annotations

import json
import uuid
from typing import Any

# Sensitive fields whose edit on a provisioned DR requires admin approval.
SENSITIVE_FIELDS = ("access.developers", "access.qa_users", "additional_objects")


def compute_diff(old: dict[str, Any], new: dict[str, Any]) -> list[dict[str, Any]]:
    """Return [{field, before, after}, ...] for fields that differ.

    Compares the union of ``access.developers``, ``access.qa_users``,
    ``additional_objects``, ``description``, ``lifecycle.expiration_date``.
    """
    fields = (
        ("access.developers", "developers"),
        ("access.qa_users", "qa_users"),
        ("additional_objects", "additional_objects"),
        ("description", "description"),
        ("lifecycle.expiration_date", "expiration_date"),
    )
    changes: list[dict[str, Any]] = []
    for label, key in fields:
        before = old.get(key)
        after = new.get(key)
        # Treat None and empty list as equivalent for list fields
        if isinstance(before, list) or isinstance(after, list):
            before_norm = sorted(before or [])
            after_norm = sorted(after or [])
            if before_norm != after_norm:
                changes.append({"field": label, "before": before_norm, "after": after_norm})
        else:
            if (before or None) != (after or None):
                changes.append({"field": label, "before": before, "after": after})
    return changes


def has_sensitive_change(changes: list[dict[str, Any]]) -> bool:
    return any(c["field"] in SENSITIVE_FIELDS for c in changes)


def new_pending_edit_id() -> str:
    return f"pe-{uuid.uuid4().hex[:12]}"


def stage_pending_edit(
    audit_repo,
    db_client,
    *,
    dr_id: str,
    requester: str,
    proposed_config_json: str,
    changes: list[dict[str, Any]],
    original_created_by: str | None = None,
) -> str:
    """Write a CONFIG_EDIT_PENDING audit row and return the pending_edit_id.

    ``original_created_by`` snapshots the config row's owner at staging
    time.  The approve endpoint compares this against the live
    ``created_by`` to detect a delete-and-recreate-with-same-dr_id race.
    """
    from devmirror.utils import now_iso
    pending_edit_id = new_pending_edit_id()
    detail = {
        "pending_edit_id": pending_edit_id,
        "changes": changes,
        "proposed_config_json": proposed_config_json,
        "original_created_by": original_created_by,
    }
    audit_repo.append(
        db_client,
        dr_id=dr_id,
        action="CONFIG_EDIT_PENDING",
        performed_by=requester,
        performed_at=now_iso(),
        status="PENDING",
        action_detail=json.dumps(detail),
    )
    return pending_edit_id


def list_pending(audit_repo, db_client) -> list[dict[str, Any]]:
    """Return CONFIG_EDIT_PENDING audit rows that have no APPROVED/REJECTED twin.

    The audit log doesn't have a status-resolution column, so we read all
    PENDING rows then exclude any whose pending_edit_id appears in a later
    APPROVED/REJECTED row.
    """
    rows = audit_repo.list_by_action(db_client, action="CONFIG_EDIT_PENDING")
    resolved_ids = {
        _extract_pending_id(r)
        for r in audit_repo.list_by_action(db_client, action="CONFIG_EDIT_APPROVED")
    } | {
        _extract_pending_id(r)
        for r in audit_repo.list_by_action(db_client, action="CONFIG_EDIT_REJECTED")
    }
    pending: list[dict[str, Any]] = []
    for r in rows:
        pid = _extract_pending_id(r)
        if pid and pid not in resolved_ids:
            r_copy = dict(r)
            r_copy["pending_edit_id"] = pid
            r_copy["detail"] = _safe_load(r.get("action_detail"))
            pending.append(r_copy)
    return pending


def find_pending(audit_repo, db_client, pending_edit_id: str) -> dict[str, Any] | None:
    for r in list_pending(audit_repo, db_client):
        if r.get("pending_edit_id") == pending_edit_id:
            return r
    return None


def _extract_pending_id(row: dict[str, Any]) -> str | None:
    detail = _safe_load(row.get("action_detail"))
    if isinstance(detail, dict):
        return detail.get("pending_edit_id")
    return None


def _safe_load(s: Any) -> Any:
    if not s:
        return None
    try:
        return json.loads(s) if isinstance(s, str) else s
    except (json.JSONDecodeError, TypeError):
        return None
