"""Cleanup engine for expired development requests."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from devmirror.control.control_table import DRStatus, ObjectStatus
from devmirror.utils import now_iso

if TYPE_CHECKING:
    from devmirror.control.audit import AuditRepository
    from devmirror.control.control_table import (
        DrAccessRepository,
        DrObjectRepository,
        DRRepository,
    )
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)


@dataclass
class CleanupResult:
    """Outcome of a single DR cleanup run."""

    dr_id: str
    objects_dropped: int = 0
    objects_skipped: int = 0
    objects_failed: list[tuple[str, str]] = field(default_factory=list)
    revokes_succeeded: int = 0
    revokes_failed: list[tuple[str, str]] = field(default_factory=list)
    schemas_dropped: int = 0
    schemas_failed: list[tuple[str, str]] = field(default_factory=list)
    final_status: str = ""

    @property
    def fully_cleaned(self) -> bool:
        """True when no failures occurred during cleanup."""
        return (
            len(self.objects_failed) == 0
            and len(self.revokes_failed) == 0
            and len(self.schemas_failed) == 0
        )


def _drop_object_sql(target_fqn: str, object_type: str) -> str:
    """Generate DROP IF EXISTS SQL for an object."""
    if object_type == "view":
        return f"DROP VIEW IF EXISTS {target_fqn}"
    return f"DROP TABLE IF EXISTS {target_fqn}"


def _drop_schema_sql(schema_fqn: str) -> str:
    """Generate DROP SCHEMA IF EXISTS ... CASCADE SQL."""
    return f"DROP SCHEMA IF EXISTS {schema_fqn} CASCADE"


def _collect_schemas_from_objects(
    objects: list[dict[str, Any]],
) -> list[str]:
    """Extract unique target schema FQNs from object rows."""
    schemas: dict[str, None] = {}
    for obj in objects:
        target_fqn = obj.get("target_fqn", "")
        parts = target_fqn.split(".")
        if len(parts) >= 2:
            schema_fqn = f"{parts[0]}.{parts[1]}"
            schemas[schema_fqn] = None
    return list(schemas)


def cleanup_dr(
    dr_id: str,
    *,
    db_client: DbClient,
    dr_repo: DRRepository,
    obj_repo: DrObjectRepository,
    access_repo: DrAccessRepository,
    audit_repo: AuditRepository,
    current_status: DRStatus | None = None,
) -> CleanupResult:
    """Execute the full cleanup flow for a development request."""
    result = CleanupResult(dr_id=dr_id)
    now = now_iso()

    # Transition to CLEANUP_IN_PROGRESS
    if current_status is None:
        dr_row = dr_repo.get(db_client, dr_id=dr_id)
        if dr_row is None:
            result.final_status = "NOT_FOUND"
            return result
        current_status = DRStatus(dr_row["status"])

    if current_status != DRStatus.CLEANUP_IN_PROGRESS:
        try:
            dr_repo.update_status(
                db_client,
                dr_id=dr_id,
                current_status=current_status,
                new_status=DRStatus.CLEANUP_IN_PROGRESS,
                last_modified_at=now,
            )
        except Exception as exc:
            logger.error("Failed to set CLEANUP_IN_PROGRESS for %s: %s", dr_id, exc)
            result.final_status = current_status.value
            return result

    audit_repo.append(
        db_client,
        dr_id=dr_id,
        action="CLEANUP",
        performed_by="SYSTEM",
        performed_at=now,
        status="SUCCESS",
        action_detail=json.dumps({"phase": "start"}),
    )

    # Revoke all grants via SDK
    access_rows = access_repo.list_by_dr_id(db_client, dr_id=dr_id)
    objects = obj_repo.list_by_dr_id(db_client, dr_id=dr_id)
    schemas_from_objects = _collect_schemas_from_objects(objects)

    for access_row in access_rows:
        principal = access_row.get("user_email", "")
        if not principal:
            continue
        for schema_fqn in schemas_from_objects:
            try:
                from databricks.sdk.service.catalog import Privilege, SecurableType

                db_client.revoke(
                    SecurableType.SCHEMA, schema_fqn, principal,
                    [Privilege.USE_SCHEMA, Privilege.SELECT, Privilege.MODIFY],
                )
                result.revokes_succeeded += 1
            except Exception as exc:
                msg = f"REVOKE on {schema_fqn} for {principal}"
                logger.error("Revoke failed: %s -- %s", msg, exc)
                result.revokes_failed.append((msg, str(exc)))

    # Drop objects using SDK delete_table (views first, then tables)
    views = [o for o in objects if o.get("object_type") == "view"]
    tables = [o for o in objects if o.get("object_type") != "view"]

    for obj in views + tables:
        obj_status_raw = obj.get("status", "")

        if obj_status_raw == ObjectStatus.DROPPED.value:
            result.objects_skipped += 1
            continue

        target_fqn = obj.get("target_fqn", "")

        try:
            db_client.delete_table(target_fqn)
            result.objects_dropped += 1

            try:
                current_obj_status = ObjectStatus(obj_status_raw) if obj_status_raw else None
                if current_obj_status is not None:
                    obj_repo.update_object_status(
                        db_client,
                        dr_id=dr_id,
                        source_fqn=obj.get("source_fqn", ""),
                        target_environment=obj.get("target_environment", "dev"),
                        current_status=current_obj_status,
                        new_status=ObjectStatus.DROPPED,
                    )
            except Exception as status_exc:
                logger.debug("Failed to update status to DROPPED: %s", status_exc)
        except Exception as exc:
            logger.error("Failed to drop %s: %s", target_fqn, exc)
            result.objects_failed.append((target_fqn, str(exc)))

    # Drop schemas via SDK
    for schema_fqn in schemas_from_objects:
        parts = schema_fqn.split(".")
        if len(parts) == 2:
            try:
                db_client.delete_schema(parts[0], parts[1])
                result.schemas_dropped += 1
            except Exception as exc:
                logger.error("Failed to drop schema %s: %s", schema_fqn, exc)
                result.schemas_failed.append((schema_fqn, str(exc)))
        else:
            result.schemas_failed.append((schema_fqn, "Invalid schema FQN"))

    # Set final DR status
    now_final = now_iso()
    if result.fully_cleaned:
        try:
            dr_repo.update_status(
                db_client,
                dr_id=dr_id,
                current_status=DRStatus.CLEANUP_IN_PROGRESS,
                new_status=DRStatus.CLEANED_UP,
                last_modified_at=now_final,
            )
            result.final_status = DRStatus.CLEANED_UP.value
        except Exception as exc:
            logger.error("Failed to set CLEANED_UP for %s: %s", dr_id, exc)
            result.final_status = DRStatus.CLEANUP_IN_PROGRESS.value
    else:
        result.final_status = DRStatus.CLEANUP_IN_PROGRESS.value

    # Audit entry
    audit_status = "SUCCESS" if result.fully_cleaned else "PARTIAL_SUCCESS"
    error_msg = None
    if not result.fully_cleaned:
        error_detail: dict[str, Any] = {}
        if result.objects_failed:
            error_detail["objects_failed"] = [fqn for fqn, _ in result.objects_failed]
        if result.revokes_failed:
            error_detail["revokes_failed"] = [desc for desc, _ in result.revokes_failed]
        if result.schemas_failed:
            error_detail["schemas_failed"] = [fqn for fqn, _ in result.schemas_failed]
        error_msg = json.dumps(error_detail)

    audit_repo.append(
        db_client,
        dr_id=dr_id,
        action="CLEANUP",
        performed_by="SYSTEM",
        performed_at=now_final,
        status=audit_status,
        action_detail=json.dumps({
            "phase": "complete",
            "objects_dropped": result.objects_dropped,
            "objects_skipped": result.objects_skipped,
            "revokes_succeeded": result.revokes_succeeded,
            "schemas_dropped": result.schemas_dropped,
        }),
        error_message=error_msg,
    )

    return result


def find_expired_drs(
    db_client: DbClient,
    dr_repo: DRRepository,
) -> list[dict[str, Any]]:
    """Find DRs eligible for cleanup."""
    table = dr_repo.table_fqn
    sql = (
        f"SELECT * FROM {table} "
        "WHERE (expiration_date <= CURRENT_DATE() "
        "AND status = :active_status) "
        "OR status = :cleanup_status"
    )
    params: dict[str, str | None] = {
        "active_status": DRStatus.ACTIVE.value,
        "cleanup_status": DRStatus.CLEANUP_IN_PROGRESS.value,
    }
    return db_client.sql_with_params(sql, params)
