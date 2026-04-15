"""Refresh engine for re-syncing DEV objects from production."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING, Any, Literal

from devmirror.control.control_table import DRStatus, ObjectStatus
from devmirror.provision.object_cloner import ClonerError, _revision_clause, _validate_fqn
from devmirror.utils import TaskResult, now_iso, run_bounded
from devmirror.utils.validation import validate_delta_retention

if TYPE_CHECKING:
    from devmirror.config.schema import DataRevision
    from devmirror.control.audit import AuditRepository
    from devmirror.control.control_table import DrObjectRepository, DRRepository
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)

RefreshMode = Literal["full", "incremental", "selective"]

_ACTIVE_STATUSES = frozenset({DRStatus.ACTIVE.value, DRStatus.EXPIRING_SOON.value})


class RefreshError(Exception):
    """Raised when refresh validation or execution fails."""


@dataclass
class ObjectRefreshResult:
    """Outcome of refreshing a single object."""

    source_fqn: str
    target_fqn: str
    strategy: str
    sql: str
    success: bool
    error: str | None = None


@dataclass
class RefreshResult:
    """Overall result of a refresh run."""

    dr_id: str
    mode: RefreshMode
    objects_succeeded: list[ObjectRefreshResult] = field(default_factory=list)
    objects_failed: list[ObjectRefreshResult] = field(default_factory=list)
    audit_status: str = "SUCCESS"


def _generate_object_sql(
    source_fqn: str,
    target_fqn: str,
    strategy: str,
    data_revision: DataRevision | None = None,
    *,
    full_refresh: bool = False,
) -> list[str]:
    """Generate SQL statements for refreshing a single object."""
    _validate_fqn(source_fqn, "source_fqn")
    _validate_fqn(target_fqn, "target_fqn")
    rev = _revision_clause(data_revision)

    if strategy == "shallow_clone":
        if full_refresh:
            return [
                f"DROP TABLE IF EXISTS {target_fqn}",
                f"CREATE TABLE {target_fqn} SHALLOW CLONE {source_fqn}{rev}",
            ]
        return [f"CREATE OR REPLACE TABLE {target_fqn} SHALLOW CLONE {source_fqn}{rev}"]

    if strategy == "deep_clone":
        if full_refresh:
            return [
                f"DROP TABLE IF EXISTS {target_fqn}",
                f"CREATE TABLE {target_fqn} DEEP CLONE {source_fqn}{rev}",
            ]
        return [f"CREATE OR REPLACE TABLE {target_fqn} DEEP CLONE {source_fqn}{rev}"]

    if strategy == "view":
        if full_refresh:
            return [
                f"DROP VIEW IF EXISTS {target_fqn}",
                f"CREATE VIEW {target_fqn} AS SELECT * FROM {source_fqn}{rev}",
            ]
        return [f"CREATE OR REPLACE VIEW {target_fqn} AS SELECT * FROM {source_fqn}{rev}"]

    if strategy == "schema_only":
        if full_refresh:
            return [
                f"DROP TABLE IF EXISTS {target_fqn}",
                f"CREATE TABLE {target_fqn} LIKE {source_fqn}",
            ]
        return [f"TRUNCATE TABLE {target_fqn}"]

    raise ClonerError(f"Unknown clone strategy for refresh: {strategy!r}")


def _filter_objects(
    obj_rows: list[dict[str, Any]],
    mode: RefreshMode,
    selected_fqns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Filter object rows based on refresh mode."""
    active = [
        r for r in obj_rows
        if r.get("status") not in (ObjectStatus.DROPPED.value, None)
    ]

    if mode == "full":
        return active

    if mode == "incremental":
        return [
            r for r in active
            if r.get("clone_strategy") in ("shallow_clone", "deep_clone")
        ]

    if mode == "selective":
        if not selected_fqns:
            return []
        fqn_set = set(selected_fqns)
        return [r for r in active if r.get("source_fqn") in fqn_set]

    return active


def _refresh_single_object(
    db_client: DbClient,
    obj_row: dict[str, Any],
    data_revision: DataRevision | None,
    mode: RefreshMode,
) -> ObjectRefreshResult:
    """Refresh a single object, returning the result."""
    source_fqn = obj_row["source_fqn"]
    target_fqn = obj_row["target_fqn"]
    strategy = obj_row["clone_strategy"]

    try:
        statements = _generate_object_sql(
            source_fqn, target_fqn, strategy, data_revision,
            full_refresh=(mode == "full"),
        )
        for sql in statements:
            db_client.sql_exec(sql)
        return ObjectRefreshResult(
            source_fqn=source_fqn,
            target_fqn=target_fqn,
            strategy=strategy,
            sql="; ".join(statements),
            success=True,
        )
    except Exception as exc:
        logger.error("Refresh failed for %s -> %s: %s", source_fqn, target_fqn, exc)
        return ObjectRefreshResult(
            source_fqn=source_fqn,
            target_fqn=target_fqn,
            strategy=strategy,
            sql="",
            success=False,
            error=str(exc),
        )


def refresh_dr(
    dr_id: str,
    mode: RefreshMode,
    *,
    db_client: DbClient,
    dr_repo: DRRepository,
    obj_repo: DrObjectRepository,
    audit_repo: AuditRepository,
    data_revision: DataRevision | None = None,
    selected_fqns: list[str] | None = None,
    max_parallel: int = 10,
) -> RefreshResult:
    """Execute a refresh for the given DR."""
    result = RefreshResult(dr_id=dr_id, mode=mode)

    # Validate DR (inlined from _validate_dr_for_refresh)
    dr_row = dr_repo.get(db_client, dr_id=dr_id)
    if dr_row is None:
        raise RefreshError(f"Development request {dr_id!r} not found.")
    status = dr_row.get("status", "")
    if status not in _ACTIVE_STATUSES:
        raise RefreshError(
            f"DR {dr_id!r} is not active (status={status!r}). "
            "Refresh is only allowed on ACTIVE or EXPIRING_SOON DRs."
        )
    exp_date_raw = dr_row.get("expiration_date")
    if exp_date_raw:
        exp_date = (
            date.fromisoformat(exp_date_raw) if isinstance(exp_date_raw, str)
            else exp_date_raw if isinstance(exp_date_raw, date)
            else None
        )
        if exp_date is not None and exp_date < date.today():
            raise RefreshError(
                f"DR {dr_id!r} has expired (expiration_date={exp_date.isoformat()}). "
                "Refresh is not allowed on expired DRs."
            )

    # Validate Delta retention
    if data_revision is not None and data_revision.mode != "latest":
        all_obj_rows = obj_repo.list_by_dr_id(db_client, dr_id=dr_id)
        source_fqns_for_retention = [
            row["source_fqn"] for row in all_obj_rows if row.get("source_fqn")
        ]
        retention_warnings = validate_delta_retention(
            db_client, source_fqns_for_retention, data_revision
        )
        for warning in retention_warnings:
            logger.warning("Delta retention check: %s", warning)

    # Load and filter objects
    obj_rows = obj_repo.list_by_dr_id(db_client, dr_id=dr_id)
    targets = _filter_objects(obj_rows, mode, selected_fqns)

    if not targets:
        logger.info("No objects to refresh for DR %s (mode=%s).", dr_id, mode)
        audit_repo.append(
            db_client,
            dr_id=dr_id,
            action="REFRESH",
            performed_by="SYSTEM",
            performed_at=now_iso(),
            status="SUCCESS",
            action_detail=json.dumps({
                "mode": mode,
                "objects_refreshed": 0,
                "objects_failed": 0,
            }),
        )
        return result

    # Execute refresh in parallel
    def _make_task(row: dict[str, Any]) -> ObjectRefreshResult:
        return _refresh_single_object(db_client, row, data_revision, mode)

    tasks = [lambda r=row: _make_task(r) for row in targets]
    task_results: list[TaskResult] = run_bounded(tasks, max_workers=max_parallel)

    # Collect results
    now = now_iso()
    for tr, row in zip(task_results, targets, strict=True):
        obj_result: ObjectRefreshResult = tr.value  # type: ignore[assignment]
        if tr.success and obj_result is not None and obj_result.success:
            result.objects_succeeded.append(obj_result)
            try:
                current_status = ObjectStatus(row.get("status", "PROVISIONED"))
                obj_repo.update_object_status(
                    db_client,
                    dr_id=dr_id,
                    source_fqn=row["source_fqn"],
                    target_environment=row["target_environment"],
                    current_status=current_status,
                    new_status=ObjectStatus.PROVISIONED,
                    last_refreshed_at=now,
                )
            except Exception:
                logger.debug("Object status update after refresh failed, non-fatal")
        else:
            if obj_result is not None and not obj_result.success:
                result.objects_failed.append(obj_result)
            else:
                result.objects_failed.append(ObjectRefreshResult(
                    source_fqn=row["source_fqn"],
                    target_fqn=row["target_fqn"],
                    strategy=row["clone_strategy"],
                    sql="",
                    success=False,
                    error=tr.error or "unknown error",
                ))

    # Update DR last_refreshed_at
    try:
        dr_last_refresh_sql = (
            f"UPDATE {dr_repo.table_fqn} SET "
            f"last_refreshed_at = '{now}' "
            f"WHERE dr_id = '{dr_id}'"
        )
        db_client.sql_exec(dr_last_refresh_sql)
    except Exception:
        logger.debug("DR last_refreshed_at update failed, non-fatal")

    # Determine audit status
    if not result.objects_succeeded and result.objects_failed:
        result.audit_status = "FAILED"
    elif result.objects_failed and result.objects_succeeded:
        result.audit_status = "PARTIAL_SUCCESS"
    else:
        result.audit_status = "SUCCESS"

    error_msg_audit = None
    if result.objects_failed:
        failed_sources = [r.source_fqn for r in result.objects_failed]
        error_msg_audit = json.dumps({"failed_objects": failed_sources})

    audit_repo.append(
        db_client,
        dr_id=dr_id,
        action="REFRESH",
        performed_by="SYSTEM",
        performed_at=now_iso(),
        status=result.audit_status,
        action_detail=json.dumps({
            "mode": mode,
            "objects_refreshed": len(result.objects_succeeded),
            "objects_failed": len(result.objects_failed),
        }),
        error_message=error_msg_audit,
    )

    return result
