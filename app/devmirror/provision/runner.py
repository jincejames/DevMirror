"""Provision orchestration runner."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from devmirror.control.control_table import DRStatus, ObjectStatus
from devmirror.provision.access_manager import apply_grants
from devmirror.provision.object_cloner import (
    CloneResult,
    default_clone_strategy,
    execute_clone,
    provision_schemas,
)
from devmirror.utils import now_iso, revision_values, run_bounded
from devmirror.utils.naming import (
    required_target_schemas,
    resolve_target_catalog,
    target_object_fqn,
)
from devmirror.utils.validation import validate_delta_retention

if TYPE_CHECKING:
    from devmirror.config.schema import DevMirrorConfig
    from devmirror.control.audit import AuditRepository
    from devmirror.control.control_table import (
        DrAccessRepository,
        DrObjectRepository,
        DRRepository,
    )
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)


@dataclass
class ProvisionResult:
    """Overall result of a provisioning run."""

    dr_id: str
    schemas_created: list[str] = field(default_factory=list)
    schemas_failed: dict[str, str] = field(default_factory=dict)
    objects_succeeded: list[CloneResult] = field(default_factory=list)
    objects_failed: list[CloneResult] = field(default_factory=list)
    grants_applied: int = 0
    grants_failed: list[tuple[str, str]] = field(default_factory=list)
    final_status: str = ""

    @property
    def is_partial_success(self) -> bool:
        return bool(self.objects_failed) and bool(self.objects_succeeded)

    @property
    def all_objects_failed(self) -> bool:
        return not self.objects_succeeded and bool(self.objects_failed)




def _build_object_rows(
    config: DevMirrorConfig,
    manifest: dict[str, Any],
    env: str,
) -> list[dict[str, Any]]:
    """Build object row dicts from manifest for a given environment."""
    dr = config.development_request
    dr_id = dr.dr_id
    data_revision = dr.data_revision
    objects = manifest["scan_result"]["objects"]

    rows: list[dict[str, Any]] = []
    for obj in objects:
        source_fqn = obj["fqn"]
        parts = source_fqn.split(".")
        if len(parts) != 3:
            continue

        source_catalog = parts[0]
        target_catalog = resolve_target_catalog(source_catalog, env)
        t_fqn = target_object_fqn(target_catalog, source_fqn, dr_id, env)

        obj_type = obj.get("type", "table")
        access_mode = obj.get("access_mode", "READ_ONLY")

        # Use manifest override or default strategy
        strategy = obj.get("clone_strategy") or default_clone_strategy(obj_type, access_mode)

        rows.append({
            "dr_id": dr_id,
            "source_fqn": source_fqn,
            "target_fqn": t_fqn,
            "target_environment": env,
            "object_type": obj_type,
            "access_mode": access_mode,
            "clone_strategy": strategy,
            "clone_revision_mode": data_revision.mode,
            "clone_revision_value": revision_values(data_revision)[1],
            "provisioned_at": None,
            "last_refreshed_at": None,
            "status": ObjectStatus.REFRESH_PENDING.value,
            "estimated_size_gb": obj.get("estimated_size_gb"),
        })

    return rows


def _get_schemas_for_env(
    config: DevMirrorConfig,
    manifest: dict[str, Any],
    env: str,
) -> list[str]:
    """Compute target schema FQNs for an environment from the manifest."""
    dr = config.development_request
    prod_schemas = manifest["scan_result"]["schemas_required"]

    all_target_schemas: list[str] = []
    for prod_schema_fqn in prod_schemas:
        parts = prod_schema_fqn.split(".")
        if len(parts) < 2:
            continue
        source_catalog = parts[0]
        target_catalog = resolve_target_catalog(source_catalog, env)
        targets = required_target_schemas(
            target_catalog, [prod_schema_fqn], dr.dr_id, env
        )
        all_target_schemas.extend(targets)

    return sorted(set(all_target_schemas))


class SchemaCollisionError(Exception):
    """Raised when an active DR already occupies the same schema prefix."""


def provision_dr(
    config: DevMirrorConfig,
    manifest: dict[str, Any],
    *,
    db_client: DbClient,
    dr_repo: DRRepository,
    obj_repo: DrObjectRepository,
    access_repo: DrAccessRepository,
    audit_repo: AuditRepository,
    max_parallel: int = 10,
    force_replace: bool = False,
) -> ProvisionResult:
    """Execute the full provisioning flow for a development request."""
    dr = config.development_request
    dr_id = dr.dr_id
    result = ProvisionResult(dr_id=dr_id)

    now = now_iso()

    # Collision detection
    active_statuses = frozenset({DRStatus.ACTIVE.value, DRStatus.EXPIRING_SOON.value})
    existing_dr = dr_repo.get(db_client, dr_id=dr_id)
    if existing_dr is not None:
        existing_status = existing_dr.get("status", "")
        if existing_status in active_statuses:
            logger.warning(
                "DR %s already exists with status %s. Objects will be replaced.",
                dr_id,
                existing_status,
            )
            review_required = manifest.get("scan_result", {}).get("review_required", False)
            if review_required and not force_replace:
                raise SchemaCollisionError(
                    f"DR {dr_id} already exists with status {existing_status} and "
                    f"manifest has review_required=True. Pass force_replace=True "
                    f"(or --auto-approve) to proceed."
                )

    # Insert DR record (or, if it already exists, normalise its status to
    # PROVISIONING so the post-work CAS update can find it).  Without this,
    # a re-provision of a previously FAILED DR would silently no-op its
    # final status update -- the row would stay at FAILED even after every
    # object successfully cloned.
    inserted = False
    try:
        dr_repo.insert(
            db_client,
            dr_id=dr_id,
            description=dr.description,
            status=DRStatus.PROVISIONING.value,
            config_yaml=config.model_dump_json(),
            created_at=now,
            created_by=dr.access.developers[0] if dr.access.developers else "SYSTEM",
            expiration_date=dr.lifecycle.expiration_date.isoformat(),
            last_modified_at=now,
        )
        inserted = True
    except Exception:
        logger.debug("DR insert may have failed (possibly already exists), continuing")

    if not inserted and existing_dr is not None:
        # Row exists -- normalise to PROVISIONING before any work runs.  We
        # use force_status (no CAS gate) because the previous CAS-based
        # approach silently no-op'd whenever the live row's status didn't
        # match the value cached in existing_dr (e.g. a row stuck at FAILED
        # from a prior crashed run).  The runner is the authoritative
        # writer for this DR thanks to TaskTracker single-flighting, so
        # losing the CAS check is safe.
        try:
            dr_repo.force_status(
                db_client,
                dr_id=dr_id,
                new_status=DRStatus.PROVISIONING,
                last_modified_at=now,
            )
        except Exception as exc:
            logger.error(
                "Pre-provision force_status to PROVISIONING failed for %s: %s",
                dr_id, exc,
            )
            raise

    # Audit start
    audit_repo.append(
        db_client,
        dr_id=dr_id,
        action="PROVISION",
        performed_by="SYSTEM",
        performed_at=now,
        status="SUCCESS",
        action_detail=json.dumps({"phase": "start"}),
    )

    # Determine environments
    envs = ["dev"]
    if dr.environments.qa and dr.environments.qa.enabled:
        envs.append("qa")

    all_schemas: list[str] = []
    all_object_rows: list[dict[str, Any]] = []

    for env in envs:
        schemas = _get_schemas_for_env(config, manifest, env)
        all_schemas.extend(schemas)
        obj_rows = _build_object_rows(config, manifest, env)
        all_object_rows.extend(obj_rows)

    # Validate Delta retention window
    if dr.data_revision.mode != "latest":
        source_fqns = [row["source_fqn"] for row in all_object_rows]
        retention_warnings = validate_delta_retention(
            db_client, source_fqns, dr.data_revision
        )
        for warning in retention_warnings:
            logger.warning("Delta retention check: %s", warning)

    # Provision schemas
    schema_result = provision_schemas(db_client, all_schemas)
    result.schemas_created = schema_result.created
    result.schemas_failed = schema_result.failed

    if not schema_result.all_succeeded:
        logger.warning(
            "Some schemas failed to create: %s",
            list(schema_result.failed.keys()),
        )

    # Clear stale object rows from previous provision attempts
    if force_replace:
        try:
            obj_repo.delete_by_dr_id(db_client, dr_id=dr_id)
        except Exception:
            logger.debug("Stale object row cleanup failed, continuing")

    # Insert planned object rows
    try:
        obj_repo.bulk_insert(db_client, objects=all_object_rows)
    except Exception:
        logger.debug("Object row insert may have partially failed, continuing")

    # Clone objects with bounded parallelism
    data_revision = dr.data_revision

    def _clone_one(obj_row: dict[str, Any]) -> CloneResult:
        return execute_clone(
            db_client,
            source_fqn=obj_row["source_fqn"],
            target_fqn=obj_row["target_fqn"],
            strategy=obj_row["clone_strategy"],
            data_revision=data_revision,
        )

    tasks = [lambda r=row: _clone_one(r) for row in all_object_rows]
    task_results = run_bounded(tasks, max_workers=max_parallel)

    for tr, row in zip(task_results, all_object_rows, strict=True):
        clone_result: CloneResult
        if tr.success and tr.value is not None:
            clone_result = tr.value  # type: ignore[assignment]
        else:
            clone_result = CloneResult(
                source_fqn=row["source_fqn"],
                target_fqn=row["target_fqn"],
                strategy=row["clone_strategy"],
                sql="",
                success=False,
                error=tr.error or "unknown",
            )

        if clone_result.success:
            result.objects_succeeded.append(clone_result)
            # Mark PROVISIONED
            try:
                obj_repo.update_object_status(
                    db_client,
                    dr_id=dr_id,
                    source_fqn=clone_result.source_fqn,
                    target_environment=row["target_environment"],
                    current_status=ObjectStatus.REFRESH_PENDING,
                    new_status=ObjectStatus.PROVISIONED,
                    last_refreshed_at=now_iso(),
                )
            except Exception:
                logger.debug("Status update to PROVISIONED failed, non-fatal")
        else:
            result.objects_failed.append(clone_result)
            # Mark FAILED
            try:
                obj_repo.update_object_status(
                    db_client,
                    dr_id=dr_id,
                    source_fqn=clone_result.source_fqn,
                    target_environment=row["target_environment"],
                    current_status=ObjectStatus.REFRESH_PENDING,
                    new_status=ObjectStatus.FAILED,
                )
            except Exception:
                logger.debug("Status update to FAILED failed, non-fatal")

    # Grant access -- skip if no schemas were created for this environment
    # (otherwise apply_grants would still validate principals and produce
    # spurious "principal not found" entries).
    dev_schemas = _get_schemas_for_env(config, manifest, "dev")
    if dev_schemas:
        dev_principals = list(dr.access.developers)
        grant_result = apply_grants(db_client, dev_schemas, dev_principals)
        result.grants_applied += grant_result.granted
        result.grants_failed.extend(grant_result.failed)

    if "qa" in envs and dr.access.qa_users:
        qa_schemas = _get_schemas_for_env(config, manifest, "qa")
        if qa_schemas:
            qa_principals = list(dr.access.qa_users)
            qa_grant_result = apply_grants(db_client, qa_schemas, qa_principals)
            result.grants_applied += qa_grant_result.granted
            result.grants_failed.extend(qa_grant_result.failed)

    # Record access rows
    access_rows: list[dict[str, str]] = []
    for dev in dr.access.developers:
        access_rows.append({
            "dr_id": dr_id,
            "user_email": dev,
            "environment": "dev",
            "access_level": "READ_WRITE",
            "granted_at": now_iso(),
        })
    if "qa" in envs and dr.access.qa_users:
        for qa_user in dr.access.qa_users:
            access_rows.append({
                "dr_id": dr_id,
                "user_email": qa_user,
                "environment": "qa",
                "access_level": "READ_WRITE",
                "granted_at": now_iso(),
            })

    if force_replace:
        try:
            access_repo.delete_by_dr_id(db_client, dr_id=dr_id)
        except Exception:
            logger.debug("Stale access row cleanup failed, continuing")

    try:
        access_repo.bulk_insert(db_client, rows=access_rows)
    except Exception:
        logger.debug("Access row insert may have partially failed, non-fatal")

    # Collect per-phase failure details so admins can debug without trawling
    # worker logs.  Each list is capped per-row to keep audit cells bounded.
    object_failure_details: list[dict[str, str]] = [
        {
            "source_fqn": r.source_fqn,
            "target_fqn": r.target_fqn,
            "error": (r.error or "unknown error")[:1000],
        }
        for r in result.objects_failed
    ]
    grant_failure_details: list[dict[str, str]] = [
        {"statement": stmt[:500], "error": err[:1000]}
        for stmt, err in result.grants_failed
    ]
    schema_failure_details: list[dict[str, str]] = [
        {"schema_fqn": fqn, "error": (err or "unknown error")[:1000]}
        for fqn, err in result.schemas_failed.items()
    ]

    # Determine final status.  Grants and schemas count too: a DR whose
    # objects cloned fine but whose grants all failed is not usable, so we
    # surface that as PARTIAL_SUCCESS (or FAILED if literally nothing
    # worked).
    grants_attempted = result.grants_applied + len(result.grants_failed)
    all_grants_failed = grants_attempted > 0 and result.grants_applied == 0
    nothing_succeeded = (
        not result.objects_succeeded
        and not result.schemas_created
        and result.grants_applied == 0
    )
    any_failure = bool(
        result.objects_failed
        or result.grants_failed
        or result.schemas_failed
    )

    if (result.all_objects_failed and all_grants_failed) or (
        nothing_succeeded and any_failure
    ):
        final_status = DRStatus.FAILED
        audit_status = "FAILED"
    elif any_failure:
        final_status = DRStatus.ACTIVE
        audit_status = "PARTIAL_SUCCESS"
    else:
        final_status = DRStatus.ACTIVE
        audit_status = "SUCCESS"

    # Update DR status -- unconditional (no CAS gate).  See pre-provision
    # comment above for the rationale; same defect would otherwise sink
    # this update silently if the row is at a status other than
    # PROVISIONING for any reason.
    try:
        dr_repo.force_status(
            db_client,
            dr_id=dr_id,
            new_status=final_status,
            last_modified_at=now_iso(),
        )
    except Exception:
        logger.error(
            "Final force_status update failed for %s", dr_id, exc_info=True,
        )
        raise

    # Read-back verification.  The Statement Execution API doesn't return
    # affected-row counts, so the only way to detect a bad write is to
    # re-read.  Mismatch is logged at CRITICAL because the audit row that
    # was just appended now disagrees with the live row.
    try:
        verify_row = dr_repo.get(db_client, dr_id=dr_id)
        live_status = verify_row.get("status") if verify_row else None
        if live_status != final_status.value:
            logger.critical(
                "Status read-back mismatch for %s: wrote %s, row reads %s",
                dr_id, final_status.value, live_status or "<missing>",
            )
    except Exception:
        logger.warning("Status read-back query failed for %s", dr_id, exc_info=True)

    result.final_status = final_status.value

    # error_message column: JSON-encoded summary of every failure phase.
    # action_detail mirrors the same data so the DR Status page (which
    # surfaces action_detail) can render it without an extra column join.
    error_payload: dict[str, list[dict[str, str]]] = {}
    if object_failure_details:
        error_payload["failed_objects"] = object_failure_details
    if grant_failure_details:
        error_payload["failed_grants"] = grant_failure_details
    if schema_failure_details:
        error_payload["failed_schemas"] = schema_failure_details
    error_msg = json.dumps(error_payload) if error_payload else None

    audit_repo.append(
        db_client,
        dr_id=dr_id,
        action="PROVISION",
        performed_by="SYSTEM",
        performed_at=now_iso(),
        status=audit_status,
        action_detail=json.dumps({
            "phase": "complete",
            "objects_succeeded": len(result.objects_succeeded),
            "objects_failed": len(result.objects_failed),
            "schemas_created": len(result.schemas_created),
            "schemas_failed": len(result.schemas_failed),
            "grants_applied": result.grants_applied,
            "grants_failed": len(result.grants_failed),
            "failures": object_failure_details,
            "grant_failures": grant_failure_details,
            "schema_failures": schema_failure_details,
        }),
        error_message=error_msg,
    )

    return result
