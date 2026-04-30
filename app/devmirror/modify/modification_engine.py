"""Modification engine for active development requests."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import date
from typing import TYPE_CHECKING

from devmirror.control.control_table import DRStatus, ObjectStatus
from devmirror.provision.access_manager import (
    apply_grants,
    apply_revokes,
)
from devmirror.provision.object_cloner import execute_clone, provision_schemas
from devmirror.utils import now_iso, revision_values
from devmirror.utils.naming import resolve_target_catalog, target_object_fqn

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient

    from devmirror.config.schema import DataRevision
    from devmirror.control.audit import AuditRepository
    from devmirror.control.control_table import (
        DrAccessRepository,
        DrObjectRepository,
        DRRepository,
    )
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)

_ACTIVE_STATUSES = frozenset({DRStatus.ACTIVE.value, DRStatus.EXPIRING_SOON.value})


class ModificationError(Exception):
    """Raised when modification validation fails."""


@dataclass
class ActionResult:
    """Outcome of a single modification action."""

    action: str
    success: bool
    detail: str = ""
    error: str | None = None


@dataclass
class ModifyResult:
    """Overall result of a modification run."""

    dr_id: str
    actions: list[ActionResult] = field(default_factory=list)
    audit_status: str = "SUCCESS"

    @property
    def has_failures(self) -> bool:
        return any(not a.success for a in self.actions)

    @property
    def all_failed(self) -> bool:
        return bool(self.actions) and all(not a.success for a in self.actions)


def _get_schemas_for_env(
    obj_repo: DrObjectRepository,
    db_client: DbClient,
    dr_id: str,
    environment: str,
) -> list[str]:
    """Get unique target schema FQNs for a DR in a given environment."""
    all_objects = obj_repo.list_by_dr_id(db_client, dr_id=dr_id)
    return sorted({
        ".".join(o["target_fqn"].split(".")[:2])
        for o in all_objects
        if o.get("target_environment") == environment
        and o.get("status") != ObjectStatus.DROPPED.value
    })


def _add_objects(
    dr_id: str,
    objects_to_add: list[dict[str, str]],
    env: str,
    data_revision: DataRevision | None,
    db_client: DbClient,
    obj_repo: DrObjectRepository,
) -> ActionResult:
    """Add new objects to the DR scope."""
    try:
        from devmirror.provision.object_cloner import default_clone_strategy

        succeeded = 0
        failed_items: list[str] = []
        now = now_iso()

        for obj in objects_to_add:
            source_fqn = obj["fqn"]
            parts = source_fqn.split(".")
            if len(parts) != 3:
                failed_items.append(source_fqn)
                continue

            source_catalog = parts[0]
            target_catalog = resolve_target_catalog(source_catalog, env)
            t_fqn = target_object_fqn(target_catalog, source_fqn, dr_id, env)

            obj_type = obj.get("type", "table")
            access_mode = obj.get("access_mode", "READ_ONLY")
            strategy = obj.get("clone_strategy") or default_clone_strategy(
                obj_type, access_mode
            )

            # Provision the schema if needed
            schema_fqn = ".".join(t_fqn.split(".")[:2])
            provision_schemas(db_client, [schema_fqn])

            # Clone the object
            clone_result = execute_clone(
                db_client,
                source_fqn=source_fqn,
                target_fqn=t_fqn,
                strategy=strategy,
                data_revision=data_revision,
            )

            if clone_result.success:
                succeeded += 1
                rev_mode, rev_value = revision_values(data_revision)
                try:
                    obj_repo.bulk_insert(
                        db_client,
                        objects=[{
                            "dr_id": dr_id,
                            "source_fqn": source_fqn,
                            "target_fqn": t_fqn,
                            "target_environment": env,
                            "object_type": obj_type,
                            "access_mode": access_mode,
                            "clone_strategy": strategy,
                            "clone_revision_mode": rev_mode,
                            "clone_revision_value": rev_value,
                            "provisioned_at": now,
                            "last_refreshed_at": now,
                            "status": ObjectStatus.PROVISIONED.value,
                            "estimated_size_gb": obj.get("estimated_size_gb"),
                        }],
                    )
                except Exception:
                    logger.debug("Object row insert failed for %s, non-fatal", source_fqn)
            else:
                failed_items.append(source_fqn)

        detail = f"Added {succeeded} objects"
        if failed_items:
            detail += f", {len(failed_items)} failed: {failed_items}"
            return ActionResult(
                action="add_objects",
                success=succeeded > 0,
                detail=detail,
                error=f"Failed: {failed_items}" if not succeeded else None,
            )
        return ActionResult(action="add_objects", success=True, detail=detail)
    except Exception as exc:
        return ActionResult(
            action="add_objects", success=False, error=str(exc)
        )


def _remove_objects(
    dr_id: str,
    fqns_to_remove: list[str],
    db_client: DbClient,
    obj_repo: DrObjectRepository,
) -> ActionResult:
    """Remove objects from the DR scope (DROP from DEV/QA + update status)."""
    try:
        dropped = 0
        failed_items: list[str] = []

        all_objects = obj_repo.list_by_dr_id(db_client, dr_id=dr_id)
        fqn_set = set(fqns_to_remove)

        for obj_row in all_objects:
            if obj_row.get("source_fqn") not in fqn_set:
                continue
            if obj_row.get("status") == ObjectStatus.DROPPED.value:
                continue

            target_fqn = obj_row["target_fqn"]
            strategy = obj_row.get("clone_strategy", "shallow_clone")

            try:
                if strategy == "view":
                    db_client.sql_exec(f"DROP VIEW IF EXISTS {target_fqn}")
                else:
                    db_client.sql_exec(f"DROP TABLE IF EXISTS {target_fqn}")

                current_status = ObjectStatus(obj_row.get("status", "PROVISIONED"))
                obj_repo.update_object_status(
                    db_client,
                    dr_id=dr_id,
                    source_fqn=obj_row["source_fqn"],
                    target_environment=obj_row["target_environment"],
                    current_status=current_status,
                    new_status=ObjectStatus.DROPPED,
                )
                dropped += 1
            except Exception as exc:
                logger.error("Failed to drop %s: %s", target_fqn, exc)
                failed_items.append(obj_row["source_fqn"])

        detail = f"Dropped {dropped} objects"
        if failed_items:
            detail += f", {len(failed_items)} failed"
        return ActionResult(
            action="remove_objects",
            success=dropped > 0 or not failed_items,
            detail=detail,
            error=f"Failed: {failed_items}" if failed_items else None,
        )
    except Exception as exc:
        return ActionResult(
            action="remove_objects", success=False, error=str(exc)
        )


def _manage_users(
    action: str,
    dr_id: str,
    users: list[str],
    environment: str,
    db_client: DbClient,
    obj_repo: DrObjectRepository,
    access_repo: DrAccessRepository | None = None,
) -> ActionResult:
    """Grant or revoke access for users on all DR schemas."""
    try:
        schema_fqns = _get_schemas_for_env(obj_repo, db_client, dr_id, environment)

        if not schema_fqns:
            return ActionResult(
                action=action,
                success=True,
                detail=f"No schemas found for env={environment}, nothing to {'grant' if action == 'add_users' else 'revoke'}.",
            )

        if action == "add_users":
            access_result = apply_grants(db_client, schema_fqns, users)
            # Record access rows
            if access_repo is not None:
                now = now_iso()
                for user in users:
                    try:
                        access_repo.bulk_insert(
                            db_client,
                            rows=[{
                                "dr_id": dr_id,
                                "user_email": user,
                                "environment": environment,
                                "access_level": "READ_WRITE",
                                "granted_at": now,
                            }],
                        )
                    except Exception:
                        logger.debug("Access row insert failed for %s, non-fatal", user)
        else:
            access_result = apply_revokes(db_client, schema_fqns, users)

        verb = "Granted" if action == "add_users" else "Revoked"
        detail = f"{verb} {access_result.granted} statements for {len(users)} users"
        if access_result.failed:
            detail += f", {len(access_result.failed)} failed"
        return ActionResult(
            action=action,
            success=access_result.all_succeeded,
            detail=detail,
            error=str(access_result.failed) if access_result.failed else None,
        )
    except Exception as exc:
        return ActionResult(action=action, success=False, error=str(exc))


def _add_streams(
    dr_id: str,
    stream_names: list[str],
    env: str,
    data_revision: DataRevision | None,
    db_client: DbClient,
    obj_repo: DrObjectRepository,
    client: WorkspaceClient,
) -> ActionResult:
    """Scan new streams, discover objects, and provision net-new ones."""
    try:
        from devmirror.scan.dependency_classifier import classify_dependencies
        from devmirror.scan.lineage import query_lineage
        from devmirror.scan.stream_resolver import resolve_streams

        resolved, unresolved = resolve_streams(client, stream_names)
        if unresolved:
            return ActionResult(
                action="add_streams",
                success=False,
                error=f"Unresolved streams: {unresolved}",
            )

        lineage_result = query_lineage(db_client, resolved)
        classification = classify_dependencies(lineage_result.edges)

        existing_objects = obj_repo.list_by_dr_id(db_client, dr_id=dr_id)
        existing_fqns = {row["source_fqn"] for row in existing_objects}

        new_objects: list[dict[str, str]] = []
        for obj in classification.objects:
            if obj.fqn not in existing_fqns:
                new_objects.append({
                    "fqn": obj.fqn,
                    "type": obj.object_type,
                    "access_mode": obj.access_mode,
                })

        if not new_objects:
            return ActionResult(
                action="add_streams",
                success=True,
                detail="No net-new objects discovered from streams.",
            )

        add_result = _add_objects(
            dr_id, new_objects, env, data_revision, db_client, obj_repo
        )

        return ActionResult(
            action="add_streams",
            success=add_result.success,
            detail=f"Streams resolved: {len(resolved)}. {add_result.detail}",
            error=add_result.error,
        )
    except Exception as exc:
        return ActionResult(
            action="add_streams", success=False, error=str(exc)
        )


def modify_dr(
    dr_id: str,
    *,
    db_client: DbClient,
    dr_repo: DRRepository,
    obj_repo: DrObjectRepository,
    access_repo: DrAccessRepository,
    audit_repo: AuditRepository,
    add_objects: list[dict[str, str]] | None = None,
    remove_objects: list[str] | None = None,
    add_dev_users: list[str] | None = None,
    remove_dev_users: list[str] | None = None,
    add_qa_users: list[str] | None = None,
    remove_qa_users: list[str] | None = None,
    new_expiration_date: str | None = None,
    data_revision: DataRevision | None = None,
    add_streams: list[str] | None = None,
    client: WorkspaceClient | None = None,
    performed_by: str = "SYSTEM",
) -> ModifyResult:
    """Apply modifications to an active DR with partial-success semantics.

    ``performed_by`` is recorded on the audit row.  Default ``"SYSTEM"``
    is correct for lifecycle DAB jobs; the FastAPI ``modify_dr_endpoint``
    overrides with the authenticated user's email so the audit trail
    correctly attributes user-initiated changes.
    """
    result = ModifyResult(dr_id=dr_id)

    # Validate DR is active (inlined from _validate_dr_for_modify)
    dr_row = dr_repo.get(db_client, dr_id=dr_id)
    if dr_row is None:
        raise ModificationError(f"Development request {dr_id!r} not found.")
    status = dr_row.get("status", "")
    if status not in _ACTIVE_STATUSES:
        raise ModificationError(
            f"DR {dr_id!r} is not active (status={status!r}). "
            "Modification is only allowed on ACTIVE or EXPIRING_SOON DRs."
        )

    # Dispatch actions via a table-driven pattern
    _user_actions: list[tuple[list[str] | None, str, str]] = [
        (add_dev_users, "add_users", "dev"),
        (remove_dev_users, "remove_users", "dev"),
        (add_qa_users, "add_users", "qa"),
        (remove_qa_users, "remove_users", "qa"),
    ]

    if add_objects:
        ar = _add_objects(
            dr_id, add_objects, "dev", data_revision, db_client, obj_repo
        )
        result.actions.append(ar)

    if remove_objects:
        ar = _remove_objects(dr_id, remove_objects, db_client, obj_repo)
        result.actions.append(ar)

    for users, action_name, env in _user_actions:
        if users:
            ar = _manage_users(
                action_name, dr_id, users, env, db_client, obj_repo,
                access_repo if action_name == "add_users" else None,
            )
            result.actions.append(ar)

    if new_expiration_date:
        try:
            new_date = date.fromisoformat(new_expiration_date)
            if new_date < date.today():
                ar = ActionResult(action="change_expiration", success=False,
                                  error=f"New expiration date {new_expiration_date} is in the past.")
            else:
                from devmirror.utils.sql_executor import escape_sql_string as _esc
                _now = now_iso()
                sql = (f"UPDATE {dr_repo.table_fqn} SET "
                       f"expiration_date = '{_esc(new_expiration_date)}', "
                       f"last_modified_at = '{_esc(_now)}' "
                       f"WHERE dr_id = '{_esc(dr_id)}'")
                db_client.sql_exec(sql)
                ar = ActionResult(action="change_expiration", success=True,
                                  detail=f"Expiration updated to {new_expiration_date}")
        except Exception as exc:
            ar = ActionResult(action="change_expiration", success=False, error=str(exc))
        result.actions.append(ar)

    if add_streams:
        if client is None:
            result.actions.append(
                ActionResult(
                    action="add_streams",
                    success=False,
                    error="WorkspaceClient is required for add_streams but was not provided.",
                )
            )
        else:
            ar = _add_streams(
                dr_id, add_streams, "dev", data_revision, db_client, obj_repo, client
            )
            result.actions.append(ar)

    # Update DR last_modified_at
    try:
        from devmirror.utils.sql_executor import escape_sql_string as _escape

        now = now_iso()
        sql = (
            f"UPDATE {dr_repo.table_fqn} SET "
            f"last_modified_at = '{_escape(now)}' "
            f"WHERE dr_id = '{_escape(dr_id)}'"
        )
        db_client.sql_exec(sql)
    except Exception:
        logger.debug("DR last_modified_at update failed, non-fatal")

    # Determine audit status
    if result.all_failed:
        result.audit_status = "FAILED"
    elif result.has_failures:
        result.audit_status = "PARTIAL_SUCCESS"
    else:
        result.audit_status = "SUCCESS"

    # Write audit entry
    error_msg = None
    if result.has_failures:
        failed_actions = [
            {"action": a.action, "error": a.error}
            for a in result.actions
            if not a.success
        ]
        error_msg = json.dumps(failed_actions)

    audit_repo.append(
        db_client,
        dr_id=dr_id,
        action="MODIFY",
        performed_by=performed_by,
        performed_at=now_iso(),
        status=result.audit_status,
        action_detail=json.dumps({
            "actions": [
                {"action": a.action, "success": a.success, "detail": a.detail}
                for a in result.actions
            ],
        }),
        error_message=error_msg,
    )

    return result
