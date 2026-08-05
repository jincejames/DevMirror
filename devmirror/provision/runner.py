"""Provision orchestration runner."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from devmirror.control.control_table import DRStatus, ObjectStatus
from devmirror.provision.access_manager import apply_grants, apply_volume_grants
from devmirror.provision.object_cloner import (
    CloneResult,
    default_clone_strategy,
    execute_clone,
    provision_schemas,
)
from devmirror.utils import now_iso, revision_values, run_bounded
from devmirror.utils.naming import (
    import_schema_fqn,
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


def _import_schema_suffix() -> str:
    """Configured suffix for the per-DR import schema; '' means feature off."""
    import os
    return os.environ.get("DEVMIRROR_IMPORT_SCHEMA_SUFFIX", "").strip()


def _import_volume_name() -> str:
    """Configured volume name for the import schema; '' means no volume."""
    import os
    return os.environ.get("DEVMIRROR_IMPORT_VOLUME_NAME", "").strip()


def _source_catalogs_from_manifest(manifest: dict[str, Any]) -> set[str]:
    """Distinct source catalogs (parts[0] of each 3-part object FQN)."""
    cats: set[str] = set()
    for obj in manifest.get("scan_result", {}).get("objects", []):
        parts = (obj.get("fqn") or "").split(".")
        if len(parts) == 3 and parts[0]:
            cats.add(parts[0])
    return cats


def _get_import_schemas_for_env(
    manifest: dict[str, Any], dr_id: str, env: str,
) -> list[str]:
    """Return import-schema FQNs (one per source catalog), or [] if feature off.

    Each DR provision creates one of these schemas per (env, source-catalog)
    pair so the customer has a known location for sideloaded artifacts.
    Activated only when ``DEVMIRROR_IMPORT_SCHEMA_SUFFIX`` is set.
    """
    suffix = _import_schema_suffix()
    if not suffix:
        return []
    schemas: list[str] = []
    for source_cat in sorted(_source_catalogs_from_manifest(manifest)):
        target_cat = resolve_target_catalog(source_cat, env)
        schemas.append(import_schema_fqn(target_cat, dr_id, env, suffix))
    return sorted(set(schemas))


def _get_import_volume_rows_for_env(
    config: DevMirrorConfig, manifest: dict[str, Any], env: str,
) -> list[dict[str, Any]]:
    """Build ``dr_objects`` rows for the per-import-schema managed Volume.

    Tracking volumes as object rows piggybacks on the existing clone and
    cleanup machinery: ``execute_clone`` with strategy ``create_volume``
    runs the DDL; ``_collect_schemas_from_objects`` discovers the import
    schema for the cleanup-time DROP; the schema CASCADE removes the
    volume even if the row-level DROP VOLUME is skipped.

    Returns [] when ``DEVMIRROR_IMPORT_SCHEMA_SUFFIX`` or
    ``DEVMIRROR_IMPORT_VOLUME_NAME`` is unset.
    """
    suffix = _import_schema_suffix()
    volume = _import_volume_name()
    if not suffix or not volume:
        return []
    dr = config.development_request
    data_revision = dr.data_revision
    rows: list[dict[str, Any]] = []
    for source_cat in sorted(_source_catalogs_from_manifest(manifest)):
        target_cat = resolve_target_catalog(source_cat, env)
        schema_fqn = import_schema_fqn(target_cat, dr.dr_id, env, suffix)
        volume_fqn = f"{schema_fqn}.{volume}"
        rows.append({
            "dr_id": dr.dr_id,
            # source_fqn is empty -- a volume is not a clone of anything.
            # Storing the source catalog here helps audit/debugging without
            # breaking downstream FQN-shape checks (they only fire on the
            # clone path, which short-circuits for create_volume).
            "source_fqn": source_cat,
            "target_fqn": volume_fqn,
            "target_environment": env,
            "object_type": "volume",
            "access_mode": "READ_WRITE",
            "clone_strategy": "create_volume",
            "clone_revision_mode": data_revision.mode,
            "clone_revision_value": revision_values(data_revision)[1],
            "provisioned_at": None,
            "last_refreshed_at": None,
            "status": ObjectStatus.REFRESH_PENDING.value,
            "estimated_size_gb": None,
        })
    return rows


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
    original_created_by: str | None = None,
) -> ProvisionResult:
    """Execute the full provisioning flow for a development request.

    ``original_created_by`` is the email of the user who *submitted* the
    underlying config (from the ConfigRepository row).  When set, it is
    written to the DR row's ``created_by`` so the displayed owner matches
    the requester rather than the first developer in the access list.
    Falls back to ``dr.access.developers[0]`` for legacy callers / CLI
    use that doesn't have a config-row context.
    """
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

    # Insert OR upsert the DR row.  Delta tables don't enforce PK
    # uniqueness, so the prior "try INSERT, swallow on duplicate" pattern
    # silently created duplicate rows on every re-provision (the INSERT
    # never raised).  Gate the INSERT on existing_dr is None and rely on
    # force_status for the existing-row path -- both branches end with the
    # row at PROVISIONING, exactly one row per dr_id.
    if existing_dr is None:
        # Truly a new DR.  We don't catch here -- a real INSERT failure
        # (network, schema mismatch, etc.) is fatal and should surface.
        dr_repo.insert(
            db_client,
            dr_id=dr_id,
            description=dr.description,
            status=DRStatus.PROVISIONING.value,
            config_yaml=config.model_dump_json(),
            created_at=now,
            created_by=(
                original_created_by
                or (dr.access.developers[0] if dr.access.developers else "SYSTEM")
            ),
            expiration_date=dr.lifecycle.expiration_date.isoformat(),
            last_modified_at=now,
        )
    else:
        # Re-provision: row exists, normalise to PROVISIONING.  Uses
        # force_status (no CAS gate) because TaskTracker single-flights
        # this DR, so we are the authoritative writer.  CAS would silently
        # no-op if the live row's status didn't match the value cached in
        # existing_dr (e.g. a row stuck at FAILED from a prior crashed run).
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
        # Per-DR import schemas (one per source catalog) for sideloaded
        # artifacts -- merged into the same provision_schemas call so
        # they're created in lockstep with the regular clone targets.
        # Skipped when DEVMIRROR_IMPORT_SCHEMA_SUFFIX is unset.
        schemas = sorted(set(
            schemas + _get_import_schemas_for_env(manifest, dr_id, env),
        ))
        all_schemas.extend(schemas)

        obj_rows = _build_object_rows(config, manifest, env)
        # Volume rows piggyback on the object-row pipeline so cleanup,
        # status tracking, and audit logs all work uniformly.  Skipped
        # when DEVMIRROR_IMPORT_VOLUME_NAME is unset.
        obj_rows += _get_import_volume_rows_for_env(config, manifest, env)
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

    # On re-provision, drop v1 physical UC objects whose target FQN
    # isn't in v2's plan before wiping the metadata rows.  Without this,
    # any v1 target dropped from v2 (because the user removed a source
    # table, switched catalogs, etc.) becomes a permanent orphan in UC
    # -- the next cleanup_dr has no dr_objects row to find it.
    #
    # Targets that ARE in v2 are skipped here because the clone DDL
    # uses CREATE OR REPLACE and will overwrite them in the clone pass.
    if force_replace:
        try:
            old_rows = obj_repo.list_by_dr_id(db_client, dr_id=dr_id)
        except Exception:
            logger.debug("Failed to read v1 object rows for orphan drop", exc_info=True)
            old_rows = []

        # Import schemas hold customer-sideloaded Volume data.  They must
        # NEVER be auto-dropped on re-provision -- if the import feature was
        # turned off or its suffix changed in v2, the v1 import schema would
        # otherwise look like an orphan and get CASCADE-dropped, silently
        # destroying uploaded data.  Identify them from v1's own rows:
        # volumes are only ever created inside import schemas, so any schema
        # containing a v1 volume row is an import schema.  This is robust even
        # when DEVMIRROR_IMPORT_SCHEMA_SUFFIX is unset at re-provision time.
        # These schemas (and their volumes) are still removed at DR expiry by
        # the cleanup engine, which reads all object rows including volumes.
        import_schemas_from_v1: set[str] = set()
        for row in old_rows:
            if row.get("object_type") == "volume":
                parts = row.get("target_fqn", "").split(".")
                if len(parts) >= 2:
                    import_schemas_from_v1.add(f"{parts[0]}.{parts[1]}")

        new_target_fqns = {row["target_fqn"] for row in all_object_rows}
        old_schemas: set[str] = set()
        for row in old_rows:
            old_target = row.get("target_fqn", "")
            if not old_target:
                continue
            parts = old_target.split(".")
            if len(parts) >= 2:
                old_schemas.add(f"{parts[0]}.{parts[1]}")
            if old_target in new_target_fqns:
                # v2 re-creates this target -- CREATE OR REPLACE handles it.
                continue
            # Never drop objects living in an import schema (see above).
            if len(parts) >= 2 and f"{parts[0]}.{parts[1]}" in import_schemas_from_v1:
                continue
            try:
                if row.get("object_type") == "volume":
                    db_client.sql_exec(f"DROP VOLUME IF EXISTS {old_target}")
                else:
                    db_client.delete_table(old_target)
                logger.info(
                    "Dropped v1 orphan %s during re-provision of %s",
                    old_target, dr_id,
                )
            except Exception as exc:
                # Non-fatal -- log and continue.  Cleanup will retry at
                # DR expiration, but a manual drop may be needed if the
                # orphan persists past that point.
                logger.warning(
                    "Failed to drop v1 orphan %s during re-provision of %s: %s",
                    old_target, dr_id, exc,
                )

        # Drop v1 schemas that v2 no longer references.  v2-recreated
        # schemas were already CREATE SCHEMA IF NOT EXISTS'd above and
        # need to stay.  Import schemas are preserved (see above).
        new_schemas_set = set(all_schemas)
        for old_schema in sorted(old_schemas - new_schemas_set):
            if old_schema in import_schemas_from_v1:
                logger.info(
                    "Preserving v1 import schema %s on re-provision of %s "
                    "(holds sideloaded data; cleanup engine drops it at expiry)",
                    old_schema, dr_id,
                )
                continue
            parts = old_schema.split(".")
            if len(parts) != 2:
                continue
            try:
                db_client.delete_schema(parts[0], parts[1])
                logger.info(
                    "Dropped v1 orphan schema %s during re-provision of %s",
                    old_schema, dr_id,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to drop v1 orphan schema %s during re-provision of %s: %s",
                    old_schema, dr_id, exc,
                )

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

    # Grant matrix:
    #   - Developers   -> RW on EVERY provisioned env (dev, and qa when
    #     enabled).  They need to fix anything UAT reviewers flag.
    #   - UAT users    -> RO on EVERY provisioned env.  Replaces the old
    #     qa-only-readers contract; UAT now means "additional read-only
    #     audience" regardless of which envs were requested.
    #   - Dedup: if a principal appears in both lists, RW wins (developer
    #     pass runs first; we filter the UAT-pass principals to those not
    #     already in developers).
    # Skip a pass entirely when no schemas were created for an env --
    # apply_grants would still validate principals and produce spurious
    # "principal not found" entries.
    dev_emails = {d.lower() for d in (dr.access.developers or [])}
    uat_only_users = [
        u for u in (dr.access.uat_users or [])
        if u.lower() not in dev_emails
    ]

    # Build the (env, schemas) pairs once so schema and volume grants share
    # the same env iteration order.  Include the per-DR import schemas (same
    # merge as `all_schemas` above) so USE_SCHEMA/SELECT(/MODIFY) is granted
    # on them too -- otherwise the volume grants below land but principals
    # can't traverse into the import schema to use the volume.  When the
    # import feature is off, _get_import_schemas_for_env returns [] so this
    # is a no-op.
    def _env_schema_list(env: str) -> list[str]:
        return sorted(set(
            _get_schemas_for_env(config, manifest, env)
            + _get_import_schemas_for_env(manifest, dr_id, env)
        ))

    env_schemas: list[tuple[str, list[str]]] = [("dev", _env_schema_list("dev"))]
    if "qa" in envs:
        env_schemas.append(("qa", _env_schema_list("qa")))

    for env_name, schemas in env_schemas:
        if not schemas:
            continue
        if dr.access.developers:
            grant_result = apply_grants(
                db_client, schemas, list(dr.access.developers), writable=True,
            )
            result.grants_applied += grant_result.granted
            result.grants_failed.extend(grant_result.failed)
        if uat_only_users:
            uat_grant_result = apply_grants(
                db_client, schemas, uat_only_users, writable=False,
            )
            result.grants_applied += uat_grant_result.granted
            result.grants_failed.extend(uat_grant_result.failed)

    # Per-volume grants for the per-DR import-schema Volumes.  Schema-level
    # USE_SCHEMA on the import schemas was granted by the schema grant loop
    # above (they're now included in env_schemas); this layers the
    # volume-securable grants (READ_VOLUME/WRITE_VOLUME) on top with the same
    # RW/RO matrix.
    for env_name, _ in env_schemas:
        env_volumes = [
            row["target_fqn"] for row in all_object_rows
            if row.get("object_type") == "volume"
            and row.get("target_environment") == env_name
        ]
        if not env_volumes:
            continue
        if dr.access.developers:
            vol_grant_result = apply_volume_grants(
                db_client, env_volumes, list(dr.access.developers), writable=True,
            )
            result.grants_applied += vol_grant_result.granted
            result.grants_failed.extend(vol_grant_result.failed)
        if uat_only_users:
            uat_vol_result = apply_volume_grants(
                db_client, env_volumes, uat_only_users, writable=False,
            )
            result.grants_applied += uat_vol_result.granted
            result.grants_failed.extend(uat_vol_result.failed)

    # Record access rows -- one row per (principal, environment).
    # Developers: READ_WRITE on every provisioned env.
    # UAT-only users: READ_ONLY on every provisioned env.
    access_rows: list[dict[str, str]] = []
    granted_envs = [env for env, schemas in env_schemas if schemas]
    granted_at = now_iso()
    for env_name in granted_envs:
        for dev in dr.access.developers:
            access_rows.append({
                "dr_id": dr_id,
                "user_email": dev,
                "environment": env_name,
                "access_level": "READ_WRITE",
                "granted_at": granted_at,
            })
        for uat_user in uat_only_users:
            access_rows.append({
                "dr_id": dr_id,
                "user_email": uat_user,
                "environment": env_name,
                "access_level": "READ_ONLY",
                "granted_at": granted_at,
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
