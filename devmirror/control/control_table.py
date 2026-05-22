"""Repository layer for DevMirror control tables and DDL bootstrap."""

from __future__ import annotations

from enum import StrEnum
from importlib import resources as importlib_resources
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from devmirror.settings import Settings
    from devmirror.utils.db_client import DbClient as _DbClient


def _param_or_null(params: dict[str, str | None], key: str, value: Any) -> str:
    """Bind value to params[key] and return ``:key``; or ``NULL`` if value is None.

    Returned value is the SQL fragment to embed in the statement.
    """
    if value is None:
        return "NULL"
    params[key] = str(value)
    return f":{key}"


def _load_ddl_template() -> str:
    """Load and concatenate every `.sql` migration in the migrations package
    so that ``apply_control_ddl()`` provisions the full control plane on
    first request.  Migrations are concatenated in lexicographic filename
    order (matching ``scripts/bootstrap_env.sh``'s ``sorted()`` traversal),
    which keeps 001 -> 002 -> 003 ordering and is what the bundled SQL
    files were written to assume.
    """
    mig_dir = importlib_resources.files("devmirror.migrations")
    parts: list[str] = []
    for entry in sorted(mig_dir.iterdir(), key=lambda r: r.name):
        if entry.name.endswith(".sql"):
            parts.append(entry.read_text(encoding="utf-8"))
    return "\n".join(parts)


def render_ddl(control_catalog: str, control_schema: str) -> list[str]:
    """Render the DDL templates (every migration in
    ``devmirror.migrations``) into individual SQL statements."""
    raw = _load_ddl_template()
    rendered = raw.replace("{control_catalog}", control_catalog).replace(
        "{control_schema}", control_schema
    )

    statements: list[str] = []
    current: list[str] = []

    for line in rendered.splitlines():
        stripped = line.strip()
        if not current and (stripped.startswith("--") or not stripped):
            continue
        if stripped.startswith("--"):
            continue
        current.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []

    if current:
        stmt = "\n".join(current).strip()
        if stmt:
            statements.append(stmt)

    return statements


def apply_control_ddl(db_client: _DbClient, settings: Settings) -> list[str]:
    """Apply all control table DDL statements idempotently, best-effort.

    Each statement is wrapped so a single failure (e.g. ``ALTER TABLE ADD
    COLUMNS`` re-run after columns already exist) does NOT abort the rest
    of the bootstrap.  Failures are logged as warnings; the returned list
    contains only the statements that actually succeeded.
    """
    import logging as _logging
    log = _logging.getLogger(__name__)
    statements = render_ddl(settings.control_catalog, settings.control_schema)
    applied: list[str] = []
    for stmt in statements:
        try:
            db_client.sql_exec(stmt)
            applied.append(stmt)
        except Exception as exc:
            preview = " ".join(stmt.split())[:120]
            log.warning(
                "Control DDL statement skipped (continuing): %s -- %s",
                preview, str(exc)[:200],
            )
    return applied


class DRStatus(StrEnum):
    """Lifecycle statuses for a Development Request."""

    PENDING_REVIEW = "PENDING_REVIEW"
    PROVISIONING = "PROVISIONING"
    ACTIVE = "ACTIVE"
    EXPIRING_SOON = "EXPIRING_SOON"
    EXPIRED = "EXPIRED"
    CLEANUP_IN_PROGRESS = "CLEANUP_IN_PROGRESS"
    CLEANED_UP = "CLEANED_UP"
    FAILED = "FAILED"
    # Admin-rejected before provisioning ever started.  Terminal -- no further
    # transitions allowed.  Rejection rationale is stored alongside the row
    # in `rejection_comment`/`rejected_by`/`rejected_at` so the owner can
    # see why their request was turned down.
    REJECTED = "REJECTED"


class ObjectStatus(StrEnum):
    """Lifecycle statuses for a DR object row."""

    PROVISIONED = "PROVISIONED"
    REFRESH_PENDING = "REFRESH_PENDING"
    FAILED = "FAILED"
    DROPPED = "DROPPED"


class StatusTransitionError(Exception):
    """Raised when a status transition is not allowed."""


_DR_TRANSITIONS: dict[DRStatus, frozenset[DRStatus]] = {
    DRStatus.PENDING_REVIEW: frozenset(
        {DRStatus.PROVISIONING, DRStatus.FAILED, DRStatus.REJECTED}
    ),
    DRStatus.PROVISIONING: frozenset({DRStatus.ACTIVE, DRStatus.FAILED}),
    DRStatus.ACTIVE: frozenset(
        {
            DRStatus.EXPIRING_SOON,
            DRStatus.EXPIRED,
            DRStatus.CLEANUP_IN_PROGRESS,
            DRStatus.FAILED,
        }
    ),
    DRStatus.EXPIRING_SOON: frozenset(
        {DRStatus.EXPIRED, DRStatus.CLEANUP_IN_PROGRESS, DRStatus.ACTIVE}
    ),
    DRStatus.EXPIRED: frozenset({DRStatus.CLEANUP_IN_PROGRESS}),
    DRStatus.CLEANUP_IN_PROGRESS: frozenset(
        {DRStatus.CLEANED_UP, DRStatus.CLEANUP_IN_PROGRESS}
    ),
    DRStatus.CLEANED_UP: frozenset(),
    # FAILED is recoverable: re-provision and refresh can move it back
    # into the active lifecycle.  Cleanup can also still drop a failed DR.
    DRStatus.FAILED: frozenset(
        {DRStatus.PROVISIONING, DRStatus.ACTIVE, DRStatus.CLEANUP_IN_PROGRESS}
    ),
    # REJECTED is terminal -- no infrastructure was ever provisioned, so
    # there's nothing to clean up.  Owner who wants to retry creates a
    # fresh DR.
    DRStatus.REJECTED: frozenset(),
}

_OBJECT_TRANSITIONS: dict[ObjectStatus, frozenset[ObjectStatus]] = {
    ObjectStatus.PROVISIONED: frozenset(
        {ObjectStatus.REFRESH_PENDING, ObjectStatus.FAILED, ObjectStatus.DROPPED}
    ),
    ObjectStatus.REFRESH_PENDING: frozenset(
        {ObjectStatus.PROVISIONED, ObjectStatus.FAILED}
    ),
    ObjectStatus.FAILED: frozenset(
        {ObjectStatus.PROVISIONED, ObjectStatus.REFRESH_PENDING, ObjectStatus.DROPPED}
    ),
    ObjectStatus.DROPPED: frozenset(),
}


def validate_dr_status_transition(current: DRStatus, target: DRStatus) -> None:
    """Raise ``StatusTransitionError`` if the transition is not allowed."""
    allowed = _DR_TRANSITIONS.get(current, frozenset())
    if target not in allowed:
        raise StatusTransitionError(
            f"Cannot transition DR status from {current.value} to {target.value}. "
            f"Allowed targets: {sorted(s.value for s in allowed) or 'none (terminal)'}."
        )


def validate_object_status_transition(current: ObjectStatus, target: ObjectStatus) -> None:
    """Raise ``StatusTransitionError`` if the object status transition is not allowed."""
    allowed = _OBJECT_TRANSITIONS.get(current, frozenset())
    if target not in allowed:
        raise StatusTransitionError(
            f"Cannot transition object status from {current.value} to {target.value}. "
            f"Allowed targets: {sorted(s.value for s in allowed) or 'none (terminal)'}."
        )


class DRRepository:
    """CRUD operations for ``fastsetup_development_requests``."""

    def __init__(self, fqn_prefix: str) -> None:
        self._table = f"{fqn_prefix}.fastsetup_development_requests"

    @property
    def table_fqn(self) -> str:
        return self._table

    def insert(
        self,
        db_client: Any,
        *,
        dr_id: str,
        description: str | None,
        status: str,
        config_yaml: str | None,
        created_at: str,
        created_by: str,
        expiration_date: str,
        last_modified_at: str | None = None,
    ) -> str:
        """Insert a new DR row.  Returns the generated SQL."""
        params: dict[str, str | None] = {}
        desc_expr = _param_or_null(params, "description", description)
        config_expr = _param_or_null(params, "config_yaml", config_yaml)
        last_mod_expr = _param_or_null(params, "last_modified_at", last_modified_at)
        params.update({
            "dr_id": dr_id,
            "status": status,
            "created_at": created_at,
            "created_by": created_by,
            "expiration_date": expiration_date,
        })
        sql = (
            f"INSERT INTO {self._table} "
            "(dr_id, description, status, config_yaml, created_at, created_by, "
            "expiration_date, last_refreshed_at, last_modified_at, notification_sent_at) "
            "VALUES ("
            f":dr_id, {desc_expr}, :status, "
            f"{config_expr}, :created_at, :created_by, "
            f":expiration_date, NULL, {last_mod_expr}, NULL)"
        )
        db_client.sql_exec_with_params(sql, params)
        return sql

    def update_status(
        self,
        db_client: Any,
        *,
        dr_id: str,
        current_status: DRStatus,
        new_status: DRStatus,
        last_modified_at: str,
    ) -> str:
        """Update DR status with transition validation.  Returns the SQL."""
        validate_dr_status_transition(current_status, new_status)
        sql = (
            f"UPDATE {self._table} SET "
            "status = :new_status, "
            "last_modified_at = :last_modified_at "
            "WHERE dr_id = :dr_id "
            "AND status = :current_status"
        )
        params: dict[str, str | None] = {
            "dr_id": dr_id,
            "new_status": new_status.value,
            "current_status": current_status.value,
            "last_modified_at": last_modified_at,
        }
        db_client.sql_exec_with_params(sql, params)
        return sql

    def force_status(
        self,
        db_client: Any,
        *,
        dr_id: str,
        new_status: DRStatus,
        last_modified_at: str,
    ) -> str:
        """Set DR status unconditionally, skipping the CAS gate that
        update_status uses.  Use only from inside provision_dr / cleanup_dr
        where TaskTracker single-flighting makes the runner the
        authoritative writer for the row.  The CAS in update_status would
        otherwise silently no-op (the Statement Execution API doesn't
        return affected-row counts) and leave the row at a stale status.
        """
        sql = (
            f"UPDATE {self._table} SET "
            "status = :new_status, "
            "last_modified_at = :last_modified_at "
            "WHERE dr_id = :dr_id"
        )
        params: dict[str, str | None] = {
            "dr_id": dr_id,
            "new_status": new_status.value,
            "last_modified_at": last_modified_at,
        }
        db_client.sql_exec_with_params(sql, params)
        return sql

    def get(self, db_client: Any, *, dr_id: str) -> dict[str, Any] | None:
        """Fetch a single DR row by id, or ``None`` if not found."""
        sql = f"SELECT * FROM {self._table} WHERE dr_id = :dr_id"
        rows = db_client.sql_with_params(sql, {"dr_id": dr_id})
        return rows[0] if rows else None

    def list_active(self, db_client: Any) -> list[dict[str, Any]]:
        """Return all DRs in active-ish states (for collision checks)."""
        params: dict[str, str | None] = {
            "s_pending": DRStatus.PENDING_REVIEW.value,
            "s_provisioning": DRStatus.PROVISIONING.value,
            "s_active": DRStatus.ACTIVE.value,
            "s_expiring": DRStatus.EXPIRING_SOON.value,
        }
        sql = (
            f"SELECT * FROM {self._table} "
            "WHERE status IN (:s_pending, :s_provisioning, :s_active, :s_expiring)"
        )
        return db_client.sql_with_params(sql, params)

    def update_notification_sent(
        self,
        db_client: Any,
        *,
        dr_id: str,
        notification_sent_at: str,
    ) -> str:
        """Record that the expiry notification was sent."""
        sql = (
            f"UPDATE {self._table} SET "
            "notification_sent_at = :notification_sent_at "
            "WHERE dr_id = :dr_id"
        )
        params: dict[str, str | None] = {
            "dr_id": dr_id,
            "notification_sent_at": notification_sent_at,
        }
        db_client.sql_exec_with_params(sql, params)
        return sql

    def reject(
        self,
        db_client: Any,
        *,
        dr_id: str,
        current_status: DRStatus,
        comment: str,
        rejected_by: str,
        rejected_at: str,
    ) -> str:
        """Mark a pending DR as REJECTED with the admin's comment.

        Validates the transition (only ``PENDING_REVIEW`` is allowed today),
        then writes status + the three rejection metadata columns in a
        single UPDATE.  The owner sees ``rejection_comment`` on the DR
        status page so they know why their request was turned down.
        """
        validate_dr_status_transition(current_status, DRStatus.REJECTED)
        sql = (
            f"UPDATE {self._table} SET "
            "status = :new_status, "
            "rejection_comment = :rejection_comment, "
            "rejected_by = :rejected_by, "
            "rejected_at = :rejected_at, "
            "last_modified_at = :last_modified_at "
            "WHERE dr_id = :dr_id "
            "AND status = :current_status"
        )
        params: dict[str, str | None] = {
            "dr_id": dr_id,
            "new_status": DRStatus.REJECTED.value,
            "current_status": current_status.value,
            "rejection_comment": comment,
            "rejected_by": rejected_by,
            "rejected_at": rejected_at,
            "last_modified_at": rejected_at,
        }
        db_client.sql_exec_with_params(sql, params)
        return sql


class DrObjectRepository:
    """CRUD operations for ``fastsetup_dr_objects``."""

    def __init__(self, fqn_prefix: str) -> None:
        self._table = f"{fqn_prefix}.fastsetup_dr_objects"

    @property
    def table_fqn(self) -> str:
        return self._table

    def bulk_insert(
        self,
        db_client: Any,
        *,
        objects: list[dict[str, Any]],
    ) -> list[str]:
        """Insert multiple object rows. Returns the list of SQL executed."""
        statements: list[str] = []
        for obj in objects:
            params: dict[str, str | None] = {}
            clone_rev_expr = _param_or_null(
                params, "clone_revision_value", obj.get("clone_revision_value")
            )
            provisioned_expr = _param_or_null(
                params, "provisioned_at", obj.get("provisioned_at")
            )
            last_refreshed_expr = _param_or_null(
                params, "last_refreshed_at", obj.get("last_refreshed_at")
            )
            est_gb = obj.get("estimated_size_gb")
            # estimated_size_gb is numeric; safe to interpolate (None -> NULL else float)
            est_gb_sql = "NULL" if est_gb is None else str(float(est_gb))
            params.update({
                "dr_id": str(obj["dr_id"]),
                "source_fqn": str(obj["source_fqn"]),
                "target_fqn": str(obj["target_fqn"]),
                "target_environment": str(obj["target_environment"]),
                "object_type": str(obj["object_type"]),
                "access_mode": str(obj["access_mode"]),
                "clone_strategy": str(obj["clone_strategy"]),
                "clone_revision_mode": str(obj["clone_revision_mode"]),
                "status": str(obj["status"]),
            })
            sql = (
                f"INSERT INTO {self._table} "
                "(dr_id, source_fqn, target_fqn, target_environment, object_type, "
                "access_mode, clone_strategy, clone_revision_mode, clone_revision_value, "
                "provisioned_at, last_refreshed_at, status, estimated_size_gb) "
                "VALUES ("
                ":dr_id, :source_fqn, :target_fqn, :target_environment, :object_type, "
                ":access_mode, :clone_strategy, :clone_revision_mode, "
                f"{clone_rev_expr}, "
                f"{provisioned_expr}, "
                f"{last_refreshed_expr}, "
                f":status, {est_gb_sql})"
            )
            db_client.sql_exec_with_params(sql, params)
            statements.append(sql)
        return statements

    def update_object_status(
        self,
        db_client: Any,
        *,
        dr_id: str,
        source_fqn: str,
        target_environment: str,
        current_status: ObjectStatus,
        new_status: ObjectStatus,
        last_refreshed_at: str | None = None,
    ) -> str:
        """Update a single object row's status with transition validation."""
        validate_object_status_transition(current_status, new_status)
        params: dict[str, str | None] = {
            "dr_id": dr_id,
            "source_fqn": source_fqn,
            "target_environment": target_environment,
            "current_status": current_status.value,
            "new_status": new_status.value,
        }
        set_parts = ["status = :new_status"]
        if last_refreshed_at:
            set_parts.append("last_refreshed_at = :last_refreshed_at")
            params["last_refreshed_at"] = last_refreshed_at
        set_clause = ", ".join(set_parts)
        sql = (
            f"UPDATE {self._table} SET {set_clause} "
            "WHERE dr_id = :dr_id "
            "AND source_fqn = :source_fqn "
            "AND target_environment = :target_environment "
            "AND status = :current_status"
        )
        db_client.sql_exec_with_params(sql, params)
        return sql

    def list_by_dr_id(
        self, db_client: Any, *, dr_id: str
    ) -> list[dict[str, Any]]:
        """Return all object rows for a given DR."""
        sql = f"SELECT * FROM {self._table} WHERE dr_id = :dr_id"
        return db_client.sql_with_params(sql, {"dr_id": dr_id})

    def counts_by_dr_id(
        self, db_client: Any, *, dr_ids: list[str],
    ) -> dict[str, int]:
        """Return ``{dr_id: object_count}`` for the given DR ids.

        Implementation: a single unfiltered ``GROUP BY dr_id`` over the
        whole ``fastsetup_dr_objects`` table, then filter in Python.
        Avoids the IN-clause-with-named-params shape, which was prone
        to upstream errors against the LH workspace's Statement
        Execution API.  Cost is negligible -- the object table
        typically has at most low-thousands of rows, and the engine
        only runs the GROUP BY once per ``/api/drs`` call.

        DR ids passed in but absent from the result map back to 0 via
        the caller's ``.get(dr_id, 0)``.
        """
        if not dr_ids:
            return {}
        sql = f"SELECT dr_id, COUNT(*) AS n FROM {self._table} GROUP BY dr_id"
        rows = db_client.sql(sql)
        wanted = set(dr_ids)
        out: dict[str, int] = {}
        for r in rows:
            dr_id = r.get("dr_id")
            if not dr_id or dr_id not in wanted:
                continue
            n = r.get("n")
            try:
                out[dr_id] = int(n) if n is not None else 0
            except (ValueError, TypeError):
                continue
        return out

    def delete_by_dr_id(self, db_client: Any, *, dr_id: str) -> str:
        """Delete all object rows for a DR (used before re-provisioning)."""
        sql = f"DELETE FROM {self._table} WHERE dr_id = :dr_id"
        db_client.sql_exec_with_params(sql, {"dr_id": dr_id})
        return sql


class DrAccessRepository:
    """CRUD operations for ``fastsetup_dr_access``."""

    def __init__(self, fqn_prefix: str) -> None:
        self._table = f"{fqn_prefix}.fastsetup_dr_access"

    @property
    def table_fqn(self) -> str:
        return self._table

    def bulk_insert(
        self,
        db_client: Any,
        *,
        rows: list[dict[str, str]],
    ) -> list[str]:
        """Insert multiple access grant rows."""
        statements: list[str] = []
        for row in rows:
            params: dict[str, str | None] = {
                "dr_id": row["dr_id"],
                "user_email": row["user_email"],
                "environment": row["environment"],
                "access_level": row["access_level"],
                "granted_at": row["granted_at"],
            }
            sql = (
                f"INSERT INTO {self._table} "
                "(dr_id, user_email, environment, access_level, granted_at) "
                "VALUES ("
                ":dr_id, :user_email, :environment, :access_level, :granted_at)"
            )
            db_client.sql_exec_with_params(sql, params)
            statements.append(sql)
        return statements

    def list_by_dr_id(
        self, db_client: Any, *, dr_id: str
    ) -> list[dict[str, Any]]:
        """Return all access rows for a given DR."""
        sql = f"SELECT * FROM {self._table} WHERE dr_id = :dr_id"
        return db_client.sql_with_params(sql, {"dr_id": dr_id})

    def delete_by_dr_id(self, db_client: Any, *, dr_id: str) -> str:
        """Delete all access rows for a DR (used before re-insert on modification)."""
        sql = f"DELETE FROM {self._table} WHERE dr_id = :dr_id"
        db_client.sql_exec_with_params(sql, {"dr_id": dr_id})
        return sql
