"""Repository layer for DevMirror control tables and DDL bootstrap."""

from __future__ import annotations

from enum import StrEnum
from importlib import resources as importlib_resources
from typing import TYPE_CHECKING, Any

from devmirror.utils.sql_executor import escape_sql_string as _escape

if TYPE_CHECKING:
    from devmirror.settings import Settings
    from devmirror.utils.db_client import DbClient as _DbClient


def _sql_val(v: Any) -> str:
    """Format a value for SQL: NULL if None, else escaped string literal."""
    return "NULL" if v is None else f"'{_escape(str(v))}'"


def _load_ddl_template() -> str:
    """Load the raw DDL SQL template from the migrations package."""
    ref = importlib_resources.files("devmirror.migrations").joinpath("001_control_tables.sql")
    return ref.read_text(encoding="utf-8")


def render_ddl(control_catalog: str, control_schema: str) -> list[str]:
    """Render the DDL template into individual SQL statements."""
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
    """Apply all control table DDL statements idempotently."""
    statements = render_ddl(settings.control_catalog, settings.control_schema)
    for stmt in statements:
        db_client.sql_exec(stmt)
    return statements


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


class ObjectStatus(StrEnum):
    """Lifecycle statuses for a DR object row."""

    PROVISIONED = "PROVISIONED"
    REFRESH_PENDING = "REFRESH_PENDING"
    FAILED = "FAILED"
    DROPPED = "DROPPED"


class StatusTransitionError(Exception):
    """Raised when a status transition is not allowed."""


_DR_TRANSITIONS: dict[DRStatus, frozenset[DRStatus]] = {
    DRStatus.PENDING_REVIEW: frozenset({DRStatus.PROVISIONING, DRStatus.FAILED}),
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
    DRStatus.FAILED: frozenset(),
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
    """CRUD operations for ``devmirror_development_requests``."""

    def __init__(self, fqn_prefix: str) -> None:
        self._table = f"{fqn_prefix}.devmirror_development_requests"

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
        sql = (
            f"INSERT INTO {self._table} "
            f"(dr_id, description, status, config_yaml, created_at, created_by, "
            f"expiration_date, last_refreshed_at, last_modified_at, notification_sent_at) "
            f"VALUES ("
            f"'{_escape(dr_id)}', {_sql_val(description)}, '{_escape(status)}', "
            f"{_sql_val(config_yaml)}, '{_escape(created_at)}', '{_escape(created_by)}', "
            f"'{_escape(expiration_date)}', NULL, {_sql_val(last_modified_at)}, NULL)"
        )
        db_client.sql_exec(sql)
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
            f"status = '{new_status.value}', "
            f"last_modified_at = '{_escape(last_modified_at)}' "
            f"WHERE dr_id = '{_escape(dr_id)}' "
            f"AND status = '{current_status.value}'"
        )
        db_client.sql_exec(sql)
        return sql

    def get(self, db_client: Any, *, dr_id: str) -> dict[str, Any] | None:
        """Fetch a single DR row by id, or ``None`` if not found."""
        sql = f"SELECT * FROM {self._table} WHERE dr_id = '{_escape(dr_id)}'"
        rows = db_client.sql(sql)
        return rows[0] if rows else None

    def list_active(self, db_client: Any) -> list[dict[str, Any]]:
        """Return all DRs in active-ish states (for collision checks)."""
        active_states = (
            DRStatus.PENDING_REVIEW.value,
            DRStatus.PROVISIONING.value,
            DRStatus.ACTIVE.value,
            DRStatus.EXPIRING_SOON.value,
        )
        in_clause = ", ".join(f"'{s}'" for s in active_states)
        sql = f"SELECT * FROM {self._table} WHERE status IN ({in_clause})"
        return db_client.sql(sql)

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
            f"notification_sent_at = '{_escape(notification_sent_at)}' "
            f"WHERE dr_id = '{_escape(dr_id)}'"
        )
        db_client.sql_exec(sql)
        return sql


class DrObjectRepository:
    """CRUD operations for ``devmirror_dr_objects``."""

    def __init__(self, fqn_prefix: str) -> None:
        self._table = f"{fqn_prefix}.devmirror_dr_objects"

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
            est_gb = obj.get("estimated_size_gb")
            est_gb_sql = str(est_gb) if est_gb is not None else "NULL"
            sql = (
                f"INSERT INTO {self._table} "
                f"(dr_id, source_fqn, target_fqn, target_environment, object_type, "
                f"access_mode, clone_strategy, clone_revision_mode, clone_revision_value, "
                f"provisioned_at, last_refreshed_at, status, estimated_size_gb) "
                f"VALUES ("
                f"'{_escape(obj['dr_id'])}', '{_escape(obj['source_fqn'])}', "
                f"'{_escape(obj['target_fqn'])}', '{_escape(obj['target_environment'])}', "
                f"'{_escape(obj['object_type'])}', '{_escape(obj['access_mode'])}', "
                f"'{_escape(obj['clone_strategy'])}', '{_escape(obj['clone_revision_mode'])}', "
                f"{_sql_val(obj.get('clone_revision_value'))}, "
                f"{_sql_val(obj.get('provisioned_at'))}, "
                f"{_sql_val(obj.get('last_refreshed_at'))}, "
                f"'{_escape(obj['status'])}', {est_gb_sql})"
            )
            db_client.sql_exec(sql)
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
        set_parts = [f"status = '{new_status.value}'"]
        if last_refreshed_at:
            set_parts.append(f"last_refreshed_at = '{_escape(last_refreshed_at)}'")
        set_clause = ", ".join(set_parts)
        sql = (
            f"UPDATE {self._table} SET {set_clause} "
            f"WHERE dr_id = '{_escape(dr_id)}' "
            f"AND source_fqn = '{_escape(source_fqn)}' "
            f"AND target_environment = '{_escape(target_environment)}' "
            f"AND status = '{current_status.value}'"
        )
        db_client.sql_exec(sql)
        return sql

    def list_by_dr_id(
        self, db_client: Any, *, dr_id: str
    ) -> list[dict[str, Any]]:
        """Return all object rows for a given DR."""
        sql = f"SELECT * FROM {self._table} WHERE dr_id = '{_escape(dr_id)}'"
        return db_client.sql(sql)

    def delete_by_dr_id(self, db_client: Any, *, dr_id: str) -> str:
        """Delete all object rows for a DR (used before re-provisioning)."""
        sql = f"DELETE FROM {self._table} WHERE dr_id = '{_escape(dr_id)}'"
        db_client.sql_exec(sql)
        return sql


class DrAccessRepository:
    """CRUD operations for ``devmirror_dr_access``."""

    def __init__(self, fqn_prefix: str) -> None:
        self._table = f"{fqn_prefix}.devmirror_dr_access"

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
            sql = (
                f"INSERT INTO {self._table} "
                f"(dr_id, user_email, environment, access_level, granted_at) "
                f"VALUES ("
                f"'{_escape(row['dr_id'])}', "
                f"'{_escape(row['user_email'])}', "
                f"'{_escape(row['environment'])}', "
                f"'{_escape(row['access_level'])}', "
                f"'{_escape(row['granted_at'])}')"
            )
            db_client.sql_exec(sql)
            statements.append(sql)
        return statements

    def list_by_dr_id(
        self, db_client: Any, *, dr_id: str
    ) -> list[dict[str, Any]]:
        """Return all access rows for a given DR."""
        sql = f"SELECT * FROM {self._table} WHERE dr_id = '{_escape(dr_id)}'"
        return db_client.sql(sql)

    def delete_by_dr_id(self, db_client: Any, *, dr_id: str) -> str:
        """Delete all access rows for a DR (used before re-insert on modification)."""
        sql = f"DELETE FROM {self._table} WHERE dr_id = '{_escape(dr_id)}'"
        db_client.sql_exec(sql)
        return sql
