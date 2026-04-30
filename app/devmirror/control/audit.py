"""Append-only audit log repository."""

from __future__ import annotations

import logging
import uuid
from typing import Any

logger = logging.getLogger(__name__)


class AuditRepository:
    """Append and query operations for the ``audit_log`` table."""

    def __init__(self, fqn_prefix: str) -> None:
        self._table = f"{fqn_prefix}.audit_log"

    @property
    def table_fqn(self) -> str:
        return self._table

    def append(
        self,
        db_client: Any,
        *,
        dr_id: str,
        action: str,
        performed_by: str,
        performed_at: str,
        status: str,
        log_id: str | None = None,
        action_detail: str | None = None,
        error_message: str | None = None,
    ) -> str:
        """Append a single audit entry. Returns the executed SQL."""
        log_id = log_id or str(uuid.uuid4())
        params: dict[str, str | None] = {
            "log_id": log_id,
            "dr_id": dr_id,
            "action": action,
            "action_detail": action_detail,
            "performed_by": performed_by,
            "performed_at": performed_at,
            "status": status,
            "error_message": error_message,
        }
        sql = (
            f"INSERT INTO {self._table} "
            "(log_id, dr_id, action, action_detail, performed_by, "
            "performed_at, status, error_message) "
            "VALUES ("
            ":log_id, :dr_id, :action, :action_detail, "
            ":performed_by, :performed_at, :status, :error_message)"
        )
        db_client.sql_exec_with_params(sql, params)
        return sql

    def list_by_dr_id(
        self,
        db_client: Any,
        *,
        dr_id: str,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Return audit entries for a DR, ordered by performed_at descending."""
        # `limit` is int-cast and safe to interpolate; LIMIT clauses are not
        # parameterizable in Statement Execution API.
        sql = (
            f"SELECT * FROM {self._table} "
            "WHERE dr_id = :dr_id "
            "ORDER BY performed_at DESC "
            f"LIMIT {int(limit)}"
        )
        return db_client.sql_with_params(sql, {"dr_id": dr_id})

    def list_by_action(
        self,
        db_client: Any,
        *,
        action: str,
    ) -> list[dict[str, Any]]:
        """Return all audit entries with a given action, newest first."""
        sql = (
            f"SELECT * FROM {self._table} "
            "WHERE action = :action "
            "ORDER BY performed_at DESC"
        )
        return db_client.sql_with_params(sql, {"action": action})

    def purge_old_entries(
        self,
        db_client: Any,
        retention_days: int = 365,
    ) -> int:
        """Delete audit entries older than *retention_days*."""
        # `retention_days` is int-cast and safe to interpolate; DATEADD's
        # interval offset must be a literal int in Spark SQL.
        sql = (
            f"DELETE FROM {self._table} "
            f"WHERE performed_at < DATEADD(DAY, -{int(retention_days)}, CURRENT_TIMESTAMP())"
        )
        logger.info(
            "Purging audit entries older than %d days from %s",
            retention_days,
            self._table,
        )

        try:
            db_client.sql_exec(sql)
            # purge_old_entries previously relied on statement execution API response
            # to count affected rows. With spark.sql() we can't easily get this,
            # so we return 0 as the default.
            logger.info("Audit purge completed.")
            return 0
        except Exception:
            logger.exception("Audit purge failed for %s", self._table)
            raise
