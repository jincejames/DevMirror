"""Append-only audit log repository."""

from __future__ import annotations

import logging
import uuid
from typing import Any

from devmirror.utils.sql_executor import escape_sql_string as _escape

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
        detail_sql = f"'{_escape(action_detail)}'" if action_detail else "NULL"
        error_sql = f"'{_escape(error_message)}'" if error_message else "NULL"

        sql = (
            f"INSERT INTO {self._table} "
            f"(log_id, dr_id, action, action_detail, performed_by, "
            f"performed_at, status, error_message) "
            f"VALUES ("
            f"'{_escape(log_id)}', "
            f"'{_escape(dr_id)}', "
            f"'{_escape(action)}', "
            f"{detail_sql}, "
            f"'{_escape(performed_by)}', "
            f"'{_escape(performed_at)}', "
            f"'{_escape(status)}', "
            f"{error_sql})"
        )
        db_client.sql_exec(sql)
        return sql

    def list_by_dr_id(
        self,
        db_client: Any,
        *,
        dr_id: str,
        limit: int = 500,
    ) -> list[dict[str, Any]]:
        """Return audit entries for a DR, ordered by performed_at descending."""
        sql = (
            f"SELECT * FROM {self._table} "
            f"WHERE dr_id = '{_escape(dr_id)}' "
            f"ORDER BY performed_at DESC "
            f"LIMIT {int(limit)}"
        )
        return db_client.sql(sql)

    def purge_old_entries(
        self,
        db_client: Any,
        retention_days: int = 365,
    ) -> int:
        """Delete audit entries older than *retention_days*."""
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
