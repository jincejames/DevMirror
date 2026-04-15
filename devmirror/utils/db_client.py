"""Unified Databricks client using SDK APIs + spark.sql() fallback.

Replaces SqlExecutor as the primary execution interface. Uses:
- Python SDK for schema CRUD, grants, table delete
- spark.sql() for CLONE/VIEW DDL, DML, lineage queries, DESCRIBE HISTORY
- Statement Execution API as a fallback when no SparkSession is available
"""

from __future__ import annotations

import logging
import os
from typing import Any

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class DbClient:
    """Unified Databricks client using SDK APIs + spark.sql() fallback."""

    def __init__(self, client: WorkspaceClient | None = None) -> None:
        self._client = client or WorkspaceClient()

    @property
    def client(self) -> WorkspaceClient:
        return self._client

    # ------------------------------------------------------------------
    # Schema operations via SDK
    # ------------------------------------------------------------------

    def create_schema(self, catalog: str, schema: str) -> None:
        """Create a schema idempotently via the SDK."""
        try:
            self._client.schemas.create(name=schema, catalog_name=catalog)
        except Exception as e:
            if "SCHEMA_ALREADY_EXISTS" in str(e) or "already exists" in str(e).lower():
                return  # Idempotent
            raise

    def delete_schema(self, catalog: str, schema: str) -> None:
        """Delete a schema (best effort)."""
        import contextlib

        with contextlib.suppress(Exception):
            self._client.schemas.delete(f"{catalog}.{schema}")

    # ------------------------------------------------------------------
    # Grant operations via SDK
    # ------------------------------------------------------------------

    def grant(
        self,
        securable_type: Any,
        full_name: str,
        principal: str,
        privileges: list[Any],
    ) -> None:
        """Grant privileges via the SDK grants API."""
        from databricks.sdk.service.catalog import PermissionsChange

        self._client.grants.update(
            securable_type=securable_type,
            full_name=full_name,
            changes=[PermissionsChange(add=privileges, principal=principal)],
        )

    def revoke(
        self,
        securable_type: Any,
        full_name: str,
        principal: str,
        privileges: list[Any],
    ) -> None:
        """Revoke privileges via the SDK grants API."""
        from databricks.sdk.service.catalog import PermissionsChange

        self._client.grants.update(
            securable_type=securable_type,
            full_name=full_name,
            changes=[PermissionsChange(remove=privileges, principal=principal)],
        )

    # ------------------------------------------------------------------
    # Table operations via SDK
    # ------------------------------------------------------------------

    def delete_table(self, full_name: str) -> None:
        """Delete a table or view (best effort)."""
        import contextlib

        with contextlib.suppress(Exception):
            self._client.tables.delete(full_name)

    def table_exists(self, full_name: str) -> bool:
        """Check if a table exists."""
        return self._client.tables.exists(full_name)

    # ------------------------------------------------------------------
    # SQL execution via spark.sql() with statement execution fallback
    # ------------------------------------------------------------------

    def sql(self, statement: str) -> list[dict[str, Any]]:
        """Execute SQL and return rows. Uses spark.sql() when available."""
        spark = self._get_spark()
        if spark:
            df = spark.sql(statement)
            if df.columns:
                return [row.asDict() for row in df.collect()]
            return []
        # Fallback to statement execution when not on a cluster
        return self._execute_via_api(statement)

    def sql_exec(self, statement: str) -> None:
        """Execute a DDL/DML statement (no result needed)."""
        spark = self._get_spark()
        if spark:
            spark.sql(statement)
            return
        self._execute_via_api(statement)

    def _get_spark(self) -> Any:
        """Get SparkSession only if running on Databricks runtime."""
        if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
            return None
        try:
            from pyspark.sql import SparkSession

            return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        except ImportError:
            return None

    def _execute_via_api(self, statement: str) -> list[dict[str, Any]]:
        """Fallback: use statement execution API (requires warehouse_id)."""
        warehouse_id = os.environ.get("DEVMIRROR_WAREHOUSE_ID", "").strip()
        if not warehouse_id:
            raise RuntimeError(
                "No SparkSession available and DEVMIRROR_WAREHOUSE_ID not set. "
                "Run on a Databricks cluster or set DEVMIRROR_WAREHOUSE_ID for remote execution."
            )
        from databricks.sdk.service.sql import Disposition, Format, StatementState

        resp = self._client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=warehouse_id,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
            wait_timeout="50s",
        )
        if resp.status and resp.status.state in (
            StatementState.FAILED,
            StatementState.CANCELED,
        ):
            err = resp.status.error.message if resp.status.error else "unknown"
            raise RuntimeError(f"SQL failed: {err}")
        if resp.manifest and resp.result and resp.result.data_array:
            schema_obj = getattr(resp.manifest, "schema", resp.manifest)
            cols = [c.name for c in (getattr(schema_obj, "columns", None) or [])]
            if cols:
                return [
                    dict(zip(cols, row, strict=False))
                    for row in resp.result.data_array
                ]
        return []
