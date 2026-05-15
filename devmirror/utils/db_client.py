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


def _securable_str(securable_type: Any) -> str:
    """Coerce a SecurableType enum (or str) into the value the SDK URL builder
    expects.  ``str(SecurableType.SCHEMA)`` returns ``'SecurableType.SCHEMA'``
    which the server rejects as ``SECURABLETYPE.SCHEMA``; ``.value`` gives
    the correct ``'SCHEMA'``.
    """
    if hasattr(securable_type, "value"):
        return str(securable_type.value)
    return str(securable_type)


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
        """Delete a schema, forcing drop of contained objects.

        ``force=True`` matches CASCADE semantics so unexpected residual
        objects (e.g. a clone that failed mid-flight, or an
        externally-created table in the schema) don't silently block
        cleanup.

        Exceptions are NOT swallowed -- they propagate to the caller so
        ``cleanup_engine`` can record `schemas_failed` and surface the
        problem in the audit log.  Callers that want best-effort
        semantics must wrap the call themselves.
        """
        self._client.schemas.delete(f"{catalog}.{schema}", force=True)

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
            securable_type=_securable_str(securable_type),
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
            securable_type=_securable_str(securable_type),
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

    # ------------------------------------------------------------------
    # Parameterized SQL execution (Statement Execution API named params)
    # ------------------------------------------------------------------

    def sql_with_params(self, statement: str, params: dict[str, str | None]) -> list[dict[str, Any]]:
        """Execute SQL with named parameters and return rows.

        Parameters use :name syntax in the SQL statement.
        Falls back to string interpolation for spark.sql() path.
        """
        spark = self._get_spark()
        if spark:
            # spark.sql doesn't support StatementParameterListItem — use string formatting
            # This path is only used on clusters, not in the app
            formatted = statement
            for k, v in params.items():
                placeholder = f":{k}"
                if v is None:
                    formatted = formatted.replace(placeholder, "NULL")
                else:
                    safe_v = v.replace("'", "''")
                    formatted = formatted.replace(placeholder, f"'{safe_v}'")
            return self.sql(formatted)
        return self._execute_via_api_params(statement, params)

    def sql_exec_with_params(self, statement: str, params: dict[str, str | None]) -> None:
        """Execute DDL/DML with named parameters (no result needed)."""
        spark = self._get_spark()
        if spark:
            formatted = statement
            for k, v in params.items():
                placeholder = f":{k}"
                if v is None:
                    formatted = formatted.replace(placeholder, "NULL")
                else:
                    safe_v = v.replace("'", "''")
                    formatted = formatted.replace(placeholder, f"'{safe_v}'")
            spark.sql(formatted)
            return
        self._execute_via_api_params(statement, params)

    def _execute_via_api_params(self, statement: str, params: dict[str, str | None]) -> list[dict[str, Any]]:
        """Execute via Statement Execution API with named parameters."""
        warehouse_id = os.environ.get("DEVMIRROR_WAREHOUSE_ID", "").strip()
        if not warehouse_id:
            raise RuntimeError(
                "No SparkSession available and DEVMIRROR_WAREHOUSE_ID not set. "
                "Run on a Databricks cluster or set DEVMIRROR_WAREHOUSE_ID for remote execution."
            )
        from databricks.sdk.service.sql import (
            Disposition,
            Format,
            StatementParameterListItem,
            StatementState,
        )

        param_list = [
            StatementParameterListItem(name=k, value=v if v is not None else "NULL", type="STRING")
            for k, v in params.items()
        ]

        resp = self._client.statement_execution.execute_statement(
            statement=statement,
            warehouse_id=warehouse_id,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
            parameters=param_list if param_list else None,
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

    def _get_spark(self) -> Any:
        """Return an active SparkSession if one is reachable, else None.

        Works in three runtimes:
          - Classic Databricks clusters (DATABRICKS_RUNTIME_VERSION is set)
          - Serverless job tasks (no DATABRICKS_RUNTIME_VERSION; Spark is
            already bootstrapped by the runtime, so getActiveSession()
            returns the platform's session)
          - Off-Databricks (laptops, CI) -- returns None so callers fall
            back to the Statement Execution API instead of accidentally
            spinning up a local Spark.

        The previous implementation gated on DATABRICKS_RUNTIME_VERSION,
        which is unset on serverless and forced every query through the
        warehouse API -- requiring a CAN_USE grant on the warehouse the
        runtime SP usually doesn't have.
        """
        try:
            from pyspark.sql import SparkSession
        except ImportError:
            return None

        active = SparkSession.getActiveSession()
        if active is not None:
            return active

        # No active session.  Only call getOrCreate() when a Databricks-y
        # env var indicates we're inside a managed runtime; otherwise
        # we'd start a local Spark on a developer laptop.
        databricks_signals = (
            "DATABRICKS_RUNTIME_VERSION",      # classic cluster
            "DATABRICKS_SERVERLESS_VERSION",   # serverless compute
            "DB_HOME",                          # both, set by the runtime
            "DATABRICKS_HOST",                  # apps + jobs auto-injected
        )
        if any(os.environ.get(k) for k in databricks_signals):
            try:
                return SparkSession.builder.getOrCreate()
            except Exception:
                return None
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
