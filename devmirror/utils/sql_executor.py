"""Centralized Databricks SQL execution wrapper and low-level SQL helpers."""

from __future__ import annotations

from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Disposition,
    Format,
    StatementResponse,
    StatementState,
)


def escape_sql_string(value: str) -> str:
    """Escape single quotes for use inside SQL string literals."""
    return value.replace("'", "''")


class SqlExecutionError(Exception):
    """A SQL statement failed or timed out on the warehouse."""

    def __init__(
        self,
        message: str,
        *,
        statement_id: str | None = None,
        sql: str | None = None,
        state: str | None = None,
    ) -> None:
        self.statement_id = statement_id
        self.sql = sql
        self.state = state
        super().__init__(message)


class SqlExecutor:
    """Execute SQL statements against a Databricks SQL warehouse."""

    def __init__(
        self,
        warehouse_id: str,
        *,
        client: WorkspaceClient | None = None,
    ) -> None:
        self._warehouse_id = warehouse_id
        self._client = client or WorkspaceClient()

    @property
    def warehouse_id(self) -> str:
        return self._warehouse_id

    def execute(
        self,
        sql: str,
        *,
        catalog: str | None = None,
        schema: str | None = None,
        wait_timeout: str = "30s",
    ) -> StatementResponse:
        """Execute a single SQL statement and wait for completion."""
        response = self._client.statement_execution.execute_statement(
            statement=sql,
            warehouse_id=self._warehouse_id,
            catalog=catalog,
            schema=schema,
            wait_timeout=wait_timeout,
            disposition=Disposition.INLINE,
            format=Format.JSON_ARRAY,
        )

        status = response.status
        if status is None or status.state in (
            StatementState.FAILED,
            StatementState.CANCELED,
            StatementState.CLOSED,
        ):
            error_msg = ""
            if status and status.error:
                error_msg = f": {status.error.message}"
            state_name = status.state.value if status and status.state else "UNKNOWN"
            raise SqlExecutionError(
                f"SQL statement {state_name}{error_msg}",
                statement_id=response.statement_id,
                sql=sql,
                state=state_name,
            )

        return response

    def fetch_rows(
        self,
        sql: str,
        *,
        catalog: str | None = None,
        schema: str | None = None,
        wait_timeout: str = "30s",
    ) -> list[dict[str, Any]]:
        """Execute SQL and return result rows as a list of dicts."""
        response = self.execute(
            sql, catalog=catalog, schema=schema, wait_timeout=wait_timeout
        )

        manifest = response.manifest
        result = response.result

        if manifest is None or result is None or result.data_array is None:
            return []

        # SDK stores column info under manifest.schema.columns
        schema_obj = getattr(manifest, "schema", None) or manifest
        col_list = getattr(schema_obj, "columns", None) or []
        columns = [col.name for col in col_list]
        if not columns:
            return []

        return [dict(zip(columns, row, strict=False)) for row in result.data_array]
