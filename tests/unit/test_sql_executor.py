"""Tests for devmirror.utils.sql_executor.

All tests mock the Databricks SDK WorkspaceClient to avoid network calls.
Validates correct API invocation shape, error mapping, and result parsing.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from devmirror.utils.sql_executor import SqlExecutionError, SqlExecutor


def _mock_column(name: str) -> MagicMock:
    col = MagicMock()
    col.name = name
    return col


def _success_response(
    *,
    columns: list[str] | None = None,
    rows: list[list] | None = None,
    statement_id: str = "stmt-123",
) -> MagicMock:
    """Build a mock ExecuteStatementResponse in SUCCEEDED state."""
    from databricks.sdk.service.sql import StatementState

    resp = MagicMock()
    resp.statement_id = statement_id
    resp.status = MagicMock()
    resp.status.state = StatementState.SUCCEEDED
    resp.status.error = None

    if columns is not None:
        resp.manifest = MagicMock()
        resp.manifest.schema = MagicMock()
        resp.manifest.schema.columns = [_mock_column(c) for c in columns]
        resp.result = MagicMock()
        resp.result.data_array = rows or []
    else:
        resp.manifest = None
        resp.result = None

    return resp


def _failed_response(
    *,
    state_str: str = "FAILED",
    error_message: str = "Something went wrong",
    statement_id: str = "stmt-err",
) -> MagicMock:
    from databricks.sdk.service.sql import StatementState

    resp = MagicMock()
    resp.statement_id = statement_id
    resp.status = MagicMock()
    resp.status.state = StatementState[state_str]
    resp.status.error = MagicMock()
    resp.status.error.message = error_message
    return resp


# ===========================================================================
# SqlExecutor.execute
# ===========================================================================

class TestSqlExecutorExecute:
    def test_successful_execution(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _success_response()

        executor = SqlExecutor("wh-123", client=client)
        resp = executor.execute("SELECT 1")

        client.statement_execution.execute_statement.assert_called_once()
        call_kwargs = client.statement_execution.execute_statement.call_args
        assert call_kwargs.kwargs["statement"] == "SELECT 1"
        assert call_kwargs.kwargs["warehouse_id"] == "wh-123"
        assert resp.statement_id == "stmt-123"

    def test_catalog_and_schema_passed(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _success_response()

        executor = SqlExecutor("wh-123", client=client)
        executor.execute("SELECT 1", catalog="my_cat", schema="my_schema")

        call_kwargs = client.statement_execution.execute_statement.call_args.kwargs
        assert call_kwargs["catalog"] == "my_cat"
        assert call_kwargs["schema"] == "my_schema"

    def test_failed_state_raises(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _failed_response()

        executor = SqlExecutor("wh-123", client=client)
        with pytest.raises(SqlExecutionError, match="FAILED") as exc_info:
            executor.execute("BAD SQL")

        assert exc_info.value.statement_id == "stmt-err"
        assert exc_info.value.sql == "BAD SQL"
        assert exc_info.value.state == "FAILED"

    def test_canceled_state_raises(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _failed_response(
            state_str="CANCELED", error_message="User cancelled"
        )

        executor = SqlExecutor("wh-123", client=client)
        with pytest.raises(SqlExecutionError, match="CANCELED"):
            executor.execute("SELECT 1")

    def test_warehouse_id_property(self) -> None:
        executor = SqlExecutor("wh-abc", client=MagicMock())
        assert executor.warehouse_id == "wh-abc"


# ===========================================================================
# SqlExecutor.fetch_rows
# ===========================================================================

class TestSqlExecutorFetchRows:
    def test_returns_list_of_dicts(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _success_response(
            columns=["id", "name", "value"],
            rows=[
                ["1", "alpha", "100"],
                ["2", "beta", "200"],
            ],
        )

        executor = SqlExecutor("wh-123", client=client)
        rows = executor.fetch_rows("SELECT * FROM t")

        assert len(rows) == 2
        assert rows[0] == {"id": "1", "name": "alpha", "value": "100"}
        assert rows[1] == {"id": "2", "name": "beta", "value": "200"}

    def test_empty_result(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _success_response(
            columns=["id"], rows=[]
        )

        executor = SqlExecutor("wh-123", client=client)
        rows = executor.fetch_rows("SELECT * FROM empty_table")
        assert rows == []

    def test_no_manifest_returns_empty(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _success_response()

        executor = SqlExecutor("wh-123", client=client)
        rows = executor.fetch_rows("CREATE TABLE t (id INT)")
        assert rows == []

    def test_fetch_rows_propagates_error(self) -> None:
        client = MagicMock()
        client.statement_execution.execute_statement.return_value = _failed_response()

        executor = SqlExecutor("wh-123", client=client)
        with pytest.raises(SqlExecutionError):
            executor.fetch_rows("BAD SQL")


# ===========================================================================
# SqlExecutionError attributes
# ===========================================================================

class TestSqlExecutionError:
    def test_attributes(self) -> None:
        err = SqlExecutionError(
            "test error",
            statement_id="s-1",
            sql="SELECT 1",
            state="FAILED",
        )
        assert err.statement_id == "s-1"
        assert err.sql == "SELECT 1"
        assert err.state == "FAILED"
        assert "test error" in str(err)

    def test_defaults_to_none(self) -> None:
        err = SqlExecutionError("basic error")
        assert err.statement_id is None
        assert err.sql is None
        assert err.state is None
