"""Tests for devmirror.provision.object_cloner."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from devmirror.config.schema import DataRevision
from devmirror.provision.object_cloner import (
    ClonerError,
    SchemaProvisioningError,
    SchemaProvisionResult,
    create_deep_clone_sql,
    create_schema_only_sql,
    create_schema_sql,
    create_shallow_clone_sql,
    create_view_sql,
    default_clone_strategy,
    execute_clone,
    generate_clone_sql,
    provision_schemas,
)

# ------------------------------------------------------------------
# SQL generation (parametrized)
# ------------------------------------------------------------------

_SRC, _TGT = "prod.schema.tbl", "dev.dr_1_schema.tbl"
_REV_V = DataRevision(mode="version", version=42)
_REV_TS = DataRevision(mode="timestamp", timestamp="2026-04-01T00:00:00Z")


def _mock_db() -> MagicMock:
    m = MagicMock()
    m.sql_exec = MagicMock()
    m.sql = MagicMock(return_value=[])
    m.create_schema = MagicMock()
    return m


class TestCloneSql:
    @pytest.mark.parametrize("fn,keyword", [
        (create_shallow_clone_sql, "SHALLOW CLONE"),
        (create_deep_clone_sql, "DEEP CLONE"),
        (create_view_sql, "CREATE VIEW"),
        (create_schema_only_sql, "LIKE"),
    ])
    def test_basic(self, fn, keyword) -> None:
        assert keyword in fn(_SRC, _TGT)

    @pytest.mark.parametrize("fn", [create_shallow_clone_sql, create_deep_clone_sql, create_view_sql])
    def test_version_revision(self, fn) -> None:
        assert "VERSION AS OF 42" in fn(_SRC, _TGT, data_revision=_REV_V)

    @pytest.mark.parametrize("fn", [create_shallow_clone_sql, create_deep_clone_sql])
    def test_timestamp_revision(self, fn) -> None:
        assert "TIMESTAMP AS OF" in fn(_SRC, _TGT, data_revision=_REV_TS)

    def test_latest_no_clause(self) -> None:
        sql = create_shallow_clone_sql(_SRC, _TGT, data_revision=DataRevision(mode="latest"))
        assert "VERSION" not in sql and "TIMESTAMP" not in sql

    def test_rejects_invalid_fqn(self) -> None:
        with pytest.raises(ClonerError, match="three-part"):
            create_shallow_clone_sql("two.parts", "a.b.c")

    def test_rejects_unsafe_chars(self) -> None:
        with pytest.raises(ClonerError, match="Unsafe"):
            create_shallow_clone_sql("a.b.c; DROP TABLE", "d.e.f")


class TestGenerateCloneSql:
    @pytest.mark.parametrize("strategy,keyword", [
        ("shallow_clone", "SHALLOW CLONE"), ("deep_clone", "DEEP CLONE"),
        ("view", "CREATE VIEW"), ("schema_only", "LIKE"),
    ])
    def test_dispatches(self, strategy, keyword) -> None:
        assert keyword in generate_clone_sql("a.b.c", "d.e.f", strategy)

    def test_invalid_strategy(self) -> None:
        with pytest.raises(ClonerError, match="Unknown"):
            generate_clone_sql("a.b.c", "d.e.f", "bad")


# ------------------------------------------------------------------
# execute_clone
# ------------------------------------------------------------------

class TestExecuteClone:
    def test_success(self) -> None:
        r = execute_clone(_mock_db(), "a.b.c", "d.e.f", "shallow_clone")
        assert r.success and "SHALLOW CLONE" in r.sql

    def test_failure(self) -> None:
        db = _mock_db()
        db.sql_exec.side_effect = Exception("denied")
        r = execute_clone(db, "a.b.c", "d.e.f", "shallow_clone")
        assert not r.success and "denied" in r.error

    def test_invalid_fqn(self) -> None:
        r = execute_clone(_mock_db(), "bad", "d.e.f", "shallow_clone")
        assert not r.success and "three-part" in r.error

    def test_with_revision(self) -> None:
        r = execute_clone(_mock_db(), "a.b.c", "d.e.f", "shallow_clone", _REV_V)
        assert "VERSION AS OF 42" in r.sql


# ------------------------------------------------------------------
# default_clone_strategy
# ------------------------------------------------------------------

class TestDefaultStrategy:
    @pytest.mark.parametrize("obj_type,mode,expected", [
        ("table", "READ_ONLY", "shallow_clone"),
        ("table", "READ_WRITE", "shallow_clone"),
        ("view", "READ_ONLY", "view"),
    ])
    def test_strategies(self, obj_type, mode, expected) -> None:
        assert default_clone_strategy(obj_type, mode) == expected


# ===========================================================================
# Schema provisioning tests (merged from test_schema_provisioner.py)
# ===========================================================================


class TestCreateSchemaSql:
    def test_basic(self) -> None:
        sql = create_schema_sql("dev_analytics.dr_1042_customers")
        assert sql == "CREATE SCHEMA IF NOT EXISTS dev_analytics.dr_1042_customers"

    def test_qa_schema(self) -> None:
        sql = create_schema_sql("dev_analytics.qa_1042_shared")
        assert sql == "CREATE SCHEMA IF NOT EXISTS dev_analytics.qa_1042_shared"

    def test_rejects_single_part(self) -> None:
        with pytest.raises(SchemaProvisioningError, match="two-part"):
            create_schema_sql("just_one")

    def test_rejects_three_parts(self) -> None:
        with pytest.raises(SchemaProvisioningError, match="two-part"):
            create_schema_sql("a.b.c")

    def test_rejects_unsafe_identifier(self) -> None:
        with pytest.raises(SchemaProvisioningError, match="Unsafe"):
            create_schema_sql("dev_analytics.dr_1042; DROP TABLE --")

    def test_rejects_spaces(self) -> None:
        with pytest.raises(SchemaProvisioningError, match="Unsafe"):
            create_schema_sql("dev analytics.dr_1042_customers")

    def test_idempotent_sql(self) -> None:
        sql = create_schema_sql("catalog.schema_name")
        assert "IF NOT EXISTS" in sql


class TestProvisionSchemas:
    def test_all_success(self) -> None:
        db = _mock_db()
        schemas = [
            "dev_analytics.dr_1042_customers",
            "dev_analytics.dr_1042_shared",
        ]
        result = provision_schemas(db, schemas)
        assert result.all_succeeded
        assert len(result.created) == 2
        assert len(result.failed) == 0
        assert db.create_schema.call_count == 2

    def test_partial_failure(self) -> None:
        db = _mock_db()
        db.create_schema.side_effect = [None, Exception("Catalog not found")]
        schemas = [
            "dev_analytics.dr_1042_customers",
            "dev_analytics.dr_1042_shared",
        ]
        result = provision_schemas(db, schemas)
        assert not result.all_succeeded
        assert len(result.created) == 1
        assert result.created[0] == "dev_analytics.dr_1042_customers"
        assert "dev_analytics.dr_1042_shared" in result.failed
        assert "Catalog not found" in result.failed["dev_analytics.dr_1042_shared"]

    def test_all_fail(self) -> None:
        db = _mock_db()
        db.create_schema.side_effect = Exception("Permission denied")
        schemas = ["dev_analytics.dr_1042_customers"]
        result = provision_schemas(db, schemas)
        assert not result.all_succeeded
        assert len(result.created) == 0
        assert len(result.failed) == 1

    def test_empty_list(self) -> None:
        db = _mock_db()
        result = provision_schemas(db, [])
        assert result.all_succeeded
        assert len(result.created) == 0
        assert db.create_schema.call_count == 0

    def test_calls_create_schema_with_correct_args(self) -> None:
        db = _mock_db()
        provision_schemas(db, ["dev_analytics.dr_1042_customers"])
        db.create_schema.assert_called_once_with("dev_analytics", "dr_1042_customers")


class TestSchemaProvisionResult:
    def test_all_succeeded_true(self) -> None:
        result = SchemaProvisionResult(created=["a.b"], failed={})
        assert result.all_succeeded

    def test_all_succeeded_false(self) -> None:
        result = SchemaProvisionResult(created=[], failed={"a.b": "err"})
        assert not result.all_succeeded
