"""Tests for devmirror.provision.object_cloner."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from devmirror.config.schema import DataRevision
from devmirror.provision.object_cloner import (
    IMPORT_VOLUME_SUBDIRS,
    ClonerError,
    SchemaProvisioningError,
    SchemaProvisionResult,
    create_deep_clone_sql,
    create_schema_only_sql,
    create_schema_sql,
    create_shallow_clone_sql,
    create_view_sql,
    create_volume_sql,
    default_clone_strategy,
    execute_clone,
    generate_clone_sql,
    provision_schemas,
    provision_volume_subdirs,
    set_catalog_managed_sql,
)

# ------------------------------------------------------------------
# SQL generation (parametrized)
# ------------------------------------------------------------------

_SRC, _TGT = "prod.schema.tbl", "dev.dr_1_schema.tbl"
_REV_V = DataRevision(mode="version", version=42)
_REV_TS = DataRevision(mode="timestamp", timestamp="2026-04-01T00:00:00Z")


from .conftest import make_mock_db


def _mock_db() -> MagicMock:
    m = make_mock_db()
    m.create_schema = MagicMock()
    return m


class TestCloneSql:
    @pytest.mark.parametrize("fn,keyword", [
        (create_shallow_clone_sql, "SHALLOW CLONE"),
        (create_deep_clone_sql, "DEEP CLONE"),
        (create_view_sql, "CREATE OR REPLACE VIEW"),
        (create_schema_only_sql, "LIKE"),
    ])
    def test_basic(self, fn, keyword) -> None:
        assert keyword in fn(_SRC, _TGT)

    @pytest.mark.parametrize("fn", [
        create_shallow_clone_sql,
        create_deep_clone_sql,
        create_view_sql,
    ])
    def test_uses_create_or_replace(self, fn) -> None:
        # Idempotent re-provision: re-cloning the same target_fqn must
        # overwrite, not fail with "object exists".  Without this, a
        # re-provision of an unchanged DR breaks because the v1 clone
        # is still in UC.
        assert "CREATE OR REPLACE" in fn(_SRC, _TGT)

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

    @pytest.mark.parametrize("fn", [
        create_shallow_clone_sql,
        create_deep_clone_sql,
    ])
    def test_clone_builders_set_catalog_managed_inline(self, fn) -> None:
        # SHALLOW/DEEP CLONE honor an inline TBLPROPERTIES clause.
        assert "delta.feature.catalogManaged" in fn(_SRC, _TGT)

    def test_schema_only_omits_inline_catalog_managed(self) -> None:
        # `CREATE TABLE ... LIKE` silently drops an inline TBLPROPERTIES clause,
        # so the builder must NOT emit it (the ALTER follow-up handles it).
        sql = create_schema_only_sql(_SRC, _TGT)
        assert "catalogManaged" not in sql
        assert "LIKE" in sql

    def test_set_catalog_managed_sql(self) -> None:
        sql = set_catalog_managed_sql(_TGT)
        assert sql.startswith("ALTER TABLE")
        assert "delta.feature.catalogManaged" in sql

    @pytest.mark.parametrize("fn", [create_view_sql, create_volume_sql])
    def test_non_table_builders_omit_catalog_managed(self, fn) -> None:
        # Views/volumes are not Delta tables -- the property is invalid there.
        sql = fn(_TGT) if fn is create_volume_sql else fn(_SRC, _TGT)
        assert "catalogManaged" not in sql

    def test_catalog_managed_after_revision_clause(self) -> None:
        # The TBLPROPERTIES clause must follow the VERSION/TIMESTAMP AS OF suffix.
        sql = create_shallow_clone_sql(_SRC, _TGT, data_revision=_REV_V)
        assert "VERSION AS OF 42" in sql
        assert sql.index("VERSION AS OF 42") < sql.index("catalogManaged")


class TestGenerateCloneSql:
    @pytest.mark.parametrize("strategy,keyword", [
        ("shallow_clone", "SHALLOW CLONE"), ("deep_clone", "DEEP CLONE"),
        ("view", "CREATE OR REPLACE VIEW"), ("schema_only", "LIKE"),
        ("create_volume", "CREATE VOLUME"),
    ])
    def test_dispatches(self, strategy, keyword) -> None:
        assert keyword in generate_clone_sql("a.b.c", "d.e.f", strategy)

    def test_invalid_strategy(self) -> None:
        with pytest.raises(ClonerError, match="Unknown"):
            generate_clone_sql("a.b.c", "d.e.f", "bad")


class TestCreateVolumeSql:
    def test_basic(self) -> None:
        sql = create_volume_sql("cat.schema.main_volume")
        assert sql == "CREATE VOLUME IF NOT EXISTS cat.schema.main_volume"

    def test_rejects_invalid_fqn(self) -> None:
        with pytest.raises(ClonerError, match="three-part"):
            create_volume_sql("two.parts")

    def test_rejects_unsafe_chars(self) -> None:
        with pytest.raises(ClonerError, match="Unsafe"):
            create_volume_sql("cat.schema.v;DROP")


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

    def test_schema_only_runs_followup_alter(self) -> None:
        # schema_only executes CREATE TABLE ... LIKE then a follow-up ALTER to
        # mark the fresh table catalog-managed (LIKE drops the inline clause).
        db = _mock_db()
        r = execute_clone(db, "a.b.c", "d.e.f", "schema_only")
        assert r.success
        executed = [c.args[0] for c in db.sql_exec.call_args_list]
        assert any(s.startswith("CREATE TABLE") and "LIKE" in s for s in executed)
        assert any(
            s.startswith("ALTER TABLE") and "catalogManaged" in s for s in executed
        )

    def test_schema_only_alter_failure_is_nonfatal(self) -> None:
        # A failure on the follow-up ALTER must not fail the clone itself.
        db = _mock_db()
        db.sql_exec.side_effect = [None, Exception("alter denied")]
        r = execute_clone(db, "a.b.c", "d.e.f", "schema_only")
        assert r.success


# ------------------------------------------------------------------
# provision_volume_subdirs
# ------------------------------------------------------------------


class TestProvisionVolumeSubdirs:
    def test_creates_full_tree(self) -> None:
        db = _mock_db()
        created = provision_volume_subdirs(db, "cat.sch.main_volume")
        paths = [c.kwargs["directory_path"] for c in
                 db.client.files.create_directory.call_args_list]
        assert paths == [
            "/Volumes/cat/sch/main_volume/source",
            "/Volumes/cat/sch/main_volume/source/data",
            "/Volumes/cat/sch/main_volume/source/archive",
            "/Volumes/cat/sch/main_volume/source/ready",
        ]
        assert created == paths

    def test_continues_after_individual_failure(self) -> None:
        db = _mock_db()
        # Fail on the second call only; others should still be attempted.
        db.client.files.create_directory.side_effect = [
            None, Exception("perm denied"), None, None,
        ]
        created = provision_volume_subdirs(db, "cat.sch.main_volume")
        assert db.client.files.create_directory.call_count == 4
        assert "/Volumes/cat/sch/main_volume/source/data" not in created
        assert "/Volumes/cat/sch/main_volume/source/archive" in created

    def test_execute_clone_carves_subdirs_on_create_volume(self) -> None:
        db = _mock_db()
        r = execute_clone(db, "ignored.src.fqn", "cat.sch.main_volume",
                          "create_volume")
        assert r.success
        # CREATE VOLUME SQL + 4 directory creates.
        assert db.sql_exec.call_count == 1
        assert db.client.files.create_directory.call_count == len(
            IMPORT_VOLUME_SUBDIRS
        )

    def test_execute_clone_skips_subdirs_for_non_volume_strategy(self) -> None:
        db = _mock_db()
        execute_clone(db, "a.b.c", "d.e.f", "shallow_clone")
        db.client.files.create_directory.assert_not_called()


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
