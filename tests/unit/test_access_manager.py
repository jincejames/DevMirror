"""Tests for devmirror.provision.access_manager."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from devmirror.provision.access_manager import (
    AccessGrantResult,
    AccessManagerError,
    apply_grants,
    apply_revokes,
    generate_grant_statements,
    grant_schema_rw_sql,
    grant_schema_usage_sql,
    revoke_schema_sql,
)

# ===================================================================
# Grant SQL generation
# ===================================================================


def _mock_db() -> MagicMock:
    m = MagicMock()
    m.grant = MagicMock()
    m.revoke = MagicMock()
    return m


class TestGrantSchemaUsageSql:
    def test_basic(self) -> None:
        sql = grant_schema_usage_sql(
            "dev_analytics.dr_1042_customers", "dev@company.com"
        )
        assert sql == (
            "GRANT USAGE ON SCHEMA dev_analytics.dr_1042_customers "
            "TO `dev@company.com`"
        )

    def test_rejects_bad_schema(self) -> None:
        with pytest.raises(AccessManagerError, match="two-part"):
            grant_schema_usage_sql("single", "user@co.com")

    def test_rejects_unsafe_principal(self) -> None:
        with pytest.raises(AccessManagerError, match="Unsafe principal"):
            grant_schema_usage_sql("a.b", "user; DROP TABLE--")


class TestGrantSchemaRwSql:
    def test_basic(self) -> None:
        sql = grant_schema_rw_sql(
            "dev_analytics.dr_1042_customers", "dev@company.com"
        )
        assert sql == (
            "GRANT SELECT, MODIFY ON SCHEMA dev_analytics.dr_1042_customers "
            "TO `dev@company.com`"
        )

    def test_group_principal(self) -> None:
        sql = grant_schema_rw_sql("dev_analytics.dr_1042_customers", "data-engineers")
        assert "TO `data-engineers`" in sql


class TestRevokeSchemaSQL:
    def test_basic(self) -> None:
        sql = revoke_schema_sql("dev_analytics.dr_1042_customers", "dev@company.com")
        assert "REVOKE ALL PRIVILEGES ON SCHEMA" in sql
        assert "FROM `dev@company.com`" in sql


# ===================================================================
# generate_grant_statements
# ===================================================================


class TestGenerateGrantStatements:
    def test_single_schema_single_principal(self) -> None:
        stmts = generate_grant_statements(
            ["dev_analytics.dr_1042_customers"],
            ["dev@company.com"],
        )
        assert len(stmts) == 2
        assert "GRANT USAGE" in stmts[0]
        assert "GRANT SELECT, MODIFY" in stmts[1]

    def test_multiple_schemas_multiple_principals(self) -> None:
        stmts = generate_grant_statements(
            ["dev_analytics.dr_1042_customers", "dev_analytics.dr_1042_shared"],
            ["dev1@co.com", "dev2@co.com"],
        )
        # 2 schemas x 2 principals x 2 statements = 8
        assert len(stmts) == 8

    def test_empty_inputs(self) -> None:
        stmts = generate_grant_statements([], ["dev@co.com"])
        assert len(stmts) == 0

        stmts = generate_grant_statements(["a.b"], [])
        assert len(stmts) == 0

    def test_no_modify_on_prod(self) -> None:
        """Security: generated SQL never references prod catalogs for writes."""
        stmts = generate_grant_statements(
            ["dev_analytics.dr_1042_customers"],
            ["dev@company.com"],
        )
        for sql in stmts:
            assert "prod" not in sql.lower()


# ===================================================================
# apply_grants (now uses SDK grant API)
# ===================================================================


class TestApplyGrants:
    def test_all_success(self) -> None:
        db = _mock_db()
        result = apply_grants(
            db,
            ["dev_analytics.dr_1042_customers"],
            ["dev@company.com"],
        )
        assert result.all_succeeded
        assert result.granted == 2  # USE_SCHEMA + SELECT,MODIFY
        assert len(result.failed) == 0

    def test_partial_failure(self) -> None:
        db = _mock_db()
        call_count = 0

        def side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:  # second grant call fails
                raise Exception("Access denied")

        db.grant.side_effect = side_effect
        result = apply_grants(
            db,
            ["dev_analytics.dr_1042_customers"],
            ["dev@company.com"],
        )
        assert not result.all_succeeded
        assert result.granted == 1
        assert len(result.failed) == 1

    def test_empty_principals(self) -> None:
        db = _mock_db()
        result = apply_grants(db, ["a.b"], [])
        assert result.all_succeeded
        assert result.granted == 0


# ===================================================================
# apply_revokes (now uses SDK revoke API)
# ===================================================================


class TestApplyRevokes:
    def test_all_success(self) -> None:
        db = _mock_db()
        result = apply_revokes(
            db,
            ["dev_analytics.dr_1042_customers"],
            ["dev@company.com"],
        )
        assert result.all_succeeded
        assert result.granted == 1

    def test_failure(self) -> None:
        db = _mock_db()
        db.revoke.side_effect = Exception("fail")
        result = apply_revokes(db, ["a.b"], ["u@co.com"])
        assert not result.all_succeeded
        assert len(result.failed) == 1


# ===================================================================
# AccessGrantResult
# ===================================================================


class TestAccessGrantResult:
    def test_all_succeeded_true(self) -> None:
        result = AccessGrantResult(granted=2, failed=[])
        assert result.all_succeeded

    def test_all_succeeded_false(self) -> None:
        result = AccessGrantResult(granted=1, failed=[("sql", "err")])
        assert not result.all_succeeded
