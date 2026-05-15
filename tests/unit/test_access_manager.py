"""Tests for devmirror.provision.access_manager."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from devmirror.provision.access_manager import (
    AccessGrantResult,
    AccessManagerError,
    _validate_principal,
    apply_grants,
    apply_revokes,
    apply_volume_grants,
    generate_grant_statements,
    grant_schema_rw_sql,
    grant_schema_usage_sql,
    revoke_schema_sql,
)

# ===================================================================
# Principal validation -- users, groups, and service principals
# ===================================================================


class TestValidatePrincipal:
    """Locks in that _validate_principal accepts users, groups, and SPs."""

    @pytest.mark.parametrize(
        "principal",
        [
            "alice@company.com",              # user email
            "alice.smith@example.co.uk",      # user email with dotted local + TLD
            "data-engineers",                 # account group (hyphen)
            "data_engineers",                 # account group (underscore)
            "eng.analytics",                  # account group (dot)
            "odp-adw-developers",             # realistic hyphenated group name
            "12345678-1234-1234-1234-123456789abc",  # service principal UUID
        ],
    )
    def test_accepts_valid_principal(self, principal: str) -> None:
        # Should not raise.
        _validate_principal(principal)

    @pytest.mark.parametrize(
        "principal",
        [
            "data engineers",                 # space
            "user; DROP TABLE--",             # SQL injection
            "user#admin",                     # disallowed char
            "",                               # empty
        ],
    )
    def test_rejects_invalid_principal(self, principal: str) -> None:
        with pytest.raises(AccessManagerError, match="Unsafe principal"):
            _validate_principal(principal)

    def test_error_message_mentions_group_and_service_principal(self) -> None:
        """The error message must surface group + SP support so users discover it."""
        with pytest.raises(AccessManagerError) as exc_info:
            _validate_principal("bad name with spaces")
        msg = str(exc_info.value)
        assert "group" in msg.lower()
        assert "service principal" in msg.lower()


# ===================================================================
# Grant SQL generation
# ===================================================================


def _mock_db() -> MagicMock:
    m = MagicMock()
    m.grant = MagicMock()
    m.revoke = MagicMock()
    # Wire the SCIM existence-check mocks so apply_grants/apply_revokes
    # doesn't reject every principal as "not found" (Sec finding #9).
    found = MagicMock()
    m.client.users.list.return_value = [found]
    m.client.groups.list.return_value = [found]
    return m


@pytest.fixture(autouse=True)
def _clear_principal_cache():
    """Reset the existence-check cache between tests so mocks behave."""
    from devmirror.provision.access_manager import (
        _principal_cache,
        _principal_cache_lock,
    )
    with _principal_cache_lock:
        _principal_cache.clear()
    yield
    with _principal_cache_lock:
        _principal_cache.clear()


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

    def test_refuses_nonexistent_principal(self) -> None:
        """Sec finding #9: refuse to grant to a principal SCIM can't find."""
        db = _mock_db()
        # Override the SCIM mock to return empty (principal doesn't exist).
        db.client.users.list.return_value = []
        db.client.groups.list.return_value = []
        result = apply_grants(
            db, ["dev_analytics.dr_1042_customers"], ["ghost@company.com"],
        )
        assert not result.all_succeeded
        assert result.granted == 0
        assert len(result.failed) == 1
        assert "not found in workspace SCIM directory" in result.failed[0][1]
        # No actual grant calls should have been made.
        db.grant.assert_not_called()

    def test_writable_true_includes_modify(self) -> None:
        """writable=True (default) keeps RW behavior: USE_SCHEMA + SELECT,MODIFY."""
        from databricks.sdk.service.catalog import Privilege

        db = _mock_db()
        apply_grants(db, ["a.b"], ["dev@co.com"], writable=True)
        # Two grant calls -- USE_SCHEMA then SELECT+MODIFY.
        priv_sets = [set(c.args[3]) for c in db.grant.call_args_list]
        assert {Privilege.USE_SCHEMA} in priv_sets
        assert any(Privilege.MODIFY in s and Privilege.SELECT in s for s in priv_sets)

    def test_writable_false_omits_modify(self) -> None:
        """writable=False grants USE_SCHEMA + SELECT only, NOT MODIFY."""
        from databricks.sdk.service.catalog import Privilege

        db = _mock_db()
        apply_grants(db, ["a.b"], ["qa@co.com"], writable=False)
        priv_sets = [set(c.args[3]) for c in db.grant.call_args_list]
        # Every grant call must NOT contain MODIFY.
        for s in priv_sets:
            assert Privilege.MODIFY not in s
        # SELECT must still be granted.
        assert any(Privilege.SELECT in s for s in priv_sets)
        # USE_SCHEMA still granted.
        assert {Privilege.USE_SCHEMA} in priv_sets

    def test_writable_default_is_true(self) -> None:
        """Backwards compat: calling apply_grants() without the kwarg keeps RW."""
        from databricks.sdk.service.catalog import Privilege

        db = _mock_db()
        apply_grants(db, ["a.b"], ["dev@co.com"])
        priv_sets = [set(c.args[3]) for c in db.grant.call_args_list]
        assert any(Privilege.MODIFY in s for s in priv_sets)

    def test_scim_check_bypass_skips_existence_check(
        self, monkeypatch,
    ) -> None:
        """LH-style workspace where SP can't read SCIM: env var bypass
        must skip the filter-list rejection so grants actually fire."""
        monkeypatch.setenv("DEVMIRROR_SKIP_PRINCIPAL_SCIM_CHECK", "true")

        db = _mock_db()
        # Make SCIM return empty -- without the bypass this would
        # reject the principal as "not found".
        db.client.users.list.return_value = []
        db.client.groups.list.return_value = []

        result = apply_grants(db, ["cat.s"], ["dev@dlh.de"])
        # Grants must have been attempted (2 calls per principal:
        # USE_SCHEMA + RW), not blocked by SCIM pre-check.
        assert result.granted == 2
        assert result.failed == []
        # And users.list must NOT have been called at all -- the bypass
        # short-circuits before SCIM.
        db.client.users.list.assert_not_called()

    def test_scim_bypass_off_when_env_unset(self, monkeypatch) -> None:
        """Default (env var unset/false): SCIM check fires normally."""
        monkeypatch.delenv("DEVMIRROR_SKIP_PRINCIPAL_SCIM_CHECK", raising=False)
        db = _mock_db()
        db.client.users.list.return_value = []
        db.client.groups.list.return_value = []
        result = apply_grants(db, ["cat.s"], ["ghost@co.com"])
        assert not result.all_succeeded
        assert result.granted == 0  # SCIM rejected, no grants attempted

    def test_scim_bypass_off_when_env_false(self, monkeypatch) -> None:
        monkeypatch.setenv("DEVMIRROR_SKIP_PRINCIPAL_SCIM_CHECK", "false")
        db = _mock_db()
        db.client.users.list.return_value = []
        db.client.groups.list.return_value = []
        result = apply_grants(db, ["cat.s"], ["ghost@co.com"])
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


# ===================================================================
# apply_volume_grants -- per-volume READ_VOLUME / WRITE_VOLUME
# ===================================================================


class TestApplyVolumeGrants:
    """Volumes need their own securable-level grants on top of the
    schema-level USE_SCHEMA that apply_grants installs.  Developers get
    RW (READ + WRITE); QA users get R-only."""

    def test_writable_grants_read_and_write(self) -> None:
        from databricks.sdk.service.catalog import Privilege, SecurableType

        db = _mock_db()
        result = apply_volume_grants(
            db,
            ["cat.dr_1_import_main.main_volume"],
            ["dev@co.com"],
            writable=True,
        )
        assert result.all_succeeded and result.granted == 1
        call = db.grant.call_args
        assert call.args[0] == SecurableType.VOLUME
        assert call.args[1] == "cat.dr_1_import_main.main_volume"
        assert call.args[2] == "dev@co.com"
        privileges = call.args[3]
        assert Privilege.READ_VOLUME in privileges
        assert Privilege.WRITE_VOLUME in privileges

    def test_readonly_grants_only_read(self) -> None:
        from databricks.sdk.service.catalog import Privilege

        db = _mock_db()
        apply_volume_grants(
            db, ["cat.qa_1_import_main.main_volume"], ["qa@co.com"], writable=False,
        )
        privileges = db.grant.call_args.args[3]
        assert privileges == [Privilege.READ_VOLUME]

    def test_multiple_volumes_and_principals(self) -> None:
        db = _mock_db()
        result = apply_volume_grants(
            db,
            ["cat.dr_1_import_main.main_volume", "cat2.dr_1_import_main.main_volume"],
            ["dev1@co.com", "dev2@co.com"],
            writable=True,
        )
        # 2 volumes * 2 principals = 4 grant calls
        assert result.granted == 4
        assert db.grant.call_count == 4

    def test_empty_inputs_no_grants(self) -> None:
        db = _mock_db()
        result = apply_volume_grants(db, [], ["dev@co.com"], writable=True)
        assert result.granted == 0
        db.grant.assert_not_called()
        result = apply_volume_grants(db, ["cat.s.v"], [], writable=True)
        assert result.granted == 0
        db.grant.assert_not_called()

    def test_invalid_principal_short_circuits_with_failure(self) -> None:
        db = _mock_db()
        # Empty SCIM result -> principal "doesn't exist".
        db.client.users.list.return_value = []
        db.client.groups.list.return_value = []
        result = apply_volume_grants(
            db, ["cat.dr_1_import_main.main_volume"], ["ghost@co.com"], writable=True,
        )
        assert not result.all_succeeded
        assert any("not found" in msg for _, msg in result.failed)
        db.grant.assert_not_called()

    def test_invalid_volume_fqn_rejected(self) -> None:
        db = _mock_db()
        # Two-part FQN -- not a volume.
        with pytest.raises(AccessManagerError, match="three-part"):
            apply_volume_grants(db, ["cat.schema"], ["dev@co.com"], writable=True)

    def test_grant_failure_recorded(self) -> None:
        db = _mock_db()
        db.grant.side_effect = Exception("permission denied")
        result = apply_volume_grants(
            db, ["cat.dr_1_import_main.main_volume"], ["dev@co.com"], writable=True,
        )
        assert not result.all_succeeded
        assert "permission denied" in result.failed[0][1]
