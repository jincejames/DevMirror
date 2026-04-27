"""Unit tests for devmirror.modify.modification_engine."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from devmirror.control.audit import AuditRepository
from devmirror.control.control_table import (
    DrAccessRepository,
    DrObjectRepository,
    DRRepository,
    ObjectStatus,
)
from devmirror.modify.modification_engine import (
    ModificationError,
    _add_streams,
    modify_dr,
)

# ------------------------------------------------------------------
# Shared fixtures
# ------------------------------------------------------------------

_FUTURE = (date.today() + timedelta(days=30)).isoformat()
_FAR_FUTURE = (date.today() + timedelta(days=60)).isoformat()


@pytest.fixture(autouse=True)
def _clear_principal_cache():
    """Reset access_manager's existence-check cache between tests."""
    from devmirror.provision.access_manager import (
        _principal_cache,
        _principal_cache_lock,
    )
    with _principal_cache_lock:
        _principal_cache.clear()
    yield
    with _principal_cache_lock:
        _principal_cache.clear()


def _obj_row(
    source_fqn: str = "prod_cat.schema1.table1",
    target_fqn: str = "dev_cat.dr_100_schema1.table1",
    strategy: str = "shallow_clone",
    status: str = "PROVISIONED",
    env: str = "dev",
) -> dict[str, Any]:
    return {
        "dr_id": "DR-100", "source_fqn": source_fqn, "target_fqn": target_fqn,
        "target_environment": env, "object_type": "view" if strategy == "view" else "table",
        "access_mode": "READ_ONLY", "clone_strategy": strategy, "status": status,
    }


def _repos(
    status: str = "ACTIVE",
    obj_rows: list[dict[str, Any]] | None = None,
    dr_found: bool = True,
) -> tuple[MagicMock, MagicMock, MagicMock, MagicMock, MagicMock]:
    db = MagicMock()
    db.sql_exec = MagicMock()
    db.sql = MagicMock(return_value=[])
    db.grant = MagicMock()
    db.revoke = MagicMock()
    db.create_schema = MagicMock()
    # Wire SCIM existence-check mocks so apply_grants's principal-existence
    # check (Sec finding #9) doesn't reject every test principal.
    found = MagicMock()
    db.client.users.list.return_value = [found]
    db.client.groups.list.return_value = [found]

    dr_repo = MagicMock(spec=DRRepository)
    dr_repo.get = MagicMock(return_value={
        "dr_id": "DR-100", "status": status, "expiration_date": _FUTURE,
    } if dr_found else None)
    dr_repo.table_fqn = "ctl.admin.devmirror_development_requests"

    obj_repo = MagicMock(spec=DrObjectRepository)
    obj_repo.list_by_dr_id = MagicMock(return_value=obj_rows or [])
    obj_repo.update_object_status = MagicMock()
    obj_repo.bulk_insert = MagicMock()

    access_repo = MagicMock(spec=DrAccessRepository)
    access_repo.bulk_insert = MagicMock()

    audit_repo = MagicMock(spec=AuditRepository)
    audit_repo.append = MagicMock()

    return db, dr_repo, obj_repo, access_repo, audit_repo


def _call(db, dr_repo, obj_repo, access_repo, audit_repo, **kwargs):
    return modify_dr(
        "DR-100", db_client=db, dr_repo=dr_repo, obj_repo=obj_repo,
        access_repo=access_repo, audit_repo=audit_repo, **kwargs,
    )


# ------------------------------------------------------------------
# Validation
# ------------------------------------------------------------------

class TestModifyDrValidation:
    def test_not_found(self) -> None:
        s, d, o, a, au = _repos(dr_found=False)
        with pytest.raises(ModificationError, match="not found"):
            _call(s, d, o, a, au)

    def test_inactive_dr(self) -> None:
        s, d, o, a, au = _repos(status="CLEANED_UP")
        with pytest.raises(ModificationError, match="not active"):
            _call(s, d, o, a, au)

    def test_no_actions(self) -> None:
        s, d, o, a, au = _repos()
        result = _call(s, d, o, a, au)
        assert len(result.actions) == 0
        assert result.audit_status == "SUCCESS"


# ------------------------------------------------------------------
# Action types (parametrized happy paths)
# ------------------------------------------------------------------

class TestActions:
    def test_add_objects_success(self) -> None:
        s, d, o, a, au = _repos()
        result = _call(s, d, o, a, au, add_objects=[{
            "fqn": "prod_cat.schema1.new_table", "type": "table", "access_mode": "READ_ONLY",
        }])
        assert result.actions[0].action == "add_objects"
        assert result.actions[0].success is True

    def test_add_objects_invalid_fqn(self) -> None:
        s, d, o, a, au = _repos()
        result = _call(s, d, o, a, au, add_objects=[{
            "fqn": "bad_fqn", "type": "table", "access_mode": "READ_ONLY",
        }])
        assert len(result.actions) == 1

    def test_remove_objects_success(self) -> None:
        s, d, o, a, au = _repos(obj_rows=[_obj_row()])
        result = _call(s, d, o, a, au, remove_objects=["prod_cat.schema1.table1"])
        assert result.actions[0].success is True
        o.update_object_status.assert_called_once()
        assert o.update_object_status.call_args.kwargs["new_status"] == ObjectStatus.DROPPED

    def test_remove_view_uses_drop_view(self) -> None:
        s, d, o, a, au = _repos(obj_rows=[_obj_row(
            source_fqn="prod_cat.schema1.v1", target_fqn="dev_cat.dr_100_schema1.v1", strategy="view",
        )])
        _call(s, d, o, a, au, remove_objects=["prod_cat.schema1.v1"])
        assert any("DROP VIEW" in str(c) for c in s.sql_exec.call_args_list)

    def test_remove_already_dropped(self) -> None:
        s, d, o, a, au = _repos(obj_rows=[_obj_row(status="DROPPED")])
        result = _call(s, d, o, a, au, remove_objects=["prod_cat.schema1.table1"])
        assert result.actions[0].success is True
        o.update_object_status.assert_not_called()

    @pytest.mark.parametrize("param,action_name", [
        ("add_dev_users", "add_users"),
        ("remove_dev_users", "remove_users"),
    ])
    def test_user_management(self, param, action_name) -> None:
        s, d, o, a, au = _repos(obj_rows=[_obj_row()])
        result = _call(s, d, o, a, au, **{param: ["user@company.com"]})
        assert result.actions[0].action == action_name
        assert result.actions[0].success is True

    def test_add_users_no_schemas(self) -> None:
        s, d, o, a, au = _repos(obj_rows=[])
        result = _call(s, d, o, a, au, add_dev_users=["user@company.com"])
        assert result.actions[0].success is True

    def test_change_expiration_success(self) -> None:
        s, d, o, a, au = _repos()
        result = _call(s, d, o, a, au, new_expiration_date=_FAR_FUTURE)
        assert result.actions[0].action == "change_expiration"
        assert result.actions[0].success is True

    def test_change_expiration_past_date(self) -> None:
        s, d, o, a, au = _repos()
        past = (date.today() - timedelta(days=1)).isoformat()
        result = _call(s, d, o, a, au, new_expiration_date=past)
        assert result.actions[0].success is False
        assert "past" in (result.actions[0].error or "").lower()


# ------------------------------------------------------------------
# Partial success / audit
# ------------------------------------------------------------------

class TestPartialSuccess:
    def test_partial_failure(self) -> None:
        s, d, o, a, au = _repos(obj_rows=[_obj_row()])
        orig = s.sql_exec
        s.sql_exec = MagicMock(side_effect=lambda stmt, **kw: (_ for _ in ()).throw(RuntimeError("fail")) if "DROP" in stmt else orig(stmt, **kw))
        result = _call(s, d, o, a, au, remove_objects=["prod_cat.schema1.table1"], new_expiration_date=_FAR_FUTURE)
        assert len(result.actions) == 2
        assert result.audit_status in ("PARTIAL_SUCCESS", "FAILED")

    def test_audit_entry(self) -> None:
        s, d, o, a, au = _repos()
        _call(s, d, o, a, au, new_expiration_date=_FAR_FUTURE)
        au.append.assert_called_once()
        assert au.append.call_args.kwargs["action"] == "MODIFY"


# ------------------------------------------------------------------
# Add streams
# ------------------------------------------------------------------

class TestAddStreams:
    def test_discovers_and_provisions(self) -> None:
        from devmirror.scan.dependency_classifier import ClassificationResult, ClassifiedObject
        from devmirror.scan.lineage import LineageResult
        from devmirror.scan.stream_resolver import ResolvedStream

        db = MagicMock()
        db.sql_exec = MagicMock()
        db.sql = MagicMock(return_value=[])
        db.create_schema = MagicMock()
        obj_repo = MagicMock(spec=DrObjectRepository)
        obj_repo.list_by_dr_id = MagicMock(return_value=[])
        obj_repo.bulk_insert = MagicMock()

        with (
            patch("devmirror.scan.stream_resolver.resolve_streams",
                  return_value=([ResolvedStream(name="etl_job", resource_type="job", resource_id="123")], [])),
            patch("devmirror.scan.lineage.query_lineage",
                  return_value=LineageResult(edges=[], row_limit_hit=False)),
            patch("devmirror.scan.dependency_classifier.classify_dependencies",
                  return_value=ClassificationResult(
                      objects=[ClassifiedObject(fqn="prod_cat.schema1.new_table", object_type="table", access_mode="READ_ONLY")],
                      review_required=False)),
        ):
            result = _add_streams("DR-100", ["etl_job"], "dev", None, db, obj_repo, MagicMock())
        assert result.success is True
        assert "Streams resolved: 1" in result.detail

    def test_unresolved_fails(self) -> None:
        db = MagicMock()
        db.sql = MagicMock(return_value=[])
        with patch("devmirror.scan.stream_resolver.resolve_streams", return_value=([], ["missing"])):
            result = _add_streams("DR-100", ["missing"], "dev", None, db, MagicMock(spec=DrObjectRepository), MagicMock())
        assert result.success is False

    def test_no_client_fails(self) -> None:
        s, d, o, a, au = _repos()
        result = _call(s, d, o, a, au, add_streams=["some_stream"], client=None)
        assert result.actions[0].success is False
        assert "WorkspaceClient" in (result.actions[0].error or "")
