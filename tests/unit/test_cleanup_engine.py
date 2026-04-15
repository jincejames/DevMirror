"""Unit tests for devmirror.cleanup.cleanup_engine."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from devmirror.cleanup.cleanup_engine import (
    _collect_schemas_from_objects,
    _drop_object_sql,
    _drop_schema_sql,
    cleanup_dr,
    find_expired_drs,
)
from devmirror.control.control_table import DRStatus

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

_OBJS = [
    {"dr_id": "DR-1", "source_fqn": "p.s.t", "target_fqn": "d.dr_1_s.t",
     "target_environment": "dev", "object_type": "table", "status": "PROVISIONED"},
    {"dr_id": "DR-1", "source_fqn": "p.s.v", "target_fqn": "d.dr_1_s.v",
     "target_environment": "dev", "object_type": "view", "status": "PROVISIONED"},
]


def _mock_db() -> MagicMock:
    m = MagicMock()
    m.sql_exec = MagicMock()
    m.sql = MagicMock(return_value=[])
    m.delete_table = MagicMock()
    m.delete_schema = MagicMock()
    m.revoke = MagicMock()
    return m


def _repos(objs=None, dr_status="ACTIVE", dr_found=True):
    db = _mock_db()
    dr = MagicMock()
    dr.table_fqn = "ctl.admin.devmirror_development_requests"
    dr.get = MagicMock(return_value={"dr_id": "DR-1", "status": dr_status} if dr_found else None)
    dr.update_status = MagicMock()
    obj = MagicMock()
    obj.list_by_dr_id = MagicMock(return_value=objs if objs is not None else _OBJS)
    obj.update_object_status = MagicMock()
    acc = MagicMock()
    acc.list_by_dr_id = MagicMock(return_value=[
        {"dr_id": "DR-1", "user_email": "dev@co.com", "environment": "dev"},
    ])
    aud = MagicMock()
    aud.append = MagicMock()
    return db, dr, obj, acc, aud


def _cleanup(objs=None, dr_status="ACTIVE", current_status=None, **kw):
    db, dr, obj, acc, aud = _repos(objs=objs, dr_status=dr_status)
    r = cleanup_dr("DR-1", db_client=db, dr_repo=dr, obj_repo=obj,
                   access_repo=acc, audit_repo=aud,
                   current_status=current_status, **kw)
    return r, db, dr, aud


# ------------------------------------------------------------------
# SQL helpers
# ------------------------------------------------------------------

class TestSqlHelpers:
    @pytest.mark.parametrize("obj_type,keyword", [("view", "DROP VIEW"), ("table", "DROP TABLE")])
    def test_drop_object(self, obj_type, keyword) -> None:
        assert keyword in _drop_object_sql("d.s.o", obj_type)

    def test_drop_schema(self) -> None:
        assert "CASCADE" in _drop_schema_sql("d.s")

    def test_collect_schemas(self) -> None:
        objs = [{"target_fqn": "d.s1.t"}, {"target_fqn": "d.s1.v"}, {"target_fqn": "d.s2.x"}]
        assert set(_collect_schemas_from_objects(objs)) == {"d.s1", "d.s2"}

    def test_collect_schemas_empty(self) -> None:
        assert _collect_schemas_from_objects([]) == []


# ------------------------------------------------------------------
# Full cleanup flow
# ------------------------------------------------------------------

class TestCleanupDr:
    def test_success(self) -> None:
        r, _db, _dr, aud = _cleanup()
        assert r.final_status == "CLEANED_UP"
        assert r.objects_dropped == 2
        assert r.fully_cleaned is True
        assert aud.append.call_count >= 2

    def test_skips_already_dropped(self) -> None:
        objs = [{**_OBJS[0], "status": "DROPPED"}, _OBJS[1]]
        r, *_ = _cleanup(objs=objs)
        assert r.objects_skipped == 1 and r.objects_dropped == 1

    def test_partial_failure(self) -> None:
        db, dr, obj, acc, aud = _repos()
        # Make delete_table raise for tables
        call_count = 0
        def delete_side(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:  # second call (table after view)
                raise RuntimeError("fail")
        db.delete_table.side_effect = delete_side
        r = cleanup_dr("DR-1", db_client=db, dr_repo=dr, obj_repo=obj,
                       access_repo=acc, audit_repo=aud, current_status=DRStatus.ACTIVE)
        assert r.final_status == "CLEANUP_IN_PROGRESS"
        assert not r.fully_cleaned

    def test_retry_from_cleanup_in_progress(self) -> None:
        _r, _, dr, _ = _cleanup(current_status=DRStatus.CLEANUP_IN_PROGRESS)
        first_call = dr.update_status.call_args_list[0]
        assert first_call[1]["new_status"] == DRStatus.CLEANED_UP

    def test_not_found(self) -> None:
        db, dr, obj, acc, aud = _repos(dr_found=False)
        r = cleanup_dr("DR-99", db_client=db, dr_repo=dr, obj_repo=obj,
                       access_repo=acc, audit_repo=aud)
        assert r.final_status == "NOT_FOUND"

    def test_views_before_tables(self) -> None:
        """Verify that delete_table is called for views before tables."""
        db, dr, obj, acc, aud = _repos()
        deleted: list[str] = []
        db.delete_table.side_effect = lambda fqn: deleted.append(fqn)
        cleanup_dr("DR-1", db_client=db, dr_repo=dr, obj_repo=obj,
                   access_repo=acc, audit_repo=aud)
        # View should be deleted before table
        view_idx = next((i for i, fqn in enumerate(deleted) if "v" in fqn.split(".")[-1]), None)
        table_idx = next((i for i, fqn in enumerate(deleted) if fqn.split(".")[-1] == "t"), None)
        if view_idx is not None and table_idx is not None:
            assert view_idx < table_idx


class TestFindExpiredDrs:
    def test_query_criteria(self) -> None:
        db, dr, *_ = _repos()
        find_expired_drs(db, dr)
        q = db.sql.call_args[0][0]
        assert "ACTIVE" in q and "CLEANUP_IN_PROGRESS" in q
