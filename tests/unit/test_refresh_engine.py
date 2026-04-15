"""Unit tests for devmirror.refresh.refresh_engine."""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest

from devmirror.config.schema import DataRevision
from devmirror.control.audit import AuditRepository
from devmirror.control.control_table import DrObjectRepository, DRRepository, ObjectStatus
from devmirror.refresh.refresh_engine import (
    RefreshError,
    _filter_objects,
    _generate_object_sql,
    refresh_dr,
)

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

_FUTURE = (date.today() + timedelta(days=30)).isoformat()


def _dr(status="ACTIVE", exp=None):
    return {"dr_id": "DR-100", "status": status, "expiration_date": exp or _FUTURE, "last_refreshed_at": None}


def _obj(src="prod_cat.s1.t1", tgt="dev_cat.dr_100_s1.t1", strategy="shallow_clone", status="PROVISIONED", env="dev"):
    return {"dr_id": "DR-100", "source_fqn": src, "target_fqn": tgt, "target_environment": env,
            "object_type": "view" if strategy == "view" else "table",
            "access_mode": "READ_ONLY", "clone_strategy": strategy,
            "clone_revision_mode": "latest", "clone_revision_value": None, "status": status}


def _repos(dr_row=None, obj_rows=None):
    db = MagicMock()
    db.sql_exec = MagicMock()
    db.sql = MagicMock(return_value=[])
    dr_repo = MagicMock(spec=DRRepository)
    dr_repo.get = MagicMock(return_value=dr_row if dr_row is not None else _dr())
    dr_repo.table_fqn = "ctl.admin.devmirror_development_requests"
    obj_repo = MagicMock(spec=DrObjectRepository)
    obj_repo.list_by_dr_id = MagicMock(return_value=obj_rows if obj_rows is not None else [])
    obj_repo.update_object_status = MagicMock()
    audit_repo = MagicMock(spec=AuditRepository)
    audit_repo.append = MagicMock()
    return db, dr_repo, obj_repo, audit_repo


def _refresh(mode="incremental", obj_rows=None, dr_row=None, **kw):
    db, dr_repo, obj_repo, audit_repo = _repos(dr_row=dr_row, obj_rows=obj_rows)
    result = refresh_dr("DR-100", mode, db_client=db, dr_repo=dr_repo,
                        obj_repo=obj_repo, audit_repo=audit_repo, max_parallel=1, **kw)
    return result, db, obj_repo, audit_repo


# ------------------------------------------------------------------
# SQL generation
# ------------------------------------------------------------------

class TestGenerateObjectSql:
    @pytest.mark.parametrize("strategy,full,keyword", [
        ("shallow_clone", False, "CREATE OR REPLACE TABLE"),
        ("deep_clone", False, "CREATE OR REPLACE TABLE"),
        ("view", False, "CREATE OR REPLACE VIEW"),
        ("schema_only", False, "TRUNCATE TABLE"),
        ("shallow_clone", True, "DROP TABLE IF EXISTS"),
        ("view", True, "DROP VIEW IF EXISTS"),
    ])
    def test_sql_keywords(self, strategy, full, keyword) -> None:
        src = "prod_cat.s1.v1" if strategy == "view" else "prod_cat.s1.t1"
        tgt = "dev_cat.dr_1_s1.v1" if strategy == "view" else "dev_cat.dr_1_s1.t1"
        assert keyword in "; ".join(_generate_object_sql(src, tgt, strategy, full_refresh=full))

    def test_incremental_single_stmt(self) -> None:
        assert len(_generate_object_sql("a.b.c", "d.e.f", "shallow_clone")) == 1

    def test_full_two_stmts(self) -> None:
        assert len(_generate_object_sql("a.b.c", "d.e.f", "shallow_clone", full_refresh=True)) == 2

    def test_version_revision(self) -> None:
        stmts = _generate_object_sql("a.b.c", "d.e.f", "shallow_clone", DataRevision(mode="version", version=42))
        assert "VERSION AS OF 42" in stmts[0]

    def test_unknown_strategy(self) -> None:
        with pytest.raises(Exception, match="Unknown"):
            _generate_object_sql("a.b.c", "d.e.f", "invalid")


# ------------------------------------------------------------------
# Filter
# ------------------------------------------------------------------

class TestFilterObjects:
    def test_full_excludes_dropped(self) -> None:
        rows = [_obj(), _obj(src="a.b.d", tgt="d.e.d", status="DROPPED")]
        assert len(_filter_objects(rows, "full")) == 1

    def test_incremental_only_clones(self) -> None:
        rows = [_obj(), _obj(src="a.b.v", tgt="d.e.v", strategy="view")]
        assert len(_filter_objects(rows, "incremental")) == 1

    def test_selective(self) -> None:
        rows = [_obj(src="a.b.c"), _obj(src="a.b.d", tgt="d.e.d")]
        assert len(_filter_objects(rows, "selective", ["a.b.d"])) == 1

    @pytest.mark.parametrize("fqns", [[], None])
    def test_selective_empty(self, fqns) -> None:
        assert _filter_objects([_obj()], "selective", fqns) == []


# ------------------------------------------------------------------
# refresh_dr orchestration
# ------------------------------------------------------------------

class TestRefreshDr:
    @pytest.mark.parametrize("mode", ["incremental", "full"])
    def test_success(self, mode) -> None:
        objs = [_obj()] if mode == "incremental" else [_obj(), _obj(src="a.b.v", tgt="d.e.v", strategy="view")]
        result, _db, _obj_repo, _audit = _refresh(mode=mode, obj_rows=objs)
        assert len(result.objects_succeeded) == len(objs)
        assert result.audit_status == "SUCCESS"

    def test_selective(self) -> None:
        result, *_ = _refresh("selective", obj_rows=[_obj(src="a.b.c"), _obj(src="a.b.d", tgt="d.e.d")],
                               selected_fqns=["a.b.d"])
        assert len(result.objects_succeeded) == 1

    def test_version_revision(self) -> None:
        _result, db, *_ = _refresh(obj_rows=[_obj()], data_revision=DataRevision(mode="version", version=42))
        assert "VERSION AS OF 42" in db.sql_exec.call_args_list[0][0][0]

    def test_not_found(self) -> None:
        with pytest.raises(RefreshError, match="not found"):
            db, dr, obj, aud = _repos(dr_row=None)
            dr.get.return_value = None
            refresh_dr("DR-999", "incremental", db_client=db, dr_repo=dr, obj_repo=obj, audit_repo=aud)

    def test_inactive(self) -> None:
        with pytest.raises(RefreshError, match="not active"):
            _refresh(dr_row=_dr(status="FAILED"))

    def test_no_objects(self) -> None:
        result, _, _, audit = _refresh(obj_rows=[])
        assert len(result.objects_succeeded) == 0
        audit.append.assert_called_once()

    def test_partial_failure(self) -> None:
        db, dr, obj, aud = _repos(obj_rows=[_obj(src="a.b.ok"), _obj(src="a.b.fail", tgt="d.e.fail")])
        db.sql_exec = MagicMock(side_effect=lambda s, **kw: (_ for _ in ()).throw(RuntimeError("fail")) if "a.b.fail" in s else None)
        result = refresh_dr("DR-100", "incremental", db_client=db, dr_repo=dr, obj_repo=obj, audit_repo=aud, max_parallel=1)
        assert result.audit_status == "PARTIAL_SUCCESS"
        assert len(result.objects_failed) == 1

    def test_all_failed(self) -> None:
        db, dr, obj, aud = _repos(obj_rows=[_obj()])
        db.sql_exec = MagicMock(side_effect=RuntimeError("fail"))
        result = refresh_dr("DR-100", "incremental", db_client=db, dr_repo=dr, obj_repo=obj, audit_repo=aud, max_parallel=1)
        assert result.audit_status == "FAILED"

    def test_updates_object_status(self) -> None:
        _result, _, obj_repo, _ = _refresh(obj_rows=[_obj()])
        obj_repo.update_object_status.assert_called_once()
        assert obj_repo.update_object_status.call_args.kwargs["new_status"] == ObjectStatus.PROVISIONED

    def test_incremental_skips_views(self) -> None:
        result, *_ = _refresh(obj_rows=[_obj(), _obj(src="a.b.v", tgt="d.e.v", strategy="view")])
        assert len(result.objects_succeeded) == 1
        assert result.objects_succeeded[0].strategy == "shallow_clone"
