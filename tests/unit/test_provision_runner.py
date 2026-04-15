"""Tests for devmirror.provision.runner."""

from __future__ import annotations

import threading
import time
from unittest.mock import MagicMock

import pytest

from devmirror.config.schema import (
    Access,
    DataRevision,
    DevelopmentRequest,
    DevMirrorConfig,
    EnvironmentDev,
    EnvironmentQA,
    Environments,
    Lifecycle,
    StreamRef,
)
from devmirror.provision.object_cloner import CloneResult
from devmirror.provision.runner import (
    ProvisionResult,
    SchemaCollisionError,
    _build_object_rows,
    _get_schemas_for_env,
    provision_dr,
)
from devmirror.utils import TaskResult, run_bounded

# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _cfg(dr_id="DR-1042", qa=False, rev_mode="latest", rev_ver=None, rev_ts=None):
    return DevMirrorConfig(
        version="1.0",
        development_request=DevelopmentRequest(
            dr_id=dr_id, description="Test DR",
            streams=[StreamRef(name="test_stream")],
            environments=Environments(
                dev=EnvironmentDev(),
                qa=EnvironmentQA(enabled=True) if qa else None,
            ),
            data_revision=DataRevision(mode=rev_mode, version=rev_ver, timestamp=rev_ts),
            access=Access(
                developers=["dev@company.com"],
                qa_users=["qa@company.com"] if qa else None,
            ),
            lifecycle=Lifecycle(expiration_date="2099-12-31"),
        ),
    )


def _manifest(objects=None, schemas=None):
    if objects is None:
        objects = [
            {"fqn": "prod_analytics.customers.profile", "type": "table", "access_mode": "READ_ONLY", "estimated_size_gb": 10.0},
            {"fqn": "prod_analytics.customers.churn_scores", "type": "table", "access_mode": "READ_WRITE", "estimated_size_gb": 2.0},
        ]
    if schemas is None:
        schemas = ["prod_analytics.customers"]
    return {"scan_result": {
        "dr_id": "DR-1042", "scanned_at": "2026-04-13T10:00:00Z",
        "streams_scanned": [{"name": "test_stream", "workflow_id": "123"}],
        "objects": objects, "schemas_required": schemas,
        "total_objects": len(objects), "review_required": False,
    }}


def _mock_db() -> MagicMock:
    m = MagicMock()
    m.sql_exec = MagicMock()
    m.sql = MagicMock(return_value=[])
    m.create_schema = MagicMock()
    m.grant = MagicMock()
    m.revoke = MagicMock()
    m.delete_table = MagicMock()
    m.delete_schema = MagicMock()
    return m


def _mock_repos():
    return MagicMock(), MagicMock(), MagicMock(), MagicMock()


def _provision(config=None, manifest=None, db=None, dr_return=None, **kw):
    config = config or _cfg()
    manifest = manifest or _manifest()
    db = db or _mock_db()
    dr, obj, acc, aud = _mock_repos()
    dr.get.return_value = dr_return
    return provision_dr(config, manifest, db_client=db, dr_repo=dr, obj_repo=obj,
                        access_repo=acc, audit_repo=aud, **kw), dr, obj, acc, aud


# ------------------------------------------------------------------
# _build_object_rows
# ------------------------------------------------------------------

class TestBuildObjectRows:
    def test_basic_dev(self) -> None:
        rows = _build_object_rows(_cfg(), _manifest(), "dev")
        assert len(rows) == 2
        r = rows[0]
        assert r["dr_id"] == "DR-1042"
        assert r["source_fqn"] == "prod_analytics.customers.profile"
        assert r["target_fqn"] == "dev_analytics.dr_1042_customers.profile"
        assert r["clone_strategy"] == "shallow_clone"
        assert r["clone_revision_mode"] == "latest"

    @pytest.mark.parametrize("rev_mode,rev_kw,expected_val", [
        ("version", {"rev_ver": 42}, "42"),
        ("timestamp", {"rev_ts": "2026-04-01T00:00:00Z"}, "2026-04-01T00:00:00Z"),
    ])
    def test_revision_modes(self, rev_mode, rev_kw, expected_val) -> None:
        rows = _build_object_rows(_cfg(rev_mode=rev_mode, **rev_kw), _manifest(), "dev")
        assert rows[0]["clone_revision_mode"] == rev_mode
        assert rows[0]["clone_revision_value"] == expected_val

    def test_view_gets_view_strategy(self) -> None:
        m = _manifest(objects=[{"fqn": "prod_analytics.shared.v", "type": "view", "access_mode": "READ_ONLY"}],
                      schemas=["prod_analytics.shared"])
        assert _build_object_rows(_cfg(), m, "dev")[0]["clone_strategy"] == "view"

    def test_manifest_strategy_override(self) -> None:
        m = _manifest(objects=[{"fqn": "prod_analytics.customers.profile", "type": "table",
                                "access_mode": "READ_ONLY", "clone_strategy": "deep_clone"}])
        assert _build_object_rows(_cfg(), m, "dev")[0]["clone_strategy"] == "deep_clone"


# ------------------------------------------------------------------
# _get_schemas_for_env
# ------------------------------------------------------------------

class TestGetSchemasForEnv:
    def test_dev_schemas(self) -> None:
        assert _get_schemas_for_env(_cfg(), _manifest(), "dev") == ["dev_analytics.dr_1042_customers"]

    def test_qa_schemas(self) -> None:
        assert _get_schemas_for_env(_cfg(qa=True), _manifest(), "qa") == ["dev_analytics.qa_1042_customers"]


# ------------------------------------------------------------------
# provision_dr orchestration
# ------------------------------------------------------------------

class TestProvisionDr:
    def test_all_succeed(self) -> None:
        (result, dr, _obj, _acc, aud) = _provision()
        assert result.final_status == "ACTIVE"
        assert len(result.objects_succeeded) == 2
        dr.insert.assert_called_once()
        assert aud.append.call_count == 2

    def test_partial_failure(self) -> None:
        db = _mock_db()
        db.sql_exec.side_effect = lambda sql: (_ for _ in ()).throw(Exception("fail")) if "churn_scores" in sql and "SHALLOW CLONE" in sql else None
        (result, *_) = _provision(db=db, max_parallel=1)
        assert result.final_status == "ACTIVE"
        assert result.is_partial_success
        assert len(result.objects_failed) == 1

    def test_all_fail(self) -> None:
        db = _mock_db()
        db.sql_exec.side_effect = lambda sql: (_ for _ in ()).throw(Exception("fail")) if "SHALLOW CLONE" in sql else None
        (result, *_) = _provision(db=db, max_parallel=1)
        assert result.final_status == "FAILED"
        assert result.all_objects_failed

    def test_with_qa(self) -> None:
        (result, _, _, acc, _) = _provision(config=_cfg(qa=True))
        assert result.final_status == "ACTIVE"
        assert len(result.objects_succeeded) == 4
        envs = {r["environment"] for r in acc.bulk_insert.call_args[1]["rows"]}
        assert envs == {"dev", "qa"}

    def test_empty_manifest(self) -> None:
        (result, *_) = _provision(manifest=_manifest(objects=[], schemas=[]))
        assert result.final_status == "ACTIVE"
        assert len(result.objects_succeeded) == 0


# ------------------------------------------------------------------
# ProvisionResult
# ------------------------------------------------------------------

class TestProvisionResult:
    def test_partial_success_flag(self) -> None:
        r = ProvisionResult(dr_id="DR-1",
                            objects_succeeded=[CloneResult("a.b.c", "d.e.f", "shallow_clone", "", True)],
                            objects_failed=[CloneResult("g.h.i", "j.k.l", "shallow_clone", "", False, "err")])
        assert r.is_partial_success
        assert not r.all_objects_failed

    def test_all_failed_flag(self) -> None:
        r = ProvisionResult(dr_id="DR-1",
                            objects_failed=[CloneResult("a.b.c", "d.e.f", "shallow_clone", "", False, "err")])
        assert r.all_objects_failed


# ------------------------------------------------------------------
# Schema collision detection
# ------------------------------------------------------------------

class TestSchemaCollision:
    @pytest.mark.parametrize("status", ["ACTIVE", "EXPIRING_SOON"])
    def test_active_collision_with_review_raises(self, status) -> None:
        m = _manifest()
        m["scan_result"]["review_required"] = True
        db = _mock_db()
        dr, obj, acc, aud = _mock_repos()
        dr.get.return_value = {"dr_id": "DR-1042", "status": status}
        with pytest.raises(SchemaCollisionError):
            provision_dr(_cfg(), m, db_client=db, dr_repo=dr, obj_repo=obj, access_repo=acc, audit_repo=aud)

    def test_force_replace_proceeds(self) -> None:
        m = _manifest()
        m["scan_result"]["review_required"] = True
        (result, *_) = _provision(manifest=m, dr_return={"dr_id": "DR-1042", "status": "ACTIVE"}, force_replace=True)
        assert result.final_status == "ACTIVE"

    @pytest.mark.parametrize("dr_return", [None, {"dr_id": "DR-1042", "status": "CLEANED_UP"}])
    def test_non_active_proceeds(self, dr_return) -> None:
        m = _manifest()
        m["scan_result"]["review_required"] = True
        (result, *_) = _provision(manifest=m, dr_return=dr_return)
        assert result.final_status == "ACTIVE"


# ===========================================================================
# run_bounded / TaskResult tests (merged from test_concurrent.py)
# ===========================================================================


class TestRunBounded:
    """Tests for the ``run_bounded`` concurrency helper."""

    def test_empty_tasks_returns_empty(self) -> None:
        results = run_bounded([], max_workers=4)
        assert results == []

    def test_single_task_success(self) -> None:
        results = run_bounded([lambda: 42], max_workers=1)
        assert len(results) == 1
        assert results[0].success is True
        assert results[0].value == 42
        assert results[0].index == 0

    def test_multiple_tasks_preserve_order(self) -> None:
        tasks = [lambda i=i: i * 10 for i in range(5)]
        results = run_bounded(tasks, max_workers=3)
        assert len(results) == 5
        for i, r in enumerate(results):
            assert r.index == i
            assert r.success is True
            assert r.value == i * 10

    def test_task_failure_captured(self) -> None:
        def failing_task() -> None:
            raise ValueError("boom")

        results = run_bounded([failing_task], max_workers=1)
        assert len(results) == 1
        assert results[0].success is False
        assert "boom" in (results[0].error or "")

    def test_mixed_success_and_failure(self) -> None:
        def ok() -> str:
            return "ok"

        def fail() -> None:
            raise RuntimeError("fail")

        results = run_bounded([ok, fail, ok], max_workers=2)
        assert len(results) == 3
        assert results[0].success is True
        assert results[1].success is False
        assert results[2].success is True
        assert "fail" in (results[1].error or "")

    def test_max_workers_bounds_concurrency(self) -> None:
        max_workers = 2
        active = {"count": 0, "peak": 0}
        lock = threading.Lock()

        def tracked_task() -> None:
            with lock:
                active["count"] += 1
                if active["count"] > active["peak"]:
                    active["peak"] = active["count"]
            time.sleep(0.05)
            with lock:
                active["count"] -= 1

        tasks = [tracked_task for _ in range(6)]
        results = run_bounded(tasks, max_workers=max_workers)

        assert all(r.success for r in results)
        assert active["peak"] <= max_workers

    def test_return_values_typed(self) -> None:
        results = run_bounded([lambda: {"key": "value"}], max_workers=1)
        assert results[0].value == {"key": "value"}

    def test_task_result_dataclass(self) -> None:
        tr = TaskResult(index=0, value="hello", success=True, error=None)
        assert tr.index == 0
        assert tr.value == "hello"
        assert tr.success is True
        assert tr.error is None

    def test_workers_clamped_to_task_count(self) -> None:
        results = run_bounded([lambda: 1, lambda: 2], max_workers=100)
        assert len(results) == 2
        assert all(r.success for r in results)
