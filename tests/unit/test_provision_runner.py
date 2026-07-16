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
                uat_users=["uat@company.com"] if qa else None,
            ),
            lifecycle=Lifecycle(expiration_date="2099-12-31"),
        ),
    )


def _manifest(objects=None, schemas=None):
    if objects is None:
        objects = [
            {"fqn": "prod_analytics_p.customers.profile", "type": "table", "access_mode": "READ_ONLY", "estimated_size_gb": 10.0},
            {"fqn": "prod_analytics_p.customers.churn_scores", "type": "table", "access_mode": "READ_WRITE", "estimated_size_gb": 2.0},
        ]
    if schemas is None:
        schemas = ["prod_analytics_p.customers"]
    return {"scan_result": {
        "dr_id": "DR-1042", "scanned_at": "2026-04-13T10:00:00Z",
        "streams_scanned": [{"name": "test_stream", "workflow_id": "123"}],
        "objects": objects, "schemas_required": schemas,
        "total_objects": len(objects), "review_required": False,
    }}


from .conftest import make_mock_db


def _mock_db() -> MagicMock:
    m = make_mock_db()
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
        assert r["source_fqn"] == "prod_analytics_p.customers.profile"
        assert r["target_fqn"] == "prod_analytics_n.dr_1042_customers.profile"
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
        m = _manifest(objects=[{"fqn": "prod_analytics_p.shared.v", "type": "view", "access_mode": "READ_ONLY"}],
                      schemas=["prod_analytics_p.shared"])
        assert _build_object_rows(_cfg(), m, "dev")[0]["clone_strategy"] == "view"

    def test_manifest_strategy_override(self) -> None:
        m = _manifest(objects=[{"fqn": "prod_analytics_p.customers.profile", "type": "table",
                                "access_mode": "READ_ONLY", "clone_strategy": "deep_clone"}])
        assert _build_object_rows(_cfg(), m, "dev")[0]["clone_strategy"] == "deep_clone"

    def test_mixed_base_catalogs_route_per_object(self) -> None:
        # Two source catalogs with different bases (LH SDLC suffix scheme).
        # Each object's clone must land in *its own* base catalog with the
        # env's suffix attached -- not all in a single shared target.
        m = _manifest(
            objects=[
                {"fqn": "odp_adw_ancillaries_p.sales.bookings", "type": "table",
                 "access_mode": "READ_ONLY"},
                {"fqn": "odp_adw_offers_p.catalog.promos", "type": "table",
                 "access_mode": "READ_ONLY"},
            ],
            schemas=["odp_adw_ancillaries_p.sales", "odp_adw_offers_p.catalog"],
        )

        dev_rows = _build_object_rows(_cfg(), m, "dev")
        dev_targets = {r["target_fqn"] for r in dev_rows}
        assert dev_targets == {
            "odp_adw_ancillaries_n.dr_1042_sales.bookings",
            "odp_adw_offers_n.dr_1042_catalog.promos",
        }

        qa_rows = _build_object_rows(_cfg(qa=True), m, "qa")
        qa_targets = {r["target_fqn"] for r in qa_rows}
        assert qa_targets == {
            "odp_adw_ancillaries_i.qa_1042_sales.bookings",
            "odp_adw_offers_i.qa_1042_catalog.promos",
        }


# ------------------------------------------------------------------
# _get_schemas_for_env
# ------------------------------------------------------------------

class TestGetSchemasForEnv:
    def test_dev_schemas(self) -> None:
        assert _get_schemas_for_env(_cfg(), _manifest(), "dev") == ["prod_analytics_n.dr_1042_customers"]

    def test_qa_schemas(self) -> None:
        assert _get_schemas_for_env(_cfg(qa=True), _manifest(), "qa") == ["prod_analytics_i.qa_1042_customers"]


# ------------------------------------------------------------------
# Per-DR import schemas + Volume rows
# ------------------------------------------------------------------

from devmirror.provision.runner import (  # noqa: E402
    _get_import_schemas_for_env,
    _get_import_volume_rows_for_env,
)


class TestImportSchemasForEnv:
    """One import schema per source catalog per env, gated by the
    DEVMIRROR_IMPORT_SCHEMA_SUFFIX env var (so deployments that don't
    want the feature pay zero cost)."""

    def test_disabled_by_default(self, monkeypatch) -> None:
        monkeypatch.delenv("DEVMIRROR_IMPORT_SCHEMA_SUFFIX", raising=False)
        assert _get_import_schemas_for_env(_manifest(), "DR-1042", "dev") == []

    def test_empty_suffix_disables(self, monkeypatch) -> None:
        monkeypatch.setenv("DEVMIRROR_IMPORT_SCHEMA_SUFFIX", "")
        assert _get_import_schemas_for_env(_manifest(), "DR-1042", "dev") == []

    def test_single_catalog(self, monkeypatch) -> None:
        monkeypatch.setenv("DEVMIRROR_IMPORT_SCHEMA_SUFFIX", "import_main")
        result = _get_import_schemas_for_env(_manifest(), "DR-1042", "dev")
        assert result == ["prod_analytics_n.dr_1042_import_main"]

    def test_qa_env(self, monkeypatch) -> None:
        monkeypatch.setenv("DEVMIRROR_IMPORT_SCHEMA_SUFFIX", "import_main")
        # Note: QA catalog suffix defaults to `_i` in the resolver, so the
        # target catalog reflects that default.  LH overrides it to `_n`
        # via DEVMIRROR_QA_CATALOG_SUFFIX in app.yaml.
        result = _get_import_schemas_for_env(_manifest(), "DR-1042", "qa")
        assert result == ["prod_analytics_i.qa_1042_import_main"]

    def test_multiple_source_catalogs(self, monkeypatch) -> None:
        # Two distinct base catalogs in one manifest -> two import schemas.
        monkeypatch.setenv("DEVMIRROR_IMPORT_SCHEMA_SUFFIX", "import_main")
        m = _manifest(
            objects=[
                {"fqn": "odp_adw_ancillaries_p.sales.bookings", "type": "table",
                 "access_mode": "READ_ONLY"},
                {"fqn": "odp_adw_offers_p.catalog.promos", "type": "table",
                 "access_mode": "READ_ONLY"},
            ],
            schemas=["odp_adw_ancillaries_p.sales", "odp_adw_offers_p.catalog"],
        )
        result = _get_import_schemas_for_env(m, "DR-1042", "dev")
        assert set(result) == {
            "odp_adw_ancillaries_n.dr_1042_import_main",
            "odp_adw_offers_n.dr_1042_import_main",
        }


class TestImportVolumeRowsForEnv:
    """One volume row per source catalog per env, gated by both env vars.
    Volume rows ride through the same provisioning + cleanup pipeline as
    regular clones via object_type=volume / strategy=create_volume."""

    def test_disabled_when_either_unset(self, monkeypatch) -> None:
        monkeypatch.delenv("DEVMIRROR_IMPORT_SCHEMA_SUFFIX", raising=False)
        monkeypatch.delenv("DEVMIRROR_IMPORT_VOLUME_NAME", raising=False)
        assert _get_import_volume_rows_for_env(_cfg(), _manifest(), "dev") == []

        monkeypatch.setenv("DEVMIRROR_IMPORT_SCHEMA_SUFFIX", "import_main")
        # Suffix set but volume name still empty -> still disabled.
        assert _get_import_volume_rows_for_env(_cfg(), _manifest(), "dev") == []

    def test_row_shape(self, monkeypatch) -> None:
        monkeypatch.setenv("DEVMIRROR_IMPORT_SCHEMA_SUFFIX", "import_main")
        monkeypatch.setenv("DEVMIRROR_IMPORT_VOLUME_NAME", "main_volume")
        rows = _get_import_volume_rows_for_env(_cfg(), _manifest(), "dev")
        assert len(rows) == 1
        r = rows[0]
        assert r["target_fqn"] == "prod_analytics_n.dr_1042_import_main.main_volume"
        assert r["object_type"] == "volume"
        assert r["clone_strategy"] == "create_volume"
        # source_fqn stores the source catalog for audit -- not used by
        # the clone path (which short-circuits for create_volume).
        assert r["source_fqn"] == "prod_analytics_p"

    def test_multi_catalog(self, monkeypatch) -> None:
        monkeypatch.setenv("DEVMIRROR_IMPORT_SCHEMA_SUFFIX", "import_main")
        monkeypatch.setenv("DEVMIRROR_IMPORT_VOLUME_NAME", "main_volume")
        m = _manifest(
            objects=[
                {"fqn": "odp_adw_ancillaries_p.sales.bookings", "type": "table",
                 "access_mode": "READ_ONLY"},
                {"fqn": "odp_adw_offers_p.catalog.promos", "type": "table",
                 "access_mode": "READ_ONLY"},
            ],
            schemas=["odp_adw_ancillaries_p.sales", "odp_adw_offers_p.catalog"],
        )
        rows = _get_import_volume_rows_for_env(_cfg(), m, "dev")
        targets = {r["target_fqn"] for r in rows}
        assert targets == {
            "odp_adw_ancillaries_n.dr_1042_import_main.main_volume",
            "odp_adw_offers_n.dr_1042_import_main.main_volume",
        }


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
        rows = acc.bulk_insert.call_args[1]["rows"]
        envs = {r["environment"] for r in rows}
        assert envs == {"dev", "qa"}
        # Grant matrix:
        #   developer (dev@company.com)   -> RW on BOTH dev and qa
        #   UAT user  (uat@company.com)   -> RO on BOTH dev and qa
        by_principal = {
            (r["user_email"], r["environment"]): r["access_level"]
            for r in rows
        }
        assert by_principal[("dev@company.com", "dev")] == "READ_WRITE"
        assert by_principal[("dev@company.com", "qa")] == "READ_WRITE"
        assert by_principal[("uat@company.com", "dev")] == "READ_ONLY"
        assert by_principal[("uat@company.com", "qa")] == "READ_ONLY"

    def test_uat_users_get_select_only_on_every_env(self) -> None:
        """End-to-end grant matrix at the UC API level:

        - Developer gets MODIFY on dev AND qa schemas.
        - UAT user gets SELECT but NOT MODIFY on BOTH dev and qa schemas.
        """
        from databricks.sdk.service.catalog import Privilege, SecurableType

        from devmirror.provision.access_manager import (
            _principal_cache,
            _principal_cache_lock,
        )
        with _principal_cache_lock:
            _principal_cache.clear()

        db = _mock_db()
        found = MagicMock()
        db.client.users.list.return_value = [found]
        db.client.groups.list.return_value = [found]

        (result, *_) = _provision(config=_cfg(qa=True), db=db)
        assert result.final_status == "ACTIVE"

        # Aggregate schema grants by (target, principal) -> set of privileges.
        schema_grants: dict[tuple[str, str], set] = {}
        for call in db.grant.call_args_list:
            sec_type, fqn, principal, privs = call.args
            if sec_type == SecurableType.SCHEMA:
                schema_grants.setdefault((fqn, principal), set()).update(privs)

        dev_schema = "prod_analytics_n.dr_1042_customers"
        qa_schema = "prod_analytics_i.qa_1042_customers"
        # Developer: MODIFY on both envs.
        assert Privilege.MODIFY in schema_grants.get(
            (dev_schema, "dev@company.com"), set(),
        )
        assert Privilege.MODIFY in schema_grants.get(
            (qa_schema, "dev@company.com"), set(),
        )
        # UAT user: SELECT on both envs, MODIFY on neither.
        assert Privilege.SELECT in schema_grants.get(
            (dev_schema, "uat@company.com"), set(),
        )
        assert Privilege.SELECT in schema_grants.get(
            (qa_schema, "uat@company.com"), set(),
        )
        assert Privilege.MODIFY not in schema_grants.get(
            (dev_schema, "uat@company.com"), set(),
        )
        assert Privilege.MODIFY not in schema_grants.get(
            (qa_schema, "uat@company.com"), set(),
        )

    def test_uat_user_also_developer_not_duplicated(self) -> None:
        """If alice@ is in BOTH developers and uat_users, the UAT-user RO
        pass skips her on every env -- she already has RW from the
        developer pass.  Verifies the principal dedup contract is the same
        as the legacy qa_users behaviour, just applied to all envs."""
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
        from devmirror.provision.access_manager import (
            _principal_cache,
            _principal_cache_lock,
        )
        with _principal_cache_lock:
            _principal_cache.clear()

        cfg = DevMirrorConfig(
            version="1.0",
            development_request=DevelopmentRequest(
                dr_id="DR-1042", description="Test DR",
                streams=[StreamRef(name="test_stream")],
                environments=Environments(
                    dev=EnvironmentDev(),
                    qa=EnvironmentQA(enabled=True),
                ),
                data_revision=DataRevision(mode="latest"),
                access=Access(
                    developers=["alice@company.com"],
                    uat_users=["alice@company.com"],   # same person in both
                ),
                lifecycle=Lifecycle(expiration_date="2099-12-31"),
            ),
        )

        db = _mock_db()
        found = MagicMock()
        db.client.users.list.return_value = [found]
        db.client.groups.list.return_value = [found]

        (result, _dr, _obj, acc, _aud) = _provision(config=cfg, db=db)
        assert result.final_status == "ACTIVE"

        # access_rows must NOT duplicate alice in either env: exactly one
        # row per env and both are READ_WRITE (developer pass wins).
        rows = acc.bulk_insert.call_args[1]["rows"]
        for env in ("dev", "qa"):
            alice_rows = [
                r for r in rows
                if r["user_email"] == "alice@company.com"
                and r["environment"] == env
            ]
            assert len(alice_rows) == 1
            assert alice_rows[0]["access_level"] == "READ_WRITE"

    def test_volume_grants_dev_rw_and_uat_readonly(self, monkeypatch) -> None:
        # Enable the import-schema feature and verify volume grants:
        #   dev volume -> developer  RW (READ_VOLUME + WRITE_VOLUME)
        #   dev volume -> UAT user   RO (READ_VOLUME only)
        #   qa volume  -> developer  RW
        #   qa volume  -> UAT user   RO
        from databricks.sdk.service.catalog import Privilege, SecurableType

        from devmirror.provision.access_manager import (
            _principal_cache,
            _principal_cache_lock,
        )
        with _principal_cache_lock:
            _principal_cache.clear()

        monkeypatch.setenv("DEVMIRROR_IMPORT_SCHEMA_SUFFIX", "import_main")
        monkeypatch.setenv("DEVMIRROR_IMPORT_VOLUME_NAME", "main_volume")

        db = _mock_db()
        # Wire SCIM so apply_volume_grants accepts the principals.
        found = MagicMock()
        db.client.users.list.return_value = [found]
        db.client.groups.list.return_value = [found]

        (result, *_) = _provision(config=_cfg(qa=True), db=db)
        assert result.final_status == "ACTIVE"

        # Find every grant call against SecurableType.VOLUME and group
        # by (target_fqn, principal) -> set of privileges.
        vol_grants: dict[tuple[str, str], set] = {}
        for call in db.grant.call_args_list:
            sec_type, fqn, principal, privs = call.args
            if sec_type == SecurableType.VOLUME:
                vol_grants.setdefault((fqn, principal), set()).update(privs)

        dev_vol = "prod_analytics_n.dr_1042_import_main.main_volume"
        qa_vol = "prod_analytics_i.qa_1042_import_main.main_volume"
        # Developer: READ + WRITE on both env volumes.
        assert vol_grants[(dev_vol, "dev@company.com")] == {
            Privilege.READ_VOLUME, Privilege.WRITE_VOLUME,
        }
        assert vol_grants[(qa_vol, "dev@company.com")] == {
            Privilege.READ_VOLUME, Privilege.WRITE_VOLUME,
        }
        # UAT user: READ only on both env volumes.  (Default QA catalog
        # suffix is `_i`; LH overrides to `_n` via env in production but
        # we don't set that here, so the default applies.)
        assert vol_grants[(dev_vol, "uat@company.com")] == {Privilege.READ_VOLUME}
        assert vol_grants[(qa_vol, "uat@company.com")] == {Privilege.READ_VOLUME}
        assert Privilege.WRITE_VOLUME not in vol_grants[(dev_vol, "uat@company.com")]
        assert Privilege.WRITE_VOLUME not in vol_grants[(qa_vol, "uat@company.com")]

    def test_empty_manifest(self) -> None:
        (result, *_) = _provision(manifest=_manifest(objects=[], schemas=[]))
        assert result.final_status == "ACTIVE"
        assert len(result.objects_succeeded) == 0

    def test_failed_dr_recovers_to_active(self) -> None:
        # Re-provisioning a DR currently stuck at FAILED must land at ACTIVE
        # via force_status (no CAS) -- guards against the silent-no-op
        # regression where the row stayed at FAILED forever.
        dr, obj, acc, aud = MagicMock(), MagicMock(), MagicMock(), MagicMock()
        # The first dr.get (existing_dr fetch) should see FAILED; the
        # read-back at the end should see ACTIVE.
        dr.get.side_effect = [
            {"dr_id": "DR-1042", "status": "FAILED"},
            {"dr_id": "DR-1042", "status": "ACTIVE"},
        ]
        result = provision_dr(_cfg(), _manifest(),
                              db_client=_mock_db(), dr_repo=dr, obj_repo=obj,
                              access_repo=acc, audit_repo=aud, force_replace=True)
        assert result.final_status == "ACTIVE"
        statuses = [c.kwargs["new_status"].value for c in dr.force_status.call_args_list]
        assert "PROVISIONING" in statuses
        assert statuses[-1] == "ACTIVE"
        # update_status (CAS-gated) must NOT be called from inside the
        # runner -- that's the whole point of switching to force_status.
        dr.update_status.assert_not_called()
        # Critically: re-provision must NOT call insert -- Delta has no PK
        # so a duplicate INSERT would silently create a second row for the
        # same dr_id, which the UI then renders as a duplicate tile.
        dr.insert.assert_not_called()

    def test_reprovision_active_dr_does_not_duplicate_row(self) -> None:
        # Re-provisioning an already-ACTIVE DR must update in place
        # (force_status), never INSERT.  Without this guard the row would
        # duplicate every time someone clicked "re-provision".
        dr, obj, acc, aud = MagicMock(), MagicMock(), MagicMock(), MagicMock()
        dr.get.side_effect = [
            {"dr_id": "DR-1042", "status": "ACTIVE"},   # existing row
            {"dr_id": "DR-1042", "status": "ACTIVE"},   # post-run read-back
        ]
        result = provision_dr(_cfg(), _manifest(),
                              db_client=_mock_db(), dr_repo=dr, obj_repo=obj,
                              access_repo=acc, audit_repo=aud, force_replace=True)
        assert result.final_status == "ACTIVE"
        dr.insert.assert_not_called()
        statuses = [c.kwargs["new_status"].value for c in dr.force_status.call_args_list]
        assert statuses[0] == "PROVISIONING"   # pre-run normalize
        assert statuses[-1] == "ACTIVE"        # post-run finalize

    def test_first_time_provision_inserts(self) -> None:
        # Brand-new DR (existing_dr is None): insert path runs, force_status
        # is called only once at the end (final ACTIVE/FAILED).
        dr, obj, acc, aud = MagicMock(), MagicMock(), MagicMock(), MagicMock()
        dr.get.side_effect = [None, {"dr_id": "DR-1042", "status": "ACTIVE"}]
        # On a brand-new DR there are no v1 rows to scan for orphans.
        obj.list_by_dr_id.return_value = []
        result = provision_dr(_cfg(), _manifest(),
                              db_client=_mock_db(), dr_repo=dr, obj_repo=obj,
                              access_repo=acc, audit_repo=aud, force_replace=True)
        assert result.final_status == "ACTIVE"
        dr.insert.assert_called_once()
        # No pre-run force_status -- only the post-run one.
        statuses = [c.kwargs["new_status"].value for c in dr.force_status.call_args_list]
        assert statuses == ["ACTIVE"]

    def test_reprovision_drops_v1_orphans(self) -> None:
        # v1 had a row for prod_analytics_p.customers.churn_scores cloned
        # into prod_analytics_n.dr_1042_customers.churn_scores.  v2's
        # manifest (the _manifest() default) ALSO includes that target,
        # so it stays.  But v1 had ALSO cloned prod_analytics_p.old.gone
        # which v2 no longer mentions -- that one must be dropped from
        # UC during the re-provision, otherwise cleanup_dr can never
        # find it (the row gets wiped by delete_by_dr_id).
        dr, obj, acc, aud = MagicMock(), MagicMock(), MagicMock(), MagicMock()
        dr.get.side_effect = [
            {"dr_id": "DR-1042", "status": "ACTIVE"},     # existing_dr
            {"dr_id": "DR-1042", "status": "ACTIVE"},     # post-run read-back
        ]
        # v1 rows: one target that v2 also has, one that v2 dropped.
        obj.list_by_dr_id.return_value = [
            {"dr_id": "DR-1042", "target_fqn": "prod_analytics_n.dr_1042_customers.profile",
             "object_type": "table"},
            {"dr_id": "DR-1042", "target_fqn": "prod_analytics_n.dr_1042_old.gone",
             "object_type": "table"},
            {"dr_id": "DR-1042", "target_fqn": "prod_analytics_n.dr_1042_import_main.main_volume",
             "object_type": "volume"},
        ]
        db = _mock_db()
        result = provision_dr(_cfg(), _manifest(),
                              db_client=db, dr_repo=dr, obj_repo=obj,
                              access_repo=acc, audit_repo=aud,
                              force_replace=True)
        assert result.final_status == "ACTIVE"

        # The v1 orphan table must have been dropped via delete_table.
        delete_table_calls = [c.args[0] for c in db.delete_table.call_args_list]
        assert "prod_analytics_n.dr_1042_old.gone" in delete_table_calls
        # The v1 volume orphan must have gone through DROP VOLUME SQL.
        sql_exec_calls = [c.args[0] for c in db.sql_exec.call_args_list]
        assert any(
            "DROP VOLUME IF EXISTS prod_analytics_n.dr_1042_import_main.main_volume" in s
            for s in sql_exec_calls
        )
        # The v1 target that v2 still has must NOT have been pre-dropped --
        # CREATE OR REPLACE handles it during the clone pass.
        assert "prod_analytics_n.dr_1042_customers.profile" not in delete_table_calls

    def test_readback_mismatch_logs_critical(self, caplog) -> None:
        # Simulate the case where force_status "succeeds" (no exception) but
        # the row stayed at the wrong value (the historical silent-no-op).
        # The read-back should log CRITICAL.
        import logging
        dr, obj, acc, aud = MagicMock(), MagicMock(), MagicMock(), MagicMock()
        # First dr.get is existing_dr lookup (None means "new row"); second
        # is the read-back, which returns FAILED instead of the expected
        # ACTIVE -> CRITICAL.
        dr.get.side_effect = [None, {"dr_id": "DR-1042", "status": "FAILED"}]
        with caplog.at_level(logging.CRITICAL, logger="devmirror.provision.runner"):
            provision_dr(_cfg(), _manifest(),
                         db_client=_mock_db(), dr_repo=dr, obj_repo=obj,
                         access_repo=acc, audit_repo=aud, force_replace=True)
        critical_msgs = [r.message for r in caplog.records if r.levelno == logging.CRITICAL]
        assert any("read-back mismatch" in m for m in critical_msgs)


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
