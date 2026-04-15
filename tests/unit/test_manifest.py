"""Unit tests for devmirror.scan.manifest."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

import yaml

if TYPE_CHECKING:
    from pathlib import Path

from devmirror.scan.dependency_classifier import ClassificationResult, ClassifiedObject
from devmirror.scan.manifest import build_manifest, read_manifest, write_manifest
from devmirror.scan.stream_resolver import ResolvedStream

# ------------------------------------------------------------------
# Sample data
# ------------------------------------------------------------------

_STREAMS = [ResolvedStream(name="customer_churn_daily", resource_type="job",
                           resource_id="1234567890",
                           task_keys=["ingest_raw", "transform_silver", "aggregate_gold"])]

_CLASS = ClassificationResult(
    objects=[
        ClassifiedObject(fqn="prod_analytics.customers.customer_profile",
                         object_type="table", access_mode="READ_ONLY", format="delta"),
        ClassifiedObject(fqn="prod_analytics.customers.churn_scores",
                         object_type="table", access_mode="READ_WRITE", format="delta"),
        ClassifiedObject(fqn="prod_analytics.customers.churn_daily_output",
                         object_type="table", access_mode="WRITE_ONLY", format="delta"),
        ClassifiedObject(fqn="prod_analytics.shared.date_dim",
                         object_type="view", access_mode="READ_ONLY", format=None),
    ],
    review_required=False,
)


def _build(**kw):
    kw.setdefault("dr_id", "DR-1042")
    kw.setdefault("streams", _STREAMS)
    kw.setdefault("classification", _CLASS)
    return build_manifest(**kw)


# ------------------------------------------------------------------
# Build
# ------------------------------------------------------------------

class TestBuildManifest:
    def test_full_structure(self) -> None:
        m = _build(scanned_at=datetime(2026, 4, 13, 10, 30, tzinfo=UTC))
        sr = m["scan_result"]
        assert sr["dr_id"] == "DR-1042"
        assert sr["scanned_at"] == "2026-04-13T10:30:00+00:00"
        assert sr["total_objects"] == 4
        assert not sr["review_required"]
        assert sr["schemas_required"] == ["prod_analytics.customers", "prod_analytics.shared"]
        assert len(sr["streams_scanned"]) == 1
        assert sr["streams_scanned"][0]["workflow_id"] == "1234567890"
        # Objects
        fqns = {o["fqn"] for o in sr["objects"]}
        assert "prod_analytics.customers.customer_profile" in fqns
        assert "prod_analytics.shared.date_dim" in fqns
        # View has no format
        view = next(o for o in sr["objects"] if o["type"] == "view")
        assert "format" not in view
        # All objects have required keys
        for obj in sr["objects"]:
            assert {"fqn", "type", "access_mode"} <= obj.keys()

    def test_review_required_from_classification(self) -> None:
        c = ClassificationResult(objects=[], review_required=True)
        assert _build(classification=c)["scan_result"]["review_required"] is True

    def test_review_required_from_lineage_limit(self) -> None:
        c = ClassificationResult(objects=[], review_required=False)
        assert _build(classification=c, lineage_row_limit_hit=True)["scan_result"]["review_required"] is True

    def test_pipeline_stream(self) -> None:
        s = [ResolvedStream(name="pipe", resource_type="pipeline", resource_id="pip-001")]
        c = ClassificationResult(objects=[], review_required=False)
        ss = _build(streams=s, classification=c)["scan_result"]["streams_scanned"][0]
        assert ss["workflow_id"] == "pip-001" and "tasks" not in ss


# ------------------------------------------------------------------
# Write / read roundtrip
# ------------------------------------------------------------------

class TestWriteAndRead:
    def test_roundtrip(self, tmp_path: Path) -> None:
        m = _build(scanned_at=datetime(2026, 4, 13, 10, 30, tzinfo=UTC))
        p = tmp_path / "m.yaml"
        write_manifest(m, p)
        loaded = read_manifest(p)
        assert loaded["scan_result"]["dr_id"] == "DR-1042"
        assert loaded["scan_result"]["total_objects"] == 4

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        c = ClassificationResult(objects=[], review_required=False)
        p = tmp_path / "a" / "b" / "m.yaml"
        write_manifest(_build(streams=[], classification=c), p)
        assert p.exists()

    def test_valid_yaml(self, tmp_path: Path) -> None:
        p = tmp_path / "m.yaml"
        write_manifest(_build(), p)
        assert yaml.safe_load(p.read_text()) is not None


# ------------------------------------------------------------------
# Table sizes
# ------------------------------------------------------------------

class TestEstimatedSizeGb:
    def test_populated(self) -> None:
        sizes = {"prod_analytics.customers.customer_profile": 5.5, "prod_analytics.customers.churn_scores": 2.0}
        objs = _build(table_sizes=sizes)["scan_result"]["objects"]
        profile = next(o for o in objs if "customer_profile" in o["fqn"])
        assert profile["estimated_size_gb"] == 5.5

    def test_absent_without_sizes(self) -> None:
        for obj in _build()["scan_result"]["objects"]:
            assert "estimated_size_gb" not in obj

    def test_partial_sizes(self) -> None:
        sizes = {"prod_analytics.customers.customer_profile": 1.0}
        objs = _build(table_sizes=sizes)["scan_result"]["objects"]
        churn = next(o for o in objs if "churn_scores" in o["fqn"])
        assert "estimated_size_gb" not in churn
