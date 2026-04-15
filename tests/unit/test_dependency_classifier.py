"""Unit tests for devmirror.scan.dependency_classifier."""

from __future__ import annotations

from devmirror.scan.dependency_classifier import (
    classify_dependencies,
)
from devmirror.scan.lineage import LineageEdge


def _edge(
    source: str | None = None,
    target: str | None = None,
    source_type: str | None = None,
    target_type: str | None = None,
) -> LineageEdge:
    return LineageEdge(
        source_table_fqn=source,
        target_table_fqn=target,
        source_type=source_type,
        target_type=target_type,
        entity_id="job-1",
    )


class TestClassifyDependencies:
    def test_read_only(self) -> None:
        """An object that only appears as a source is READ_ONLY."""
        edges = [_edge(source="cat.sch.read_table")]
        result = classify_dependencies(edges)
        assert len(result.objects) == 1
        assert result.objects[0].access_mode == "READ_ONLY"
        assert result.objects[0].fqn == "cat.sch.read_table"

    def test_write_only(self) -> None:
        """An object that only appears as a target is WRITE_ONLY."""
        edges = [_edge(target="cat.sch.write_table")]
        result = classify_dependencies(edges)
        assert len(result.objects) == 1
        assert result.objects[0].access_mode == "WRITE_ONLY"

    def test_read_write(self) -> None:
        """An object appearing as both source and target is READ_WRITE."""
        edges = [
            _edge(source="cat.sch.rw_table"),
            _edge(target="cat.sch.rw_table"),
        ]
        result = classify_dependencies(edges)
        assert len(result.objects) == 1
        assert result.objects[0].access_mode == "READ_WRITE"

    def test_read_write_in_single_edge(self) -> None:
        """A single edge with both source and target being the same FQN."""
        edges = [_edge(source="cat.sch.self_ref", target="cat.sch.self_ref")]
        result = classify_dependencies(edges)
        assert len(result.objects) == 1
        assert result.objects[0].access_mode == "READ_WRITE"

    def test_mixed_classification(self) -> None:
        """Multiple objects with different classifications."""
        edges = [
            _edge(source="cat.sch.src"),
            _edge(target="cat.sch.tgt"),
            _edge(source="cat.sch.both", target="cat.sch.both"),
        ]
        result = classify_dependencies(edges)
        assert len(result.objects) == 3

        by_fqn = {o.fqn: o for o in result.objects}
        assert by_fqn["cat.sch.src"].access_mode == "READ_ONLY"
        assert by_fqn["cat.sch.tgt"].access_mode == "WRITE_ONLY"
        assert by_fqn["cat.sch.both"].access_mode == "READ_WRITE"

    def test_view_type_detected(self) -> None:
        """A VIEW source_type is reflected in object_type."""
        edges = [_edge(source="cat.sch.v1", source_type="VIEW")]
        result = classify_dependencies(edges)
        assert result.objects[0].object_type == "view"
        assert result.objects[0].format is None

    def test_table_type_default(self) -> None:
        """Default object type is table with delta format."""
        edges = [_edge(source="cat.sch.t1", source_type="TABLE")]
        result = classify_dependencies(edges)
        assert result.objects[0].object_type == "table"
        assert result.objects[0].format == "delta"

    def test_no_type_hint_defaults_to_table(self) -> None:
        edges = [_edge(source="cat.sch.t1")]
        result = classify_dependencies(edges)
        assert result.objects[0].object_type == "table"

    def test_additional_objects_not_in_lineage(self) -> None:
        """Additional objects from config that are not in lineage get READ_ONLY."""
        edges = [_edge(source="cat.sch.found")]
        result = classify_dependencies(
            edges, additional_objects=["cat.sch.extra"]
        )
        assert len(result.objects) == 2
        assert result.review_required is True

        extra = next(o for o in result.objects if o.fqn == "cat.sch.extra")
        assert extra.access_mode == "READ_ONLY"
        assert extra.object_type == "table"

    def test_additional_objects_already_in_lineage(self) -> None:
        """Additional objects already in lineage are not duplicated."""
        edges = [_edge(source="cat.sch.overlap")]
        result = classify_dependencies(
            edges, additional_objects=["cat.sch.overlap"]
        )
        assert len(result.objects) == 1
        assert result.review_required is False

    def test_empty_edges(self) -> None:
        result = classify_dependencies([])
        assert result.objects == []
        assert result.review_required is False

    def test_empty_edges_with_additional(self) -> None:
        result = classify_dependencies([], additional_objects=["cat.sch.only"])
        assert len(result.objects) == 1
        assert result.review_required is True

    def test_output_sorted_by_fqn(self) -> None:
        edges = [
            _edge(source="z.z.z"),
            _edge(source="a.a.a"),
            _edge(source="m.m.m"),
        ]
        result = classify_dependencies(edges)
        fqns = [o.fqn for o in result.objects]
        assert fqns == sorted(fqns)
