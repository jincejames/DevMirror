"""Classify discovered objects as READ_ONLY, READ_WRITE, or WRITE_ONLY."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from devmirror.scan.lineage import LineageEdge


@dataclass
class ClassifiedObject:
    """A single object with its dependency classification."""

    fqn: str
    object_type: str  # "table" or "view"
    access_mode: str  # "READ_ONLY", "READ_WRITE", "WRITE_ONLY"
    format: str | None = None


@dataclass
class ClassificationResult:
    """Result of dependency classification."""

    objects: list[ClassifiedObject]
    review_required: bool = False


def _infer_type(type_hint: str | None) -> str:
    """Infer object type from lineage type hint.

    Defaults to ``"table"`` when no hint is available.
    """
    if type_hint and type_hint.upper() == "VIEW":
        return "view"
    return "table"


def classify_dependencies(
    edges: list[LineageEdge],
    additional_objects: list[str] | None = None,
) -> ClassificationResult:
    """Classify each unique object found in lineage edges.

    Algorithm:
        1. Scan all edges.  For each object appearing as a *source* (read),
           mark it as READ.  For each object appearing as a *target* (write),
           mark it as WRITE.
        2. Combine:
            - READ only -> READ_ONLY
            - WRITE only -> WRITE_ONLY
            - both READ and WRITE -> READ_WRITE
        3. Additional objects from the config (not discovered by lineage)
           default to READ_ONLY and set ``review_required``.

    Args:
        edges: Lineage edges from the lineage query.
        additional_objects: Extra FQNs from the config that the user
            wants included even if they were not in automatic lineage.

    Returns:
        A ``ClassificationResult`` with classified objects and a flag
        indicating whether human review is recommended.
    """
    # Track read and write sets per FQN, plus type hints
    reads: set[str] = set()
    writes: set[str] = set()
    type_hints: dict[str, str | None] = {}

    for edge in edges:
        if edge.source_table_fqn:
            reads.add(edge.source_table_fqn)
            if edge.source_table_fqn not in type_hints:
                type_hints[edge.source_table_fqn] = edge.source_type
        if edge.target_table_fqn:
            writes.add(edge.target_table_fqn)
            if edge.target_table_fqn not in type_hints:
                type_hints[edge.target_table_fqn] = edge.target_type

    all_fqns = reads | writes
    review_required = False

    objects: list[ClassifiedObject] = []
    for fqn in sorted(all_fqns):
        is_read = fqn in reads
        is_write = fqn in writes

        if is_read and is_write:
            access_mode = "READ_WRITE"
        elif is_write:
            access_mode = "WRITE_ONLY"
        else:
            access_mode = "READ_ONLY"

        obj_type = _infer_type(type_hints.get(fqn))
        fmt = "delta" if obj_type == "table" else None

        objects.append(
            ClassifiedObject(
                fqn=fqn,
                object_type=obj_type,
                access_mode=access_mode,
                format=fmt,
            )
        )

    # Handle additional objects not found in lineage
    if additional_objects:
        existing_fqns = {o.fqn for o in objects}
        for fqn in additional_objects:
            if fqn not in existing_fqns:
                objects.append(
                    ClassifiedObject(
                        fqn=fqn,
                        object_type="table",
                        access_mode="READ_ONLY",
                        format="delta",
                    )
                )
                review_required = True

    # Sort by FQN for deterministic output
    objects.sort(key=lambda o: o.fqn)

    return ClassificationResult(objects=objects, review_required=review_required)
