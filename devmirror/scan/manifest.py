"""Build and serialize the scan_result manifest (T019)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from pathlib import Path

    from devmirror.scan.dependency_classifier import ClassificationResult
    from devmirror.scan.stream_resolver import ResolvedStream


def _extract_schemas(objects: list[dict[str, Any]]) -> list[str]:
    """Extract unique sorted two-part schema FQNs from object FQNs.

    Given FQN ``catalog.schema.table``, extracts ``catalog.schema``.
    """
    schemas: set[str] = set()
    for obj in objects:
        fqn = obj["fqn"]
        parts = fqn.split(".")
        if len(parts) >= 2:
            schemas.add(f"{parts[0]}.{parts[1]}")
    return sorted(schemas)


def build_manifest(
    dr_id: str,
    streams: list[ResolvedStream],
    classification: ClassificationResult,
    *,
    lineage_row_limit_hit: bool = False,
    scanned_at: datetime | None = None,
    table_sizes: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Build the scan_result manifest dict."""
    if scanned_at is None:
        scanned_at = datetime.now(tz=UTC)

    streams_scanned: list[dict[str, Any]] = []
    for s in streams:
        entry: dict[str, Any] = {
            "name": s.name,
            "workflow_id": s.resource_id,
        }
        if s.task_keys:
            entry["tasks"] = s.task_keys
        streams_scanned.append(entry)

    objects: list[dict[str, Any]] = []
    for obj in classification.objects:
        obj_dict: dict[str, Any] = {
            "fqn": obj.fqn,
            "type": obj.object_type,
            "access_mode": obj.access_mode,
        }
        if obj.format:
            obj_dict["format"] = obj.format
        if table_sizes and obj.fqn in table_sizes:
            obj_dict["estimated_size_gb"] = table_sizes[obj.fqn]
        objects.append(obj_dict)

    schemas_required = _extract_schemas(objects)

    review_required = classification.review_required or lineage_row_limit_hit

    return {
        "scan_result": {
            "dr_id": dr_id,
            "scanned_at": scanned_at.isoformat(),
            "streams_scanned": streams_scanned,
            "objects": objects,
            "schemas_required": schemas_required,
            "total_objects": len(objects),
            "review_required": review_required,
        }
    }


def write_manifest(manifest: dict[str, Any], output_path: Path) -> None:
    """Serialize the manifest dict to a YAML file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        yaml.safe_dump(manifest, f, default_flow_style=False, sort_keys=False)


def read_manifest(path: Path) -> dict[str, Any]:
    """Read a manifest YAML file back into a dict."""
    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        msg = f"Expected a YAML mapping, got {type(data).__name__}"
        raise ValueError(msg)
    return data
