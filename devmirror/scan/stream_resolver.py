"""Resolve stream names to Databricks Workflow or Pipeline definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from databricks.sdk import WorkspaceClient


class StreamResolutionError(Exception):
    """Raised when one or more streams cannot be uniquely resolved."""

    def __init__(self, message: str, unresolved: list[str] | None = None) -> None:
        self.unresolved = unresolved or []
        super().__init__(message)


@dataclass
class ResolvedStream:
    """A stream resolved to a concrete Databricks resource."""

    name: str
    resource_type: str  # "job" or "pipeline"
    resource_id: str
    task_keys: list[str] = field(default_factory=list)


def resolve_job_by_name(client: WorkspaceClient, name: str) -> ResolvedStream | None:
    """Attempt to resolve a stream name as a Databricks Workflow (job).

    Uses ``client.jobs.list(name=name)`` for exact name filtering.

    Returns:
        A ``ResolvedStream`` if exactly one job matches, else ``None``.

    Raises:
        StreamResolutionError: If multiple jobs match the same name.
    """
    matches = list(client.jobs.list(name=name))
    if len(matches) == 0:
        return None
    if len(matches) > 1:
        ids = [str(j.job_id) for j in matches]
        raise StreamResolutionError(
            f"Ambiguous job match for stream '{name}': found {len(matches)} jobs "
            f"with ids {ids}. Provide a more specific name.",
            unresolved=[name],
        )

    job = matches[0]
    task_keys: list[str] = []
    if job.settings and job.settings.tasks:
        task_keys = [t.task_key for t in job.settings.tasks if t.task_key]

    return ResolvedStream(
        name=name,
        resource_type="job",
        resource_id=str(job.job_id),
        task_keys=task_keys,
    )


def resolve_pipeline_by_name(client: WorkspaceClient, name: str) -> ResolvedStream | None:
    """Attempt to resolve a stream name as a Lakeflow Declarative Pipeline.

    Uses ``client.pipelines.list_pipelines(filter=...)`` for name filtering.

    Returns:
        A ``ResolvedStream`` if exactly one pipeline matches, else ``None``.

    Raises:
        StreamResolutionError: If multiple pipelines match the same name.
    """
    matches = list(client.pipelines.list_pipelines(filter=f"name LIKE '{name}'"))
    if len(matches) == 0:
        return None
    if len(matches) > 1:
        ids = [str(p.pipeline_id) for p in matches]
        raise StreamResolutionError(
            f"Ambiguous pipeline match for stream '{name}': found {len(matches)} pipelines "
            f"with ids {ids}. Provide a more specific name.",
            unresolved=[name],
        )

    pipeline = matches[0]
    return ResolvedStream(
        name=name,
        resource_type="pipeline",
        resource_id=str(pipeline.pipeline_id),
        task_keys=[],
    )


def resolve_streams(
    client: WorkspaceClient,
    stream_names: list[str],
) -> tuple[list[ResolvedStream], list[str]]:
    """Resolve a list of stream names to Databricks resources.

    Tries job resolution first, then pipeline resolution for each name.

    Args:
        client: Authenticated WorkspaceClient.
        stream_names: Stream names from the development request config.

    Returns:
        A tuple of (resolved_streams, unresolved_names).
    """
    resolved: list[ResolvedStream] = []
    unresolved: list[str] = []

    for name in stream_names:
        stream = resolve_job_by_name(client, name)
        if stream is None:
            stream = resolve_pipeline_by_name(client, name)
        if stream is not None:
            resolved.append(stream)
        else:
            unresolved.append(name)

    return resolved, unresolved
