"""Unit tests for devmirror.scan.stream_resolver."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from devmirror.scan.stream_resolver import (
    StreamResolutionError,
    resolve_job_by_name,
    resolve_pipeline_by_name,
    resolve_streams,
)


def _make_job(job_id: int, name: str, task_keys: list[str] | None = None) -> MagicMock:
    """Create a mock Job object."""
    job = MagicMock()
    job.job_id = job_id
    job.settings = MagicMock()
    if task_keys:
        tasks = []
        for tk in task_keys:
            task = MagicMock()
            task.task_key = tk
            tasks.append(task)
        job.settings.tasks = tasks
    else:
        job.settings.tasks = []
    return job


def _make_pipeline(pipeline_id: str, name: str) -> MagicMock:
    """Create a mock PipelineStateInfo object."""
    pipeline = MagicMock()
    pipeline.pipeline_id = pipeline_id
    pipeline.name = name
    return pipeline


class TestResolveJobByName:
    def test_single_match(self) -> None:
        client = MagicMock()
        client.jobs.list.return_value = [
            _make_job(123, "my_job", ["task_a", "task_b"])
        ]

        result = resolve_job_by_name(client, "my_job")
        assert result is not None
        assert result.name == "my_job"
        assert result.resource_type == "job"
        assert result.resource_id == "123"
        assert result.task_keys == ["task_a", "task_b"]
        client.jobs.list.assert_called_once_with(name="my_job")

    def test_no_match(self) -> None:
        client = MagicMock()
        client.jobs.list.return_value = []

        result = resolve_job_by_name(client, "missing_job")
        assert result is None

    def test_ambiguous_match_raises(self) -> None:
        client = MagicMock()
        client.jobs.list.return_value = [
            _make_job(100, "dup_job"),
            _make_job(200, "dup_job"),
        ]

        with pytest.raises(StreamResolutionError, match="Ambiguous job match"):
            resolve_job_by_name(client, "dup_job")

    def test_job_with_no_tasks(self) -> None:
        client = MagicMock()
        job = _make_job(456, "no_tasks")
        job.settings.tasks = []
        client.jobs.list.return_value = [job]

        result = resolve_job_by_name(client, "no_tasks")
        assert result is not None
        assert result.task_keys == []

    def test_job_with_none_settings(self) -> None:
        client = MagicMock()
        job = MagicMock()
        job.job_id = 789
        job.settings = None
        client.jobs.list.return_value = [job]

        result = resolve_job_by_name(client, "nil_settings")
        assert result is not None
        assert result.task_keys == []


class TestResolvePipelineByName:
    def test_single_match(self) -> None:
        client = MagicMock()
        client.pipelines.list_pipelines.return_value = [
            _make_pipeline("pip-001", "my_pipeline")
        ]

        result = resolve_pipeline_by_name(client, "my_pipeline")
        assert result is not None
        assert result.name == "my_pipeline"
        assert result.resource_type == "pipeline"
        assert result.resource_id == "pip-001"
        assert result.task_keys == []

    def test_no_match(self) -> None:
        client = MagicMock()
        client.pipelines.list_pipelines.return_value = []

        result = resolve_pipeline_by_name(client, "missing_pipeline")
        assert result is None

    def test_ambiguous_match_raises(self) -> None:
        client = MagicMock()
        client.pipelines.list_pipelines.return_value = [
            _make_pipeline("pip-a", "dup"),
            _make_pipeline("pip-b", "dup"),
        ]

        with pytest.raises(StreamResolutionError, match="Ambiguous pipeline match"):
            resolve_pipeline_by_name(client, "dup")


class TestResolveStreams:
    def test_all_resolved_as_jobs(self) -> None:
        client = MagicMock()

        def list_jobs(name: str):
            return [_make_job(10, name, ["t1"])]

        client.jobs.list.side_effect = list_jobs

        resolved, unresolved = resolve_streams(client, ["job_a", "job_b"])
        assert len(resolved) == 2
        assert len(unresolved) == 0
        assert resolved[0].name == "job_a"
        assert resolved[1].name == "job_b"

    def test_mixed_job_and_pipeline(self) -> None:
        client = MagicMock()
        client.jobs.list.side_effect = [
            [_make_job(10, "a_job")],
            [],  # second stream not a job
        ]
        client.pipelines.list_pipelines.return_value = [
            _make_pipeline("pip-1", "a_pipeline")
        ]

        resolved, unresolved = resolve_streams(client, ["a_job", "a_pipeline"])
        assert len(resolved) == 2
        assert resolved[0].resource_type == "job"
        assert resolved[1].resource_type == "pipeline"
        assert len(unresolved) == 0

    def test_unresolved_streams(self) -> None:
        client = MagicMock()
        client.jobs.list.return_value = []
        client.pipelines.list_pipelines.return_value = []

        resolved, unresolved = resolve_streams(client, ["ghost"])
        assert len(resolved) == 0
        assert unresolved == ["ghost"]

    def test_empty_input(self) -> None:
        client = MagicMock()
        resolved, unresolved = resolve_streams(client, [])
        assert resolved == []
        assert unresolved == []
