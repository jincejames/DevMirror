"""Shared utilities: SQL execution, naming conventions, concurrency."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, TypeVar

from devmirror.utils.db_client import DbClient
from devmirror.utils.naming import NamingError
from devmirror.utils.sql_executor import SqlExecutionError, SqlExecutor, escape_sql_string

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)

T = TypeVar("T")


def now_iso() -> str:
    """Return current UTC time as ISO 8601 string."""
    return datetime.now(tz=UTC).isoformat()


@dataclass
class TaskResult:
    """Outcome of a single task executed by ``run_bounded``."""

    index: int
    value: object = None
    success: bool = True
    error: str | None = None


def run_bounded(
    tasks: list[Callable[[], T]],
    *,
    max_workers: int = 10,
) -> list[TaskResult]:
    """Execute *tasks* with bounded parallelism, collecting results in original order."""
    if not tasks:
        return []

    effective_workers = min(max_workers, len(tasks))
    results: list[TaskResult | None] = [None] * len(tasks)

    with ThreadPoolExecutor(max_workers=effective_workers) as pool:
        future_to_index = {
            pool.submit(task): idx for idx, task in enumerate(tasks)
        }
        for future in as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                value = future.result()
                results[idx] = TaskResult(index=idx, value=value, success=True)
            except Exception as exc:
                logger.error("Task %d failed: %s", idx, exc)
                results[idx] = TaskResult(
                    index=idx, value=None, success=False, error=str(exc)
                )

    return [r if r is not None else TaskResult(index=i, success=False, error="unknown")
            for i, r in enumerate(results)]


def revision_values(data_revision) -> tuple[str, str | None]:
    """Extract (mode, value) from a DataRevision, defaulting to ('latest', None)."""
    if not data_revision:
        return "latest", None
    mode = data_revision.mode
    if mode == "version" and data_revision.version is not None:
        val = str(data_revision.version)
    elif mode == "timestamp":
        val = data_revision.timestamp
    else:
        val = None
    return mode, val


__all__ = [
    "DbClient",
    "NamingError",
    "SqlExecutionError",
    "SqlExecutor",
    "TaskResult",
    "escape_sql_string",
    "now_iso",
    "revision_values",
    "run_bounded",
]
