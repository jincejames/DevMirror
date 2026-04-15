"""In-memory background task tracker for long-running operations."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime


@dataclass
class TaskStatus:
    """Snapshot of a background task's current state."""

    task_id: str
    dr_id: str
    task_type: str  # "scan", "provision", "cleanup"
    status: str  # "running", "completed", "failed"
    progress: str = ""
    result: dict | None = None
    error: str | None = None
    started_at: str = ""
    completed_at: str | None = None


class TaskTracker:
    """Simple in-memory background task system stored on ``app.state``."""

    def __init__(self) -> None:
        self._tasks: dict[str, TaskStatus] = {}
        self._lock = threading.Lock()

    def submit(self, dr_id: str, task_type: str, fn: Callable) -> str:
        """Start *fn* in a background thread and return a task_id."""
        task_id = f"task-{uuid.uuid4().hex[:8]}"
        task = TaskStatus(
            task_id=task_id,
            dr_id=dr_id,
            task_type=task_type,
            status="running",
            started_at=datetime.now(UTC).isoformat(),
        )
        with self._lock:
            self._tasks[task_id] = task
        thread = threading.Thread(
            target=self._run, args=(task_id, fn), daemon=True
        )
        thread.start()
        return task_id

    def _run(self, task_id: str, fn: Callable) -> None:
        """Execute the task function and update status on completion or failure."""
        try:
            result = fn()
            with self._lock:
                t = self._tasks[task_id]
                t.status = "completed"
                t.result = result
                t.completed_at = datetime.now(UTC).isoformat()
        except Exception as exc:  # noqa: BLE001
            with self._lock:
                t = self._tasks[task_id]
                t.status = "failed"
                t.error = str(exc)
                t.completed_at = datetime.now(UTC).isoformat()

    def get(self, task_id: str) -> TaskStatus | None:
        """Return the current status of a task, or ``None``."""
        return self._tasks.get(task_id)

    def list_for_dr(self, dr_id: str) -> list[TaskStatus]:
        """Return all tasks associated with a DR."""
        return [t for t in self._tasks.values() if t.dr_id == dr_id]
