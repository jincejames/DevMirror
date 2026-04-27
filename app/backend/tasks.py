"""In-memory background task tracker for long-running operations."""

from __future__ import annotations

import logging
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

logger = logging.getLogger(__name__)


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
    """Simple in-memory background task system stored on ``app.state``.

    Caps retained tasks so memory doesn't grow unbounded over the app's
    lifetime.  When the cap is exceeded, the oldest completed/failed
    tasks are dropped first; running tasks are never evicted.
    """

    # Keep at most this many task records in memory.  Beyond this we drop
    # the oldest *completed* tasks first; a running task is never evicted.
    MAX_TASKS = 500

    def __init__(self, *, max_tasks: int = MAX_TASKS) -> None:
        self._tasks: dict[str, TaskStatus] = {}
        self._threads: dict[str, threading.Thread] = {}
        self._lock = threading.Lock()
        self._max_tasks = max_tasks

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
            self._evict_old_completed_locked()
        thread = threading.Thread(
            target=self._run, args=(task_id, fn), daemon=True
        )
        thread.start()
        with self._lock:
            self._threads[task_id] = thread
        return task_id

    def _evict_old_completed_locked(self) -> None:
        """Drop the oldest completed/failed tasks if the cap is exceeded.

        Caller must hold ``self._lock``.  Running tasks are preserved.
        Tasks are dropped in completed_at order (oldest first).
        """
        excess = len(self._tasks) - self._max_tasks
        if excess <= 0:
            return
        # Sort completed/failed tasks by completion time (oldest first).
        completed = sorted(
            (
                (t.completed_at or "", tid)
                for tid, t in self._tasks.items()
                if t.status in ("completed", "failed")
            ),
        )
        dropped = 0
        for _ts, tid in completed:
            if dropped >= excess:
                break
            self._tasks.pop(tid, None)
            self._threads.pop(tid, None)
            dropped += 1
        if dropped:
            logger.debug("TaskTracker evicted %d completed task(s)", dropped)

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

    def wait_for_running(self, timeout: float = 10.0) -> None:
        """Wait up to *timeout* seconds for running tasks to complete.

        Called during graceful shutdown so in-flight provisioning threads
        get a chance to finish before the process is killed.
        """
        with self._lock:
            running = [
                (tid, t) for tid, t in self._threads.items()
                if t.is_alive()
            ]
        if not running:
            return
        logger.info("Waiting for %d running task(s) to finish (timeout=%ss)", len(running), timeout)
        for tid, thread in running:
            thread.join(timeout=timeout / max(len(running), 1))
            if thread.is_alive():
                logger.warning("Task %s did not finish within shutdown timeout", tid)
