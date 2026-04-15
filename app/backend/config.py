"""FastAPI dependency providers for DbClient, Settings, current user, and TaskTracker."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import Request  # noqa: TC002 -- FastAPI resolves type hints at runtime

if TYPE_CHECKING:
    from devmirror.settings import Settings
    from devmirror.utils.db_client import DbClient

    from .tasks import TaskTracker


def get_db_client(request: Request) -> DbClient:
    """Return the DbClient stored on app.state during lifespan."""
    return request.app.state.db_client


def get_settings(request: Request) -> Settings:
    """Return the Settings stored on app.state during lifespan."""
    return request.app.state.settings


def get_current_user(request: Request) -> str:
    """Extract the current user email from the X-Forwarded-Email header.

    Falls back to ``"unknown"`` when the header is absent (e.g. local dev).
    """
    return request.headers.get("X-Forwarded-Email", "unknown")


def get_task_tracker(request: Request) -> TaskTracker:
    """Return the TaskTracker stored on app.state during lifespan."""
    return request.app.state.task_tracker
