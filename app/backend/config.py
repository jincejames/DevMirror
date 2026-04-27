"""FastAPI dependency providers for DbClient, Settings, current user, and TaskTracker."""

from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import HTTPException, Request  # noqa: TC002 -- FastAPI resolves type hints at runtime

from devmirror.config.schema import DR_ID_PATTERN, NEW_DR_ID_PATTERN

if TYPE_CHECKING:
    from devmirror.settings import Settings
    from devmirror.utils.db_client import DbClient

    from .tasks import TaskTracker


def validate_dr_id(dr_id: str) -> str:
    """Reject path-parameter values that don't match the legacy or new DR ID
    pattern.  Closes a CRLF / response-splitting vector on endpoints that
    interpolate the value into headers (Content-Disposition).

    Used as a FastAPI dependency:

        def endpoint(dr_id: str = Depends(validate_dr_id)) -> ...:
    """
    if not (DR_ID_PATTERN.match(dr_id) or NEW_DR_ID_PATTERN.match(dr_id)):
        raise HTTPException(status_code=400, detail=f"Invalid DR ID format: {dr_id!r}")
    return dr_id


def get_db_client(request: Request) -> DbClient:
    """Return the DbClient stored on app.state during lifespan."""
    return request.app.state.db_client


def get_settings(request: Request) -> Settings:
    """Return the Settings stored on app.state during lifespan."""
    return request.app.state.settings


def get_current_user(request: Request) -> str:
    """Extract the current user email from the X-Forwarded-Email header.

    Raises 401 if the header is absent or empty.  Databricks Apps' reverse
    proxy always sets this header for SSO-authenticated requests; an absent
    header means the request bypassed the proxy, which we treat as
    unauthenticated.  This also closes a defense-in-depth gap where rows
    with ``created_by IS NULL`` would otherwise pass an ownership check
    against a user whose email also defaulted to ``"unknown"``.
    """
    email = (request.headers.get("X-Forwarded-Email") or "").strip()
    if not email:
        raise HTTPException(
            status_code=401,
            detail="Authentication required.",
        )
    return email


def get_task_tracker(request: Request) -> TaskTracker:
    """Return the TaskTracker stored on app.state during lifespan."""
    return request.app.state.task_tracker
