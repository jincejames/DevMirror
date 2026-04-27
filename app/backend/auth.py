"""RBAC: role resolution, admin checks, and ownership helpers."""

from __future__ import annotations

import logging
import os
import re
import threading
import time

from fastapi import Depends, HTTPException, Request
from pydantic import BaseModel

# Strict email shape used as a SCIM-filter safety gate.  We refuse to call
# the users.list filter API with anything that doesn't match this -- it
# defends against SCIM filter injection regardless of how individual SDK
# versions escape internally.
_EMAIL_PATTERN = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cache: email -> (role, timestamp)
# ---------------------------------------------------------------------------
_role_cache: dict[str, tuple[str, float]] = {}
_role_cache_lock = threading.Lock()
# Lowered from 300s to 120s (Sec finding #7) so admin removal propagates
# faster.  Admins can also force-flush via POST /api/admin/cache/flush.
_CACHE_TTL_SECONDS = 120


def flush_role_cache() -> int:
    """Drop every cached role.  Returns the number of entries cleared.

    Used by the admin flush endpoint so incident response can revoke
    privileges without waiting for the TTL.
    """
    with _role_cache_lock:
        n = len(_role_cache)
        _role_cache.clear()
        return n


# ---------------------------------------------------------------------------
# Pydantic model
# ---------------------------------------------------------------------------

class UserInfo(BaseModel):
    """Public user identity with resolved role."""

    email: str
    role: str
    display_name: str


# ---------------------------------------------------------------------------
# Role resolution dependency
# ---------------------------------------------------------------------------

def get_user_role(request: Request) -> str:
    """FastAPI dependency that resolves the caller's role (``"admin"`` or ``"user"``).

    Resolution steps:
    1. Extract email from ``X-Forwarded-Email`` header (fall back to ``"unknown"``).
    2. Check the in-memory cache (TTL 5 min).
    3. Query the Databricks workspace admin group (env ``DEVMIRROR_ADMIN_GROUP``,
       default ``"devmirror-admins"``) via ``WorkspaceClient().groups.list()``.
    4. Return ``"admin"`` if the user is a member, otherwise ``"user"``.
    5. On any failure, default to ``"user"`` (fail-safe / least privilege).
    """
    email = request.headers.get("X-Forwarded-Email", "unknown")

    # Check cache --------------------------------------------------------
    now = time.time()
    with _role_cache_lock:
        cached = _role_cache.get(email)
        if cached is not None:
            role, ts = cached
            if now - ts < _CACHE_TTL_SECONDS:
                return role

    # Resolve role from Databricks group API ----------------------------
    role = _resolve_role(email)

    with _role_cache_lock:
        _role_cache[email] = (role, time.time())

    return role


def _resolve_role(email: str) -> str:
    """Query Databricks groups API to determine if *email* is an admin.

    Group members are stored as ``{value: <user_id>, display: <display name>}``
    in SCIM. Neither field is the email, so we first translate the email to
    a user ID via the SCIM users API, then check that ID against the admin
    group's members. We also fall back to ``user.userName`` and ``user.emails``
    in case display naming is non-standard.
    """
    try:
        from databricks.sdk import WorkspaceClient

        admin_group = os.environ.get("DEVMIRROR_ADMIN_GROUP", "devmirror-admins")
        ws = WorkspaceClient()
        email_lc = email.lower()

        # 1. Resolve email -> user ID via SCIM users API.  Guard against
        # SCIM filter injection: refuse to build the filter at all if the
        # email contains anything outside the strict pattern.  Any quote,
        # CRLF, or operator-like substring trips the regex and we fall
        # through to the display-string fallback paths below.
        user_id: str | None = None
        if _EMAIL_PATTERN.match(email):
            users = list(ws.users.list(filter=f"userName eq '{email}'"))
            if users:
                user_id = str(getattr(users[0], "id", "") or "")
        else:
            logger.warning(
                "Refusing SCIM users.list with non-conforming email; "
                "falling back to display match",
            )

        # 2. Find the admin group, then fetch full detail (list may omit members)
        groups = list(ws.groups.list(filter=f"displayName eq '{admin_group}'"))
        if not groups:
            logger.info("Admin group '%s' not found; defaulting to 'user'", admin_group)
            return "user"

        group_id = getattr(groups[0], "id", None)
        if not group_id:
            return "user"
        group = ws.groups.get(group_id)
        members = group.members or []

        # 3. Match by user ID (primary) or by display fields (fallback)
        for member in members:
            member_value = str(getattr(member, "value", "") or "")
            member_display = str(getattr(member, "display", "") or "").lower()
            if user_id and member_value == user_id:
                return "admin"
            if email_lc == member_display:
                return "admin"
            # Some directories store the email in member.value (e.g. SP refs)
            if email_lc == member_value.lower():
                return "admin"

        return "user"
    except Exception:
        logger.warning("Failed to resolve role for '%s'; defaulting to 'user'", email, exc_info=True)
        return "user"


# ---------------------------------------------------------------------------
# Guard dependencies
# ---------------------------------------------------------------------------

def require_admin(role: str = Depends(get_user_role)) -> None:
    """Dependency that raises 403 unless the caller is an admin."""
    if role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required.")


def require_owner_or_admin(row: dict, user: str, role: str) -> None:
    """Raise 403 if *role* is ``"user"`` and the row was not created by *user*.

    This is a plain helper (not a FastAPI dependency) — call it inside endpoints.
    """
    if role != "admin" and row.get("created_by") != user:
        raise HTTPException(status_code=403, detail="You do not have access to this resource.")
