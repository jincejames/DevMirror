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
# 120s TTL keeps SCIM lookups cheap while letting admin-group removal
# propagate within a short window.  Admins can also force-flush via
# POST /api/admin/cache/flush.
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
    2. Extract the user's OAuth token from ``X-Forwarded-Access-Token``.
       The platform sends this header only when ``user_api_scopes`` is
       declared in ``app.yaml``.
    3. Check the in-memory cache (TTL 120 s).
    4. Query SCIM ``/Me`` *as the user* (not as the app SP) for group
       memberships -- the app SP itself can't read another user's groups
       on non-admin workspaces, but every user can read their own /Me.
    5. Return ``"admin"`` if the configured admin group is among the
       user's groups, otherwise ``"user"``.
    6. On any failure, default to ``"user"`` (fail-safe / least privilege).
    """
    email = request.headers.get("X-Forwarded-Email", "unknown")
    user_token = request.headers.get("X-Forwarded-Access-Token", "") or ""

    # Check cache --------------------------------------------------------
    now = time.time()
    with _role_cache_lock:
        cached = _role_cache.get(email)
        if cached is not None:
            role, ts = cached
            if now - ts < _CACHE_TTL_SECONDS:
                return role

    # Resolve role from Databricks SCIM, using the user's own token -----
    role = _resolve_role(email, user_token)

    with _role_cache_lock:
        _role_cache[email] = (role, time.time())

    return role


def _resolve_role(email: str, user_token: str = "") -> str:
    """Query Databricks SCIM to determine if *email* is in the admin group.

    Implementation note: the workspace's ``GET /scim/v2/Users/{id}`` and
    ``GET /scim/v2/Groups/{id}`` endpoints are admin-only -- a non-admin
    service principal hits 403 on both.  ``GET /scim/v2/Users`` (list) is
    accessible to any authenticated SP and supports SCIM's
    ``?attributes=`` projection, which lets us pull the user's group
    memberships inline.  That avoids the admin-only Get endpoints entirely
    and works for:
      - workspace-local groups,
      - account-level groups assigned to the workspace (the workspace
        ``Groups/{id}.members`` list is empty for those, but ``users``
        records carry the membership in their ``groups`` attribute).

    SCIM filter injection is gated by ``_EMAIL_PATTERN``: emails that
    don't match a strict user-email shape skip the SCIM lookup and we
    return "user" (least privilege) without ever building a tainted
    filter.
    """
    try:
        from databricks.sdk import WorkspaceClient

        admin_group = os.environ.get("DEVMIRROR_ADMIN_GROUP", "devmirror-admins")
        admin_group_lc = admin_group.lower()

        if not _EMAIL_PATTERN.match(email):
            logger.warning(
                "Refusing SCIM lookup with non-conforming email %r; "
                "defaulting to 'user'",
                email,
            )
            return "user"

        # Static admin-emails supplement / break-glass path.  The OBO
        # SCIM group lookup below is the primary route (confirmed working
        # in LH pre-prod -- OBO `user_api_scopes` is effective, so members
        # of the admin group are resolved via their own `/Me` groups).
        # This list grants admin to principals not in the group, or serves
        # as a fallback if OBO ever stops working.  When DEVMIRROR_ADMIN_EMAILS
        # is set (comma-separated), any caller whose email matches (case-
        # insensitive) is granted admin without touching SCIM.  The
        # SCIM-based group lookup below still runs for callers NOT in
        # the static list, so the two paths layer naturally.
        admin_emails_raw = os.environ.get("DEVMIRROR_ADMIN_EMAILS", "").strip()
        if admin_emails_raw:
            allow = {
                e.strip().lower()
                for e in admin_emails_raw.split(",")
                if e.strip()
            }
            if email.lower() in allow:
                return "admin"

        # Two lookup paths.  Try OBO first (per-request user token) so the
        # app SP doesn't need workspace-admin privilege; fall back to the
        # app SP's own credentials.  The fallback only succeeds if the SP
        # IS a workspace admin -- non-admin SPs get an empty `groups`
        # attribute back from SCIM regardless of projection.  See the
        # "App SP SCIM-read gap" section of customers/lh/README.md for
        # the manual fix when neither path is available.
        candidates: list[WorkspaceClient] = []
        if user_token:
            host = (
                os.environ.get("DATABRICKS_HOST")
                or os.environ.get("DATABRICKS_WORKSPACE_URL")
                or ""
            )
            try:
                candidates.append(
                    WorkspaceClient(host=host, token=user_token, auth_type="pat"),
                )
            except Exception:
                logger.debug("OBO WorkspaceClient construction failed", exc_info=True)
        # SP-credentials fallback (works only when the SP is a workspace admin).
        candidates.append(WorkspaceClient())

        for ws in candidates:
            try:
                # Try /Me first when the WorkspaceClient is OBO-bound; for
                # the SP-creds client /Me would return the SP's own groups,
                # not the user's, so prefer users.list with projection.
                if ws is candidates[0] and user_token:
                    me = ws.current_user.me()
                    user_groups = me.groups or []
                else:
                    users = list(
                        ws.users.list(
                            filter=f"userName eq '{email}'",
                            attributes="id,userName,groups",
                        )
                    )
                    if not users:
                        continue
                    user_groups = users[0].groups or []
            except Exception:
                logger.debug("SCIM lookup attempt failed", exc_info=True)
                continue

            for g in user_groups:
                display = str(getattr(g, "display", "") or "").lower()
                if display == admin_group_lc:
                    return "admin"
            # If we got a non-empty groups list but didn't match, the
            # user genuinely isn't an admin; no need to try the next
            # candidate.
            if user_groups:
                return "user"

        return "user"
    except Exception:
        logger.warning("Failed to resolve role for %r; defaulting to 'user'", email, exc_info=True)
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
