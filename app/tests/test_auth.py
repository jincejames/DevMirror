"""Tests for RBAC auth module and GET /api/me endpoint."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest
from backend.auth import (
    UserInfo,
    _resolve_role,
    _role_cache,
    _role_cache_lock,
    get_user_role,
    require_admin,
    require_owner_or_admin,
)
from backend.main import app
from fastapi import HTTPException

from .conftest import make_client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _wire_ws(
    mock_ws_cls,
    *,
    user_lookup: dict[str, str],
    user_groups_by_id: dict[str, list[str]] | None = None,
):
    """Configure the WorkspaceClient mock to mimic the Databricks SCIM /Me flow.

    The role resolver issues ``ws.current_user.me()`` against a per-request
    WorkspaceClient constructed with the user's own OAuth token.  The /Me
    endpoint always returns the caller's groups, so the test mocks it
    directly: ``mock_ws_cls.return_value.current_user.me.return_value`` is
    a user object with the test's groups.

    Args:
        mock_ws_cls: the patched ``WorkspaceClient`` class.
        user_lookup: map of email -> user_id. Used to set the ``id`` on
            the returned /Me record (some tests check that the lookup
            ran with the right token).
        user_groups_by_id: map of user_id -> list of group display names
            that should appear on the /Me record's ``groups`` attribute.
    """
    user_groups_by_id = user_groups_by_id or {}

    def make_ws(*_args, **_kwargs):
        # Each WorkspaceClient(...) call (per-request, with the user token)
        # produces a fresh mock whose .current_user.me() returns the
        # configured user record.
        ws = MagicMock()
        # Heuristic: pick the first user in user_lookup -- the tests only
        # use one user per case.
        if user_lookup:
            email, uid = next(iter(user_lookup.items()))
            me = MagicMock()
            me.id = uid
            me.user_name = email
            groups = []
            for display in user_groups_by_id.get(uid, []):
                g = MagicMock()
                g.display = display
                groups.append(g)
            me.groups = groups
            ws.current_user.me.return_value = me
        else:
            ws.current_user.me.side_effect = RuntimeError("user not found")
        return ws

    mock_ws_cls.side_effect = make_ws
    return mock_ws_cls


def _clear_role_cache():
    """Clear the module-level role cache between tests."""
    with _role_cache_lock:
        _role_cache.clear()


# ===========================================================================
# Unit tests for _resolve_role
# ===========================================================================


class TestResolveRole:
    """Tests for the private _resolve_role helper.

    Resolution walks ``user -> groups``: the workspace SCIM ``users.get``
    returns the user's group memberships (workspace-local groups AND
    account-level groups assigned to the workspace) on the ``.groups``
    attribute.  The resolver matches the admin group by display name.
    """

    @patch("databricks.sdk.WorkspaceClient")
    def test_returns_admin_when_user_in_group(self, mock_ws_cls):
        _wire_ws(
            mock_ws_cls,
            user_lookup={"admin@example.com": "u-1"},
            user_groups_by_id={"u-1": ["devmirror-admins", "engineers"]},
        )
        with patch.dict("os.environ", {"DEVMIRROR_ADMIN_GROUP": "devmirror-admins"}):
            result = _resolve_role("admin@example.com", "fake-user-token")
        assert result == "admin"

    @patch("databricks.sdk.WorkspaceClient")
    def test_returns_user_when_user_not_in_group(self, mock_ws_cls):
        _wire_ws(
            mock_ws_cls,
            user_lookup={"nonmember@example.com": "u-2"},
            user_groups_by_id={"u-2": ["engineers", "users"]},
        )
        with patch.dict("os.environ", {"DEVMIRROR_ADMIN_GROUP": "devmirror-admins"}):
            result = _resolve_role("nonmember@example.com", "fake-user-token")
        assert result == "user"

    @patch("databricks.sdk.WorkspaceClient")
    def test_returns_user_when_email_unknown(self, mock_ws_cls):
        # users.list returns nothing -> we cannot resolve user_id and
        # therefore default to "user".
        _wire_ws(mock_ws_cls, user_lookup={})
        result = _resolve_role("anyone@example.com", "fake-user-token")
        assert result == "user"

    @patch("databricks.sdk.WorkspaceClient")
    def test_returns_user_when_user_has_no_groups(self, mock_ws_cls):
        _wire_ws(
            mock_ws_cls,
            user_lookup={"orphan@example.com": "u-3"},
            user_groups_by_id={"u-3": []},
        )
        result = _resolve_role("orphan@example.com", "fake-user-token")
        assert result == "user"

    @patch("databricks.sdk.WorkspaceClient")
    def test_returns_user_when_sdk_raises(self, mock_ws_cls):
        mock_ws_cls.side_effect = RuntimeError("SDK connection failed")
        result = _resolve_role("anyone@example.com", "fake-user-token")
        assert result == "user"

    @patch("databricks.sdk.WorkspaceClient")
    def test_admin_group_display_match_is_case_insensitive(self, mock_ws_cls):
        # Group display in the user's record may differ in case from the
        # configured admin group name -- match must be case-insensitive.
        _wire_ws(
            mock_ws_cls,
            user_lookup={"admin@example.com": "u-1"},
            user_groups_by_id={"u-1": ["DevMirror-Admins"]},
        )
        with patch.dict("os.environ", {"DEVMIRROR_ADMIN_GROUP": "devmirror-admins"}):
            result = _resolve_role("admin@example.com", "fake-user-token")
        assert result == "admin"

    @patch("databricks.sdk.WorkspaceClient")
    def test_account_level_group_resolves_admin(self, mock_ws_cls):
        # Regression: account-level groups assigned to the workspace
        # surface on ``user.groups`` even though ``groups.get(...).members``
        # is empty for them.  The resolver MUST find admin via user.groups.
        _wire_ws(
            mock_ws_cls,
            user_lookup={"jince.james.sp@dlh.de": "6272540133507519"},
            user_groups_by_id={
                "6272540133507519": [
                    "lhg-odp-adw-general-developer",
                    "lhg-odp-adw-support-admin",  # account-level admin group
                ],
            },
        )
        with patch.dict(
            "os.environ", {"DEVMIRROR_ADMIN_GROUP": "lhg-odp-adw-support-admin"},
        ):
            result = _resolve_role("jince.james.sp@dlh.de", "fake-user-token")
        assert result == "admin"

    @patch("databricks.sdk.WorkspaceClient")
    def test_non_email_input_skips_scim(self, mock_ws_cls):
        # Strict-email gate: non-email input must NOT touch SCIM at all
        # (defends against filter injection).
        ws = _wire_ws(mock_ws_cls, user_lookup={})
        result = _resolve_role("not an email", "fake-user-token")
        assert result == "user"
        ws.users.list.assert_not_called()

    @patch("databricks.sdk.WorkspaceClient")
    def test_admin_emails_bypass_grants_admin_without_scim(self, mock_ws_cls):
        # When DEVMIRROR_ADMIN_EMAILS lists the user's email, return
        # "admin" immediately and never touch SCIM.
        _wire_ws(mock_ws_cls, user_lookup={})
        with patch.dict(
            "os.environ",
            {"DEVMIRROR_ADMIN_EMAILS": "alice@x.com,jince.james.sp@dlh.de"},
        ):
            result = _resolve_role("jince.james.sp@dlh.de", "fake-user-token")
        assert result == "admin"
        # SCIM was never consulted (no WorkspaceClient call).
        mock_ws_cls.assert_not_called()

    @patch("databricks.sdk.WorkspaceClient")
    def test_admin_emails_bypass_is_case_insensitive(self, mock_ws_cls):
        _wire_ws(mock_ws_cls, user_lookup={})
        with patch.dict(
            "os.environ", {"DEVMIRROR_ADMIN_EMAILS": "Jince.James.Sp@DLH.de"},
        ):
            result = _resolve_role("jince.james.sp@dlh.de", "fake-user-token")
        assert result == "admin"

    @patch("databricks.sdk.WorkspaceClient")
    def test_admin_emails_bypass_falls_through_when_not_listed(self, mock_ws_cls):
        # User isn't in the static list -> SCIM path runs; without admin
        # group membership the result is "user".
        _wire_ws(
            mock_ws_cls,
            user_lookup={"someone@dlh.de": "u-2"},
            user_groups_by_id={"u-2": ["engineers"]},
        )
        with patch.dict(
            "os.environ",
            {"DEVMIRROR_ADMIN_EMAILS": "alice@x.com",
             "DEVMIRROR_ADMIN_GROUP": "lhg-odp-adw-support-admin"},
        ):
            result = _resolve_role("someone@dlh.de", "fake-user-token")
        assert result == "user"

    @patch("databricks.sdk.WorkspaceClient")
    def test_admin_emails_handles_whitespace_and_blanks(self, mock_ws_cls):
        # The parser tolerates surrounding whitespace and empty entries.
        _wire_ws(mock_ws_cls, user_lookup={})
        with patch.dict(
            "os.environ",
            {"DEVMIRROR_ADMIN_EMAILS": " ,  jince.james.sp@dlh.de  ,, alice@x.com ,"},
        ):
            result = _resolve_role("jince.james.sp@dlh.de", "fake-user-token")
        assert result == "admin"


# ===========================================================================
# Unit tests for get_user_role (caching behaviour)
# ===========================================================================


class TestGetUserRoleCache:
    """Tests for the caching layer in get_user_role."""

    def setup_method(self):
        _clear_role_cache()

    def teardown_method(self):
        _clear_role_cache()

    @patch("backend.auth._resolve_role", return_value="admin")
    def test_cache_populated_on_first_call(self, mock_resolve):
        request = MagicMock()
        # Distinct values for the two headers we read.
        def _hdr(name, default=None):
            if name == "X-Forwarded-Email":
                return "first@example.com"
            if name == "X-Forwarded-Access-Token":
                return "fake-user-token"
            return default
        request.headers.get.side_effect = _hdr

        role = get_user_role(request)
        assert role == "admin"
        mock_resolve.assert_called_once_with("first@example.com", "fake-user-token")

        with _role_cache_lock:
            assert "first@example.com" in _role_cache

    @patch("backend.auth._resolve_role", return_value="admin")
    def test_cached_value_returned_within_ttl(self, mock_resolve):
        request = MagicMock()
        request.headers.get.return_value = "cached@example.com"

        # First call populates the cache
        role1 = get_user_role(request)
        assert role1 == "admin"
        assert mock_resolve.call_count == 1

        # Second call should hit cache, NOT call _resolve_role again
        role2 = get_user_role(request)
        assert role2 == "admin"
        assert mock_resolve.call_count == 1  # still just 1

    @patch("backend.auth._resolve_role", return_value="user")
    @patch("backend.auth.time")
    def test_cache_refreshes_after_ttl(self, mock_time, mock_resolve):
        request = MagicMock()
        request.headers.get.return_value = "expire@example.com"

        # First call at t=0
        mock_time.time.return_value = 0.0
        role1 = get_user_role(request)
        assert role1 == "user"
        assert mock_resolve.call_count == 1

        # Second call at t=301 (past 300s TTL)
        mock_time.time.return_value = 301.0
        role2 = get_user_role(request)
        assert role2 == "user"
        assert mock_resolve.call_count == 2  # resolved again


# ===========================================================================
# Unit tests for require_admin
# ===========================================================================


class TestRequireAdmin:
    def test_passes_for_admin(self):
        # Should not raise
        require_admin(role="admin")

    def test_raises_403_for_user(self):
        with pytest.raises(HTTPException) as exc_info:
            require_admin(role="user")
        assert exc_info.value.status_code == 403
        assert "Admin" in exc_info.value.detail


# ===========================================================================
# Unit tests for require_owner_or_admin
# ===========================================================================


class TestRequireOwnerOrAdmin:
    def test_admin_passes_regardless_of_ownership(self):
        row = {"created_by": "other@example.com"}
        # Should not raise even though user != created_by
        require_owner_or_admin(row, user="admin@example.com", role="admin")

    def test_user_passes_when_owner(self):
        row = {"created_by": "owner@example.com"}
        require_owner_or_admin(row, user="owner@example.com", role="user")

    def test_user_raises_403_when_not_owner(self):
        row = {"created_by": "other@example.com"}
        with pytest.raises(HTTPException) as exc_info:
            require_owner_or_admin(row, user="notowner@example.com", role="user")
        assert exc_info.value.status_code == 403
        assert "access" in exc_info.value.detail.lower()


# ===========================================================================
# Endpoint tests for GET /api/me
# ===========================================================================


class TestCurrentUserEndpoint:
    """Tests for ``GET /api/me``."""

    def test_returns_admin_role(self, client):
        """client fixture defaults to role='admin'."""
        resp = client.get("/api/me")
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == "testuser@example.com"
        assert data["role"] == "admin"

    def test_returns_user_role(self, user_client):
        resp = user_client.get("/api/me")
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == "testuser@example.com"
        assert data["role"] == "user"

    def test_display_name_is_title_cased_prefix(self, client):
        resp = client.get("/api/me")
        data = resp.json()
        # "testuser@example.com" -> prefix "testuser" -> title "Testuser"
        assert data["display_name"] == "Testuser"

    def test_display_name_with_dots(self):
        """Email like 'john.doe@corp.com' -> 'John Doe'."""
        tc, _ = make_client(role="admin", email="john.doe@corp.com")
        with tc:
            resp = tc.get("/api/me")
        data = resp.json()
        assert data["display_name"] == "John Doe"
        app.dependency_overrides.clear()

    def test_display_name_with_underscores(self):
        """Email like 'jane_smith@corp.com' -> 'Jane Smith'."""
        tc, _ = make_client(role="user", email="jane_smith@corp.com")
        with tc:
            resp = tc.get("/api/me")
        data = resp.json()
        assert data["display_name"] == "Jane Smith"
        app.dependency_overrides.clear()

    def test_unknown_email_fallback(self):
        """When email is 'unknown' (no header), endpoint should still work."""
        tc, _ = make_client(role="user", email="unknown")
        with tc:
            resp = tc.get("/api/me")
        data = resp.json()
        assert data["email"] == "unknown"
        assert data["role"] == "user"
        # "unknown" has no "@", so display_name = "unknown".title() = "Unknown"
        assert data["display_name"] == "Unknown"
        app.dependency_overrides.clear()


# ===========================================================================
# UserInfo model tests
# ===========================================================================


class TestUserInfoModel:
    def test_serialization(self):
        info = UserInfo(email="test@example.com", role="admin", display_name="Test")
        d = info.model_dump()
        assert d == {"email": "test@example.com", "role": "admin", "display_name": "Test"}

    def test_json_roundtrip(self):
        info = UserInfo(email="a@b.com", role="user", display_name="A")
        raw = info.model_dump_json()
        restored = UserInfo.model_validate_json(raw)
        assert restored == info
