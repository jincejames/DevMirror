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

def _make_group(members: list[tuple[str, str]]):
    """Build a mock Databricks group object with the given (value, display) members."""
    group = MagicMock()
    member_mocks = []
    for value, display in members:
        m = MagicMock()
        m.value = value
        m.display = display
        member_mocks.append(m)
    group.members = member_mocks
    return group


def _clear_role_cache():
    """Clear the module-level role cache between tests."""
    with _role_cache_lock:
        _role_cache.clear()


# ===========================================================================
# Unit tests for _resolve_role
# ===========================================================================


class TestResolveRole:
    """Tests for the private _resolve_role helper."""

    @patch("databricks.sdk.WorkspaceClient")
    def test_returns_admin_when_email_in_group(self, mock_ws_cls):
        ws = mock_ws_cls.return_value
        ws.groups.list.return_value = [
            _make_group([("admin@example.com", "admin@example.com")])
        ]

        with patch.dict("os.environ", {"DEVMIRROR_ADMIN_GROUP": "devmirror-admins"}):
            result = _resolve_role("admin@example.com")

        assert result == "admin"

    @patch("databricks.sdk.WorkspaceClient")
    def test_returns_user_when_email_not_in_group(self, mock_ws_cls):
        ws = mock_ws_cls.return_value
        ws.groups.list.return_value = [
            _make_group([("other@example.com", "other@example.com")])
        ]

        result = _resolve_role("nonmember@example.com")
        assert result == "user"

    @patch("databricks.sdk.WorkspaceClient")
    def test_returns_user_when_group_not_found(self, mock_ws_cls):
        ws = mock_ws_cls.return_value
        ws.groups.list.return_value = []  # no groups found

        result = _resolve_role("anyone@example.com")
        assert result == "user"

    @patch("databricks.sdk.WorkspaceClient")
    def test_returns_user_when_sdk_raises(self, mock_ws_cls):
        mock_ws_cls.side_effect = RuntimeError("SDK connection failed")

        result = _resolve_role("anyone@example.com")
        assert result == "user"

    @patch("databricks.sdk.WorkspaceClient")
    def test_case_insensitive_email_matching(self, mock_ws_cls):
        ws = mock_ws_cls.return_value
        ws.groups.list.return_value = [
            _make_group([("Admin@Example.COM", "Admin@Example.COM")])
        ]

        result = _resolve_role("admin@example.com")
        assert result == "admin"

    @patch("databricks.sdk.WorkspaceClient")
    def test_matches_on_display_field(self, mock_ws_cls):
        """Member value is different but display matches the email."""
        ws = mock_ws_cls.return_value
        ws.groups.list.return_value = [
            _make_group([("some-id-123", "admin@example.com")])
        ]

        result = _resolve_role("admin@example.com")
        assert result == "admin"

    @patch("databricks.sdk.WorkspaceClient")
    def test_empty_members_returns_user(self, mock_ws_cls):
        """Group exists but has no members."""
        ws = mock_ws_cls.return_value
        group = MagicMock()
        group.members = None
        ws.groups.list.return_value = [group]

        result = _resolve_role("admin@example.com")
        assert result == "user"


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
        request.headers.get.return_value = "first@example.com"

        role = get_user_role(request)
        assert role == "admin"
        mock_resolve.assert_called_once_with("first@example.com")

        # Verify the cache now contains the entry
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
