"""Startup validation tests for the FastAPI ``lifespan``.

Stage 4 US-35 AC3/AC4 require that ``DEVMIRROR_DR_ID_PREFIX`` and
``DEVMIRROR_DR_ID_PADDING`` be read once at application startup and that an
invalid value causes the app to fail to start with a clear
:class:`SettingsError`.  These tests exercise the real ``lifespan`` context
manager (via :class:`fastapi.testclient.TestClient`) with environment
variables patched via ``monkeypatch.setenv`` so we can assert that startup
actually raises.
"""

from __future__ import annotations

import pytest
from backend.main import app
from fastapi.testclient import TestClient

from devmirror.settings import SettingsError


class TestStartupSettingsValidation:
    """AC3 + AC4: invalid env vars must surface at lifespan startup.

    The FastAPI ``TestClient`` context manager runs the ``lifespan``
    coroutine on ``__enter__``; if ``load_settings()`` raises, the error
    propagates out of the ``with`` statement.
    """

    def test_invalid_prefix_fails_startup(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A prefix starting with a digit must abort app startup."""
        monkeypatch.setenv("DEVMIRROR_DR_ID_PREFIX", "1bad")
        with pytest.raises(SettingsError, match=r"DEVMIRROR_DR_ID_PREFIX"):
            with TestClient(app):
                pass  # pragma: no cover - startup should raise

    def test_prefix_too_long_fails_startup(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """A 9-character prefix exceeds the 8-char limit and must abort startup."""
        monkeypatch.setenv("DEVMIRROR_DR_ID_PREFIX", "ABC456789")
        with pytest.raises(SettingsError, match=r"max 8 characters"):
            with TestClient(app):
                pass  # pragma: no cover

    def test_prefix_with_hyphen_fails_startup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Non-alphanumeric characters must abort startup."""
        monkeypatch.setenv("DEVMIRROR_DR_ID_PREFIX", "DR-1")
        with pytest.raises(SettingsError, match=r"alphanumeric"):
            with TestClient(app):
                pass  # pragma: no cover

    def test_padding_below_range_fails_startup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Padding = 2 is below the minimum of 3 -- must abort startup."""
        monkeypatch.setenv("DEVMIRROR_DR_ID_PADDING", "2")
        with pytest.raises(SettingsError, match=r"between 3 and 12 inclusive"):
            with TestClient(app):
                pass  # pragma: no cover

    def test_padding_above_range_fails_startup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Padding = 13 is above the maximum of 12 -- must abort startup."""
        monkeypatch.setenv("DEVMIRROR_DR_ID_PADDING", "13")
        with pytest.raises(SettingsError, match=r"between 3 and 12 inclusive"):
            with TestClient(app):
                pass  # pragma: no cover

    def test_padding_non_integer_fails_startup(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A non-integer padding must surface the ``must be an integer`` error."""
        monkeypatch.setenv("DEVMIRROR_DR_ID_PADDING", "not-a-number")
        with pytest.raises(SettingsError, match=r"must be an integer"):
            with TestClient(app):
                pass  # pragma: no cover

    def test_padding_exact_min_starts_ok(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Padding = 3 (the inclusive minimum) must start cleanly."""
        monkeypatch.setenv("DEVMIRROR_DR_ID_PADDING", "3")
        monkeypatch.setenv("DEVMIRROR_DR_ID_PREFIX", "DR")
        with TestClient(app) as client:
            assert client.get("/api/health").status_code == 200
            assert app.state.settings.dr_id_padding == 3

    def test_padding_exact_max_starts_ok(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Padding = 12 (the inclusive maximum) must start cleanly."""
        monkeypatch.setenv("DEVMIRROR_DR_ID_PADDING", "12")
        monkeypatch.setenv("DEVMIRROR_DR_ID_PREFIX", "DR")
        with TestClient(app) as client:
            assert client.get("/api/health").status_code == 200
            assert app.state.settings.dr_id_padding == 12

    def test_prefix_exact_8_char_boundary_starts_ok(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """An 8-char alphanumeric prefix (the inclusive boundary) must start cleanly."""
        monkeypatch.setenv("DEVMIRROR_DR_ID_PREFIX", "AB345678")
        with TestClient(app) as client:
            assert client.get("/api/health").status_code == 200
            assert app.state.settings.dr_id_prefix == "AB345678"
