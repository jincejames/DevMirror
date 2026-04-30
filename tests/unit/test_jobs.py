"""Tests for devmirror.jobs entry-point helpers."""

from __future__ import annotations

import os
import sys
from unittest import mock

import pytest

from devmirror.jobs import _apply_overrides_from_argv
from devmirror.settings import load_settings


@pytest.fixture
def clean_env(monkeypatch):
    """Strip DEVMIRROR_* env vars so each test starts from a known baseline."""
    for key in list(os.environ):
        if key.startswith("DEVMIRROR_"):
            monkeypatch.delenv(key, raising=False)
    yield monkeypatch


class TestApplyOverridesFromArgv:
    def test_named_parameters_promoted_to_env_vars(self, clean_env) -> None:
        with mock.patch.object(
            sys, "argv",
            [
                "devmirror-cleanup",
                "--catalog=odp_adw_support_n",
                "--schema=devmirror",
                "--warehouse-id=bda66ae121230af9",
            ],
        ):
            _apply_overrides_from_argv()

        assert os.environ["DEVMIRROR_CONTROL_CATALOG"] == "odp_adw_support_n"
        assert os.environ["DEVMIRROR_CONTROL_SCHEMA"] == "devmirror"
        assert os.environ["DEVMIRROR_WAREHOUSE_ID"] == "bda66ae121230af9"

        # And load_settings() picks them up as the control FQN.
        s = load_settings()
        assert s.control_fqn_prefix == "odp_adw_support_n.devmirror"
        assert s.warehouse_id == "bda66ae121230af9"

    def test_no_args_is_a_noop(self, clean_env) -> None:
        with mock.patch.object(sys, "argv", ["devmirror-cleanup"]):
            _apply_overrides_from_argv()

        # Defaults remain unchanged.
        assert "DEVMIRROR_CONTROL_CATALOG" not in os.environ
        assert "DEVMIRROR_CONTROL_SCHEMA" not in os.environ
        assert "DEVMIRROR_WAREHOUSE_ID" not in os.environ

        s = load_settings()
        assert s.control_catalog == "dev_analytics"
        assert s.control_schema == "devmirror_admin"

    def test_preexisting_env_var_overridden_by_cli(self, clean_env) -> None:
        # If both env var AND CLI flag are set, CLI wins (the shim writes
        # AFTER reading argv, so the most-explicit signal -- the one
        # passed to this exact run -- always lands).
        clean_env.setenv("DEVMIRROR_CONTROL_CATALOG", "from_env")
        clean_env.setenv("DEVMIRROR_CONTROL_SCHEMA", "from_env")
        with mock.patch.object(
            sys, "argv",
            ["devmirror-cleanup", "--catalog=from_cli", "--schema=from_cli"],
        ):
            _apply_overrides_from_argv()

        assert os.environ["DEVMIRROR_CONTROL_CATALOG"] == "from_cli"
        assert os.environ["DEVMIRROR_CONTROL_SCHEMA"] == "from_cli"

    def test_unknown_args_ignored(self, clean_env) -> None:
        # parse_known_args swallows unrelated flags so existing CLI calls
        # that pass other flags still work.
        with mock.patch.object(
            sys, "argv",
            ["devmirror-cleanup", "--catalog=foo", "--unknown=bar", "positional"],
        ):
            _apply_overrides_from_argv()

        assert os.environ["DEVMIRROR_CONTROL_CATALOG"] == "foo"
