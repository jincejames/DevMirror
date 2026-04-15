"""Tests for ConfigIn.to_devmirror_config() and model validation."""

from __future__ import annotations

from datetime import date, timedelta

import pytest
from backend.models import ConfigIn
from pydantic import ValidationError


def _future_date(days: int = 30) -> str:
    return (date.today() + timedelta(days=days)).isoformat()


def _minimal_config_in(**overrides) -> ConfigIn:
    defaults = {
        "dr_id": "DR-1042",
        "streams": ["my-job-1"],
        "developers": ["dev@example.com"],
        "expiration_date": _future_date(30),
    }
    defaults.update(overrides)
    return ConfigIn(**defaults)


class TestConfigInToDevMirrorConfig:
    """Tests for the to_devmirror_config() conversion."""

    def test_minimal_valid(self):
        config_in = _minimal_config_in()
        dm = config_in.to_devmirror_config()

        assert dm.version == "1.0"
        dr = dm.development_request
        assert dr.dr_id == "DR-1042"
        assert len(dr.streams) == 1
        assert dr.streams[0].name == "my-job-1"
        assert dr.environments.dev.enabled is True
        assert dr.environments.qa is not None
        assert dr.environments.qa.enabled is False
        assert dr.data_revision.mode == "latest"
        assert dr.access.developers == ["dev@example.com"]
        assert dr.lifecycle.expiration_date == date.fromisoformat(config_in.expiration_date)

    def test_qa_enabled(self):
        config_in = _minimal_config_in(
            qa_enabled=True,
            qa_users=["qa@example.com"],
        )
        dm = config_in.to_devmirror_config()
        assert dm.development_request.environments.qa is not None
        assert dm.development_request.environments.qa.enabled is True
        assert dm.development_request.access.qa_users == ["qa@example.com"]

    def test_version_revision(self):
        config_in = _minimal_config_in(
            data_revision_mode="version",
            data_revision_version=42,
        )
        dm = config_in.to_devmirror_config()
        assert dm.development_request.data_revision.mode == "version"
        assert dm.development_request.data_revision.version == 42

    def test_timestamp_revision(self):
        config_in = _minimal_config_in(
            data_revision_mode="timestamp",
            data_revision_timestamp="2026-04-01T00:00:00Z",
        )
        dm = config_in.to_devmirror_config()
        assert dm.development_request.data_revision.mode == "timestamp"
        assert dm.development_request.data_revision.timestamp == "2026-04-01T00:00:00Z"

    def test_additional_objects(self):
        config_in = _minimal_config_in(
            additional_objects=["catalog.schema.table1", "catalog.schema.table2"],
        )
        dm = config_in.to_devmirror_config()
        assert dm.development_request.additional_objects == [
            "catalog.schema.table1",
            "catalog.schema.table2",
        ]

    def test_invalid_dr_id_raises(self):
        config_in = _minimal_config_in(dr_id="INVALID-ID")
        with pytest.raises(ValidationError) as exc_info:
            config_in.to_devmirror_config()
        assert "dr_id" in str(exc_info.value)

    def test_description_passed_through(self):
        config_in = _minimal_config_in(description="Test description")
        dm = config_in.to_devmirror_config()
        assert dm.development_request.description == "Test description"

    def test_notification_settings(self):
        config_in = _minimal_config_in(
            notification_days_before=14,
            notification_recipients=["admin@example.com"],
        )
        dm = config_in.to_devmirror_config()
        assert dm.development_request.lifecycle.notification_days_before == 14
        assert dm.development_request.lifecycle.notification_recipients == ["admin@example.com"]


class TestConfigInValidation:
    """Tests for ConfigIn's own Pydantic validators."""

    def test_empty_streams_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            _minimal_config_in(streams=[])
        assert "stream" in str(exc_info.value).lower()

    def test_empty_developers_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            _minimal_config_in(developers=[])
        assert "developer" in str(exc_info.value).lower()
