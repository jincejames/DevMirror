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

    def test_empty_streams_rejected_without_additional_objects(self):
        with pytest.raises(ValidationError) as exc_info:
            _minimal_config_in(streams=[])
        # Either "stream" or "additional object" should appear in the
        # error since both are rejected together.
        msg = str(exc_info.value).lower()
        assert "stream" in msg or "additional object" in msg

    def test_empty_streams_allowed_with_additional_objects(self):
        # No exception -- the pair (streams=[], additional_objects=[...])
        # is a valid submission.
        config_in = _minimal_config_in(
            streams=[], additional_objects=["cat.sch.tbl"],
        )
        assert config_in.streams == []
        assert config_in.additional_objects == ["cat.sch.tbl"]

    def test_both_streams_and_additional_objects_empty_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            _minimal_config_in(streams=[], additional_objects=[])
        assert "stream" in str(exc_info.value).lower() or "additional object" in str(exc_info.value).lower()

    def test_empty_developers_rejected(self):
        with pytest.raises(ValidationError) as exc_info:
            _minimal_config_in(developers=[])
        assert "developer" in str(exc_info.value).lower()


class TestAdditionalObjectsNormalization:
    """ConfigIn.additional_objects accepts a list[str] (UI default) OR
    a single string with newline/comma separators (direct-API clients).
    Whitespace is trimmed; blanks are dropped.  Keeps the input pipeline
    flexible without forcing every caller to pre-parse.
    """

    def test_list_passes_through(self):
        c = _minimal_config_in(
            additional_objects=["cat.sch.t1", "cat.sch.t2"],
        )
        assert c.additional_objects == ["cat.sch.t1", "cat.sch.t2"]

    def test_list_trims_and_drops_blanks(self):
        c = _minimal_config_in(
            additional_objects=["  cat.sch.t1  ", "", "  ", "cat.sch.t2"],
        )
        assert c.additional_objects == ["cat.sch.t1", "cat.sch.t2"]

    def test_comma_separated_string(self):
        c = _minimal_config_in(
            additional_objects="cat.sch.t1, cat.sch.t2,cat.sch.t3",
        )
        assert c.additional_objects == ["cat.sch.t1", "cat.sch.t2", "cat.sch.t3"]

    def test_newline_separated_string(self):
        c = _minimal_config_in(
            additional_objects="cat.sch.t1\ncat.sch.t2\ncat.sch.t3",
        )
        assert c.additional_objects == ["cat.sch.t1", "cat.sch.t2", "cat.sch.t3"]

    def test_mixed_separators(self):
        # User pasted from a SQL editor or spreadsheet -- both forms in
        # one payload.  Repo can recover the clean list.
        c = _minimal_config_in(
            additional_objects=(
                "cat.sch.t1, cat.sch.t2\n"
                "cat.sch.t3,cat.sch.t4\n"
                "\n"
                "cat.sch.t5"
            ),
        )
        assert c.additional_objects == [
            "cat.sch.t1", "cat.sch.t2",
            "cat.sch.t3", "cat.sch.t4",
            "cat.sch.t5",
        ]

    def test_string_with_only_blanks_treated_as_empty(self):
        # All-whitespace string -> [] -> rejected by the streams-or-
        # additional-objects model validator (when streams is also empty).
        c = _minimal_config_in(additional_objects="   \n\n  , , ")
        assert c.additional_objects == []

    def test_none_passes_through(self):
        c = _minimal_config_in(additional_objects=None)
        assert c.additional_objects is None


class TestTargetCatalogValidation:
    """target_catalog must be a bare Databricks identifier or unset.

    Without this guard a typo (hyphen, space, dot) sails through submit
    and only fails mid-clone with an opaque Spark parser error -- the
    user has already waited for provisioning to start before seeing it.
    """

    def test_unset_is_allowed(self):
        # Default (None) means "use per-object base+suffix routing".
        assert _minimal_config_in().target_catalog is None

    def test_valid_identifier(self):
        c = _minimal_config_in(target_catalog="odp_adw_sandbox_n")
        assert c.target_catalog == "odp_adw_sandbox_n"

    def test_leading_underscore_allowed(self):
        c = _minimal_config_in(target_catalog="_internal_n")
        assert c.target_catalog == "_internal_n"

    def test_blank_string_treated_as_unset(self):
        # "" / "   " collapse to None -- submitting an empty form field
        # shouldn't activate the override.
        assert _minimal_config_in(target_catalog="").target_catalog is None
        assert _minimal_config_in(target_catalog="   ").target_catalog is None

    def test_strips_whitespace(self):
        c = _minimal_config_in(target_catalog="  odp_adw_sandbox_n  ")
        assert c.target_catalog == "odp_adw_sandbox_n"

    def test_hyphen_rejected(self):
        with pytest.raises(ValidationError, match="bare Databricks identifier"):
            _minimal_config_in(target_catalog="odp-adw-sandbox-n")

    def test_space_rejected(self):
        with pytest.raises(ValidationError, match="bare Databricks identifier"):
            _minimal_config_in(target_catalog="my catalog")

    def test_dot_rejected(self):
        # Three-part FQN is wrong here -- target_catalog is a CATALOG name,
        # not a fully-qualified table.
        with pytest.raises(ValidationError, match="bare Databricks identifier"):
            _minimal_config_in(target_catalog="cat.schema.table")

    def test_starts_with_digit_rejected(self):
        with pytest.raises(ValidationError, match="bare Databricks identifier"):
            _minimal_config_in(target_catalog="42_catalog")
