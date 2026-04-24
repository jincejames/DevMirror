"""Tests for devmirror.config (schema models and YAML loader)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from textwrap import dedent

import pytest
from pydantic import ValidationError

from devmirror.config.schema import (
    Access,
    DataRevision,
    DevelopmentRequest,
    DevMirrorConfig,
    DevMirrorConfigError,
    EnvironmentDev,
    EnvironmentQA,
    Environments,
    Lifecycle,
    StreamRef,
    load_development_request,
)

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _minimal_dr_dict(**overrides: object) -> dict:
    base = {
        "dr_id": "DR-100",
        "streams": [{"name": "stream_a"}],
        "environments": {"dev": {"enabled": True}},
        "data_revision": {"mode": "latest"},
        "access": {"developers": ["user@co.com"]},
        "lifecycle": {"expiration_date": "2099-12-31"},
    }
    base.update(overrides)
    return base


def _minimal_config_dict(**dr_overrides: object) -> dict:
    return {"version": "1.0", "development_request": _minimal_dr_dict(**dr_overrides)}


# ===========================================================================
# Schema model tests
# ===========================================================================

class TestStreamRef:
    def test_valid(self) -> None:
        assert StreamRef(name="my_pipeline").name == "my_pipeline"

    def test_empty_name_rejected(self) -> None:
        with pytest.raises(ValidationError, match="String should have at least 1 character"):
            StreamRef(name="")


class TestEnvironments:
    def test_dev_only(self) -> None:
        e = Environments(dev=EnvironmentDev())
        assert e.dev.enabled is True
        assert e.qa is None

    def test_dev_plus_qa(self) -> None:
        e = Environments(dev=EnvironmentDev(), qa=EnvironmentQA(enabled=True))
        assert e.qa is not None and e.qa.enabled is True

    def test_dev_enabled_must_be_true(self) -> None:
        with pytest.raises(ValidationError):
            EnvironmentDev(enabled=False)  # type: ignore[arg-type]


class TestDataRevision:
    def test_latest_mode(self) -> None:
        dr = DataRevision(mode="latest")
        assert dr.mode == "latest" and dr.version is None and dr.timestamp is None

    def test_version_mode_valid(self) -> None:
        assert DataRevision(mode="version", version=42).version == 42

    def test_version_mode_missing_version(self) -> None:
        with pytest.raises(ValidationError, match="'version' is required when mode is 'version'"):
            DataRevision(mode="version")

    def test_timestamp_mode_valid(self) -> None:
        assert DataRevision(mode="timestamp", timestamp="2026-04-01T00:00:00Z").timestamp == "2026-04-01T00:00:00Z"

    def test_timestamp_mode_missing_timestamp(self) -> None:
        with pytest.raises(ValidationError, match="'timestamp' is required when mode is 'timestamp'"):
            DataRevision(mode="timestamp")

    def test_timestamp_invalid_format(self) -> None:
        with pytest.raises(ValidationError, match="ISO 8601"):
            DataRevision(mode="timestamp", timestamp="not-a-date")

    def test_invalid_mode(self) -> None:
        with pytest.raises(ValidationError, match="Input should be 'latest', 'version' or 'timestamp'"):
            DataRevision(mode="snapshot")  # type: ignore[arg-type]


class TestAccess:
    def test_valid_with_qa(self) -> None:
        a = Access(developers=["dev@co.com"], qa_users=["qa@co.com"])
        assert a.qa_users == ["qa@co.com"]

    def test_empty_developers_rejected(self) -> None:
        with pytest.raises(ValidationError, match="at least 1"):
            Access(developers=[])

    def test_blank_developer_entry_rejected(self) -> None:
        with pytest.raises(ValidationError, match="must not be blank"):
            Access(developers=["  "])


class TestLifecycle:
    def test_valid_date_string(self) -> None:
        assert Lifecycle(expiration_date="2099-06-15").expiration_date == date(2099, 6, 15)

    def test_invalid_date_string(self) -> None:
        with pytest.raises(ValidationError, match="ISO 8601"):
            Lifecycle(expiration_date="June 15 2099")

    def test_defaults(self) -> None:
        lc = Lifecycle(expiration_date="2099-01-01")
        assert lc.notification_days_before == 7 and lc.notification_recipients is None


class TestDevelopmentRequest:
    def test_valid_minimal(self) -> None:
        dr = DevelopmentRequest(**_minimal_dr_dict())
        assert dr.dr_id == "DR-100"

    def test_valid_full(self) -> None:
        dr = DevelopmentRequest(**_minimal_dr_dict(
            description="Test DR",
            additional_objects=["cat.schema.table"],
            environments={"dev": {"enabled": True}, "qa": {"enabled": True}},
        ))
        assert dr.description == "Test DR"
        assert dr.environments.qa is not None

    def test_dr_id_valid_patterns(self) -> None:
        # Legacy DR-<digits> plus the new auto-generated format (US-34).
        valid_ids = [
            "DR-0", "DR-1", "DR-1042", "DR-999999",
            "DR00001", "DR12345", "ABC000", "PROJ00042",
        ]
        for dr_id in valid_ids:
            assert DevelopmentRequest(**_minimal_dr_dict(dr_id=dr_id)).dr_id == dr_id

    def test_dr_id_invalid_patterns(self) -> None:
        # These must match NEITHER the legacy DR-<digits> nor the new
        # auto-generated <PREFIX><zero-padded-digits> pattern.
        for bad_id in ["DR-", "dr-100", "WR-100", "DR-abc", "", "DR-12-34", "DR1", "1DR123"]:
            with pytest.raises(ValidationError, match="dr_id"):
                DevelopmentRequest(**_minimal_dr_dict(dr_id=bad_id))

    def test_empty_streams_rejected(self) -> None:
        with pytest.raises(ValidationError, match="at least 1"):
            DevelopmentRequest(**_minimal_dr_dict(streams=[]))

    def test_additional_objects_valid_fqn(self) -> None:
        dr = DevelopmentRequest(**_minimal_dr_dict(
            additional_objects=["cat.sch.tbl", "prod.marketing.dim"]
        ))
        assert len(dr.additional_objects) == 2

    def test_additional_objects_two_part_rejected(self) -> None:
        with pytest.raises(ValidationError, match="three-part"):
            DevelopmentRequest(**_minimal_dr_dict(additional_objects=["schema.table"]))

    def test_additional_objects_four_part_rejected(self) -> None:
        with pytest.raises(ValidationError, match="three-part"):
            DevelopmentRequest(**_minimal_dr_dict(additional_objects=["a.b.c.d"]))


class TestDevMirrorConfig:
    def test_valid_minimal(self) -> None:
        cfg = DevMirrorConfig(**_minimal_config_dict())
        assert cfg.version == "1.0"

    def test_wrong_version_string(self) -> None:
        d = _minimal_config_dict()
        d["version"] = "2.0"
        with pytest.raises(ValidationError, match=r"Input should be '1\.0'"):
            DevMirrorConfig.model_validate(d)

    def test_missing_development_request(self) -> None:
        with pytest.raises(ValidationError, match="development_request"):
            DevMirrorConfig.model_validate({"version": "1.0"})


# ===========================================================================
# Loader tests
# ===========================================================================

class TestLoadValidConfigs:
    def test_minimal(self) -> None:
        cfg = load_development_request(FIXTURES / "valid_minimal.yaml")
        assert cfg.development_request.dr_id == "DR-1042"
        assert cfg.development_request.data_revision.mode == "latest"

    def test_full(self) -> None:
        cfg = load_development_request(FIXTURES / "valid_full.yaml")
        dr = cfg.development_request
        assert dr.dr_id == "DR-9999"
        assert len(dr.streams) == 2
        assert dr.environments.qa is not None
        assert dr.data_revision.mode == "timestamp"

    def test_version_revision(self) -> None:
        cfg = load_development_request(FIXTURES / "valid_version_revision.yaml")
        assert cfg.development_request.data_revision.version == 55


class TestLoadErrors:
    def test_file_not_found(self) -> None:
        with pytest.raises(DevMirrorConfigError, match="file not found"):
            load_development_request(Path("/nonexistent/path/config.yaml"))

    def test_invalid_yaml_syntax(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text("version: [unclosed", encoding="utf-8")
        with pytest.raises(DevMirrorConfigError, match="YAML parse error"):
            load_development_request(bad)

    def test_non_mapping_yaml(self, tmp_path: Path) -> None:
        scalar = tmp_path / "scalar.yaml"
        scalar.write_text("just a string", encoding="utf-8")
        with pytest.raises(DevMirrorConfigError, match="expected a YAML mapping"):
            load_development_request(scalar)

    def test_validation_error_includes_path(self, tmp_path: Path) -> None:
        invalid = tmp_path / "invalid.yaml"
        invalid.write_text(
            dedent("""\
                version: "1.0"
                development_request:
                  dr_id: "bad-id"
                  streams: []
                  environments:
                    dev:
                      enabled: true
                  data_revision:
                    mode: "latest"
                  access:
                    developers:
                      - "dev@co.com"
                  lifecycle:
                    expiration_date: "2099-01-01"
            """),
            encoding="utf-8",
        )
        with pytest.raises(DevMirrorConfigError) as exc_info:
            load_development_request(invalid)
        assert str(invalid) in str(exc_info.value)

    def test_missing_required_fields(self, tmp_path: Path) -> None:
        incomplete = tmp_path / "incomplete.yaml"
        incomplete.write_text(
            dedent("""\
                version: "1.0"
                development_request:
                  dr_id: "DR-1"
            """),
            encoding="utf-8",
        )
        with pytest.raises(DevMirrorConfigError, match="validation failed"):
            load_development_request(incomplete)

    def test_empty_file(self, tmp_path: Path) -> None:
        empty = tmp_path / "empty.yaml"
        empty.write_text("", encoding="utf-8")
        with pytest.raises(DevMirrorConfigError, match="expected a YAML mapping"):
            load_development_request(empty)


class TestDevMirrorConfigError:
    def test_file_path_in_exception(self) -> None:
        err = DevMirrorConfigError("something broke", file_path=Path("/a/b.yaml"))
        assert "/a/b.yaml" in str(err)

    def test_no_file_path(self) -> None:
        assert str(DevMirrorConfigError("something broke")) == "something broke"
