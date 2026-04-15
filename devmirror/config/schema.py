"""DevMirror configuration: Pydantic models and YAML loader."""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator

DR_ID_PATTERN = re.compile(r"^DR-[0-9]+$")


class StreamRef(BaseModel):
    """A reference to a production Databricks Workflow or Pipeline."""

    name: str = Field(..., min_length=1)


class EnvironmentDev(BaseModel):
    """Dev environment configuration (always enabled)."""

    enabled: Literal[True] = True


class EnvironmentQA(BaseModel):
    """Optional QA environment configuration."""

    enabled: bool = False


class Environments(BaseModel):
    """Environment definitions for a development request."""

    dev: EnvironmentDev
    qa: EnvironmentQA | None = None


class DataRevision(BaseModel):
    """Snapshot policy for cloned data."""

    mode: Literal["latest", "version", "timestamp"]
    version: int | None = Field(default=None, ge=0)
    timestamp: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _check_conditional_fields(self) -> DataRevision:
        if self.mode == "version" and self.version is None:
            raise ValueError("'version' is required when mode is 'version'")
        if self.mode == "timestamp" and self.timestamp is None:
            raise ValueError("'timestamp' is required when mode is 'timestamp'")
        return self

    @field_validator("timestamp")
    @classmethod
    def _validate_timestamp_format(cls, v: str | None) -> str | None:
        if v is None:
            return v
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except (ValueError, AttributeError) as exc:
            raise ValueError(
                f"timestamp must be ISO 8601 format (e.g. '2026-04-01T00:00:00Z'), got: {v!r}"
            ) from exc
        return v


class Access(BaseModel):
    """Access control: who gets access to dev and qa environments."""

    developers: list[str] = Field(..., min_length=1)
    qa_users: list[str] | None = None

    @field_validator("developers")
    @classmethod
    def _non_empty_developer_entries(cls, v: list[str]) -> list[str]:
        for i, entry in enumerate(v):
            if not entry.strip():
                raise ValueError(f"developers[{i}] must not be blank")
        return v


class Lifecycle(BaseModel):
    """Lifecycle configuration: expiration and notification settings."""

    expiration_date: date
    notification_days_before: int = Field(default=7, ge=0)
    notification_recipients: list[str] | None = None

    @field_validator("expiration_date", mode="before")
    @classmethod
    def _parse_expiration_date(cls, v: str | date) -> date:
        if isinstance(v, date):
            return v
        try:
            return date.fromisoformat(v)
        except (ValueError, TypeError) as exc:
            raise ValueError(
                f"expiration_date must be ISO 8601 date (YYYY-MM-DD), got: {v!r}"
            ) from exc


class DevelopmentRequest(BaseModel):
    """Core development request definition."""

    dr_id: str
    description: str | None = None
    streams: list[StreamRef] = Field(..., min_length=1)
    additional_objects: list[str] | None = None
    environments: Environments
    data_revision: DataRevision
    access: Access
    lifecycle: Lifecycle

    @field_validator("dr_id")
    @classmethod
    def _validate_dr_id(cls, v: str) -> str:
        if not DR_ID_PATTERN.match(v):
            raise ValueError(
                f"dr_id must match pattern DR-<digits> (e.g. 'DR-1042'), got: {v!r}"
            )
        return v

    @field_validator("additional_objects")
    @classmethod
    def _validate_fqn_entries(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        for i, fqn in enumerate(v):
            parts = fqn.split(".")
            if len(parts) != 3 or any(not p.strip() for p in parts):
                raise ValueError(
                    f"additional_objects[{i}] must be a three-part fully qualified name "
                    f"(catalog.schema.object), got: {fqn!r}"
                )
        return v


class DevMirrorConfig(BaseModel):
    """Top-level config model wrapping the development request."""

    version: Literal["1.0"]
    development_request: DevelopmentRequest


# ---------------------------------------------------------------------------
# Config loader (merged from config/loader.py)
# ---------------------------------------------------------------------------


class DevMirrorConfigError(Exception):
    """Raised when a configuration file cannot be loaded or fails validation."""

    def __init__(self, message: str, file_path: Path | None = None) -> None:
        self.file_path = file_path
        prefix = f"{file_path}: " if file_path else ""
        super().__init__(f"{prefix}{message}")


def load_development_request(path: Path) -> DevMirrorConfig:
    """Load a development request config from a YAML file.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        Validated DevMirrorConfig instance.

    Raises:
        DevMirrorConfigError: If the file cannot be read, parsed, or fails validation.
    """
    path = Path(path)

    if not path.exists():
        raise DevMirrorConfigError(f"file not found: {path}", file_path=path)

    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise DevMirrorConfigError(f"cannot read file: {exc}", file_path=path) from exc

    try:
        raw_data = yaml.safe_load(raw_text)
    except yaml.YAMLError as exc:
        raise DevMirrorConfigError(f"YAML parse error: {exc}", file_path=path) from exc

    if not isinstance(raw_data, dict):
        raise DevMirrorConfigError(
            "expected a YAML mapping at the top level, "
            f"got {type(raw_data).__name__}",
            file_path=path,
        )

    try:
        return DevMirrorConfig.model_validate(raw_data)
    except ValidationError as exc:
        lines: list[str] = []
        for error in exc.errors():
            loc = " -> ".join(str(part) for part in error["loc"])
            lines.append(f"  - {loc}: {error['msg']}")
        errors = "\n".join(lines)
        raise DevMirrorConfigError(
            f"validation failed with {exc.error_count()} error(s):\n{errors}",
            file_path=path,
        ) from exc
