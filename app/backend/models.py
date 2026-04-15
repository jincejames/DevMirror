"""Pydantic models for the DevMirror web API."""

from __future__ import annotations

from pydantic import BaseModel, field_validator

from devmirror.config.schema import (
    Access,
    DataRevision,
    DevelopmentRequest,
    DevMirrorConfig,
    EnvironmentDev,
    EnvironmentQA,
    Environments,
    Lifecycle,
    StreamRef,
)


class ConfigIn(BaseModel):
    """Flat form input that maps to a nested DevMirrorConfig."""

    dr_id: str
    description: str | None = None
    streams: list[str]
    additional_objects: list[str] | None = None
    target_catalog: str | None = None
    qa_enabled: bool = False
    data_revision_mode: str = "latest"
    data_revision_version: int | None = None
    data_revision_timestamp: str | None = None
    developers: list[str]
    qa_users: list[str] | None = None
    expiration_date: str
    notification_days_before: int = 7
    notification_recipients: list[str] | None = None

    @field_validator("streams")
    @classmethod
    def _at_least_one_stream(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("At least one stream is required")
        return v

    @field_validator("developers")
    @classmethod
    def _at_least_one_developer(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("At least one developer is required")
        return v

    def to_devmirror_config(self) -> DevMirrorConfig:
        """Convert flat form fields into a nested DevMirrorConfig."""
        stream_refs = [StreamRef(name=s) for s in self.streams]

        qa_env = EnvironmentQA(enabled=True) if self.qa_enabled else EnvironmentQA(enabled=False)
        environments = Environments(dev=EnvironmentDev(), qa=qa_env)

        data_revision = DataRevision(
            mode=self.data_revision_mode,  # type: ignore[arg-type]
            version=self.data_revision_version,
            timestamp=self.data_revision_timestamp,
        )

        access = Access(
            developers=self.developers,
            qa_users=self.qa_users,
        )

        lifecycle = Lifecycle(
            expiration_date=self.expiration_date,
            notification_days_before=self.notification_days_before,
            notification_recipients=self.notification_recipients,
        )

        dr = DevelopmentRequest(
            dr_id=self.dr_id,
            description=self.description,
            streams=stream_refs,
            additional_objects=self.additional_objects,
            environments=environments,
            data_revision=data_revision,
            access=access,
            lifecycle=lifecycle,
        )

        return DevMirrorConfig(version="1.0", development_request=dr)


class FieldError(BaseModel):
    """A single validation error with location and message."""

    loc: list[str]
    msg: str


class ValidationResult(BaseModel):
    """Result of a validation check."""

    status: str
    errors: list[FieldError]


class ConfigOut(BaseModel):
    """Full config detail response."""

    dr_id: str
    description: str | None
    status: str
    config: ConfigIn
    validation_errors: list[FieldError]
    created_at: str
    created_by: str
    updated_at: str | None
    expiration_date: str


class ConfigListItem(BaseModel):
    """Summary item for config listing."""

    dr_id: str
    description: str | None
    status: str
    created_at: str
    created_by: str
    expiration_date: str


class ConfigListResponse(BaseModel):
    """Response for listing configs."""

    configs: list[ConfigListItem]
    total: int


class StreamSearchResult(BaseModel):
    """A single stream search result."""

    name: str
    type: str  # "job" or "pipeline"


class StreamSearchResponse(BaseModel):
    """Response for stream search."""

    results: list[StreamSearchResult]


# ---- Stage 2 models ----


class ScanResponse(BaseModel):
    """Response from a scan operation."""

    dr_id: str
    status: str
    manifest: dict


class ManifestResponse(BaseModel):
    """Response for retrieving a stored manifest."""

    dr_id: str
    manifest: dict
    scanned_at: str | None


class ProvisionStartResponse(BaseModel):
    """Response when provisioning is initiated (202)."""

    dr_id: str
    task_id: str
    status: str
    message: str


class TaskStatusResponse(BaseModel):
    """Response for polling a background task."""

    task_id: str
    dr_id: str
    task_type: str
    status: str
    progress: str
    result: dict | None
    error: str | None
    started_at: str
    completed_at: str | None


class DrStatusResponse(BaseModel):
    """Full lifecycle status of a provisioned DR."""

    dr_id: str
    status: str
    description: str | None
    expiration_date: str
    created_at: str
    created_by: str
    last_refreshed_at: str | None
    objects: list[dict]
    total_objects: int
    object_breakdown: dict[str, int]
    recent_audit: list[dict]


class DrListItem(BaseModel):
    """Summary item for the DR list."""

    dr_id: str
    status: str
    description: str | None
    expiration_date: str
    created_at: str
    created_by: str
    total_objects: int


class DrListResponse(BaseModel):
    """Response for listing provisioned DRs."""

    drs: list[DrListItem]
    total: int


class CleanupResponse(BaseModel):
    """Response from a cleanup operation."""

    dr_id: str
    final_status: str
    objects_dropped: int
    schemas_dropped: int
    revokes_succeeded: int


class RefreshRequest(BaseModel):
    """Body for a refresh request."""

    mode: str = "incremental"  # full, incremental, selective
    selected_objects: list[str] | None = None


class RefreshStartResponse(BaseModel):
    """Response when a refresh is initiated (202)."""

    dr_id: str
    task_id: str
    status: str
    message: str


class ModifyDrRequest(BaseModel):
    """Request body for modifying a provisioned DR."""

    new_expiration_date: str | None = None
    add_developers: list[str] | None = None
    remove_developers: list[str] | None = None
    add_qa_users: list[str] | None = None
    remove_qa_users: list[str] | None = None


class ModifyDrResponse(BaseModel):
    """Response from a DR modification."""

    dr_id: str
    status: str
    message: str
