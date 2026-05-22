"""Pydantic models for the DevMirror web API."""

from __future__ import annotations

import re

from pydantic import (
    AliasChoices,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

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
    """Flat form input that maps to a nested DevMirrorConfig.

    ``dr_id`` is intentionally optional on the input model: the server
    assigns it at ``POST /api/configs`` time (see Stage 4 US-34).  It is
    populated before :meth:`to_devmirror_config` is called, so the nested
    ``DevelopmentRequest`` still carries a concrete value.

    ``extra="forbid"``: unknown / typo'd field names are rejected with a
    422 instead of silently ignored.  This catches client typos and any
    tampered config_json read back from the DB.
    """

    # `populate_by_name=True` lets the canonical field name (`uat_users`)
    # work as input alongside the `validation_alias` choices below, so
    # both legacy `qa_users` keys (from configs created pre-rename) and
    # new `uat_users` keys parse cleanly into the same field.
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    dr_id: str | None = None
    # Description is mandatory and must be at least 5 non-blank characters.
    # Validation runs server-side at create / update / scan time so direct
    # API callers can't sidestep the UI's required-field affordance.
    description: str
    streams: list[str]
    additional_objects: list[str] | None = None
    # Optional explicit override.  When set, every object in the DR is
    # cloned into this catalog regardless of its source base.  Leave
    # unset to use the per-object base+suffix routing (LH default).
    target_catalog: str | None = None
    qa_enabled: bool = False
    data_revision_mode: str = "latest"
    data_revision_version: int | None = None
    data_revision_timestamp: str | None = None
    developers: list[str]
    # `qa_users` is accepted for backward compatibility with config rows
    # written before the UAT rename (they still have the legacy key in
    # `config_json`).  Once any such row is re-saved, the canonical
    # `uat_users` key wins on output, so storage gradually self-canonicalises.
    uat_users: list[str] | None = Field(
        default=None,
        validation_alias=AliasChoices("uat_users", "qa_users"),
    )
    expiration_date: str
    notification_days_before: int = 7
    notification_recipients: list[str] | None = None

    @model_validator(mode="after")
    def _streams_or_additional_objects(self) -> "ConfigIn":
        # A DR must scope at least one source: either a stream (job /
        # pipeline whose lineage we'll walk) or one or more explicit
        # additional_objects FQNs.  Empty-everything is rejected to
        # prevent accidental "clone nothing" submissions.
        if not self.streams and not (self.additional_objects or []):
            raise ValueError(
                "At least one stream OR one additional object is required"
            )
        return self

    @field_validator("developers")
    @classmethod
    def _at_least_one_developer(cls, v: list[str]) -> list[str]:
        if not v:
            raise ValueError("At least one developer is required")
        return v

    @field_validator("description", mode="before")
    @classmethod
    def _description_min_length(cls, v) -> str:
        # Mandatory: must have at least 5 non-blank characters.  Trimming
        # before the length check rejects "     " (whitespace-only) the
        # same way it rejects empty / None inputs.  The trimmed value is
        # what gets persisted so storage stays clean.
        if v is None or not isinstance(v, str) or not v.strip():
            raise ValueError("Description is required.")
        trimmed = v.strip()
        if len(trimmed) < 5:
            raise ValueError("Description must be at least 5 characters.")
        return trimmed

    @field_validator("additional_objects", mode="before")
    @classmethod
    def _normalize_additional_objects(cls, v):
        # Accept either:
        #   - None / missing field           -> None
        #   - list[str]                      -> trimmed, blanks dropped
        #   - str (possibly multi-line,
        #     comma- or newline-separated)   -> parsed into list
        # The UI sends the cleaned list directly, but direct-API callers
        # (and copy-pasted payloads from a SQL workbench) commonly send a
        # single string with mixed separators.  Normalising here keeps
        # all clients DRY and lets a request of either shape succeed.
        if v is None:
            return None
        if isinstance(v, str):
            tokens = re.split(r"[,\n]", v)
            return [t.strip() for t in tokens if t.strip()]
        if isinstance(v, list):
            return [str(t).strip() for t in v if str(t).strip()]
        return v

    @field_validator("target_catalog")
    @classmethod
    def _validate_target_catalog(cls, v: str | None) -> str | None:
        # None / empty / whitespace -> treat as unset so the per-object
        # base+suffix routing kicks in.  Otherwise enforce a bare
        # Databricks UC identifier so a typo (hyphen, space, etc.) is
        # caught at submit time instead of failing mid-clone with an
        # opaque Spark parse error.
        if v is None:
            return None
        v = v.strip()
        if not v:
            return None
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", v):
            raise ValueError(
                "target_catalog must be a bare Databricks identifier "
                "(letters, digits, underscores; first character a letter "
                f"or underscore). Got: {v!r}"
            )
        return v

    def to_devmirror_config(self) -> DevMirrorConfig:
        """Convert flat form fields into a nested DevMirrorConfig.

        ``self.dr_id`` must be non-None by the time this is called -- the
        server-side auto-ID layer injects it before validation.  Passing a
        ``ConfigIn`` with no ``dr_id`` raises a :class:`ValidationError`
        via the nested ``DevelopmentRequest`` model.
        """
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
            uat_users=self.uat_users,
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
    """Full config detail response.

    ``rejection_*`` fields are populated only when ``status == 'rejected'``
    (an admin clicked Reject on the Scan Results page).  They surface the
    rationale to the owner so they know why the request was turned down.
    """

    dr_id: str
    description: str | None
    status: str
    config: ConfigIn
    validation_errors: list[FieldError]
    created_at: str
    created_by: str
    updated_at: str | None
    expiration_date: str
    rejection_comment: str | None = None
    rejected_by: str | None = None
    rejected_at: str | None = None


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
    # Friendly label of the workspace where the stream was found
    # (e.g. "local", "prod").  Set when multi-workspace search is
    # configured; absent (None) for backward-compatible single-workspace
    # responses.
    workspace: str | None = None


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
    """Full lifecycle status of a provisioned DR.

    ``created_by`` is the canonical owner -- the user who submitted the
    original config (from the ConfigRepository row).  It will not match the
    DRRepository row's ``created_by`` if the runner mis-recorded it as the
    first developer in the access list; that legacy quirk is reconciled at
    response-build time so the UI always shows the requester.

    The four ``rejection_*`` fields are populated only when ``status`` is
    ``REJECTED`` and surface the admin's rationale in the UI.
    """

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
    rejection_comment: str | None = None
    rejected_by: str | None = None
    rejected_at: str | None = None


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


class CleanupFailure(BaseModel):
    """One object/schema/revoke that the cleanup couldn't complete."""

    fqn: str
    error: str


class CleanupResponse(BaseModel):
    """Response from a cleanup operation."""

    dr_id: str
    final_status: str
    objects_dropped: int
    schemas_dropped: int
    revokes_succeeded: int
    # Per-failure detail.  Empty when cleanup is fully successful; non-empty
    # entries cause the UI to render a "Partial cleanup" banner so users
    # know which FQNs they need to chase down with the platform team.
    objects_failed: list[CleanupFailure] = []
    schemas_failed: list[CleanupFailure] = []
    revokes_failed: list[CleanupFailure] = []


class RejectRequest(BaseModel):
    """Body for an admin rejection of a config (DR request).

    Used by ``POST /api/configs/{dr_id}/reject`` -- the comment is the
    rationale the owner sees on the rejected request.
    """

    comment: str

    @field_validator("comment")
    @classmethod
    def _non_empty_comment(cls, v: str) -> str:
        # Forbid empty / whitespace-only comments -- the rationale is what
        # the owner sees, so a blank one fails the UX contract.
        trimmed = v.strip()
        if not trimmed:
            raise ValueError("Rejection comment must not be empty.")
        return trimmed


class RejectResponse(BaseModel):
    """Response after a successful reject call."""

    dr_id: str
    status: str
    rejection_comment: str
    rejected_by: str
    rejected_at: str


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
    add_uat_users: list[str] | None = None
    remove_uat_users: list[str] | None = None


class ModifyDrResponse(BaseModel):
    """Response from a DR modification."""

    dr_id: str
    status: str
    message: str
