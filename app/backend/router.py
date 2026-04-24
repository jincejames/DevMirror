"""Stage 1 API router: config CRUD endpoints (1-8)."""
# ruff: noqa: B008  -- Depends() in function signatures is standard FastAPI pattern

from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import Response

from .auth import UserInfo, get_user_role, require_owner_or_admin
from .config import get_current_user, get_db_client, get_settings
from .helpers import (
    _auto_scan,
    _build_yaml,
    _get_repo,
    _parse_config_in,
    _row_to_config_out,
    _row_to_list_item,
    _validate_config,
)
from .models import (
    ConfigIn,
    ConfigListResponse,
    ConfigOut,
    StreamSearchResponse,
    StreamSearchResult,
    ValidationResult,
)

if TYPE_CHECKING:
    from devmirror.settings import Settings
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)

router = APIRouter()


# ---- 0. GET /api/me (currentUser) ----------------------------------------

@router.get("/me", response_model=UserInfo, operation_id="currentUser")
def current_user_info(
    current_user: str = Depends(get_current_user),
    role: str = Depends(get_user_role),
) -> UserInfo:
    """Return the authenticated user's identity and resolved role."""
    prefix = current_user.split("@")[0] if "@" in current_user else current_user
    display_name = prefix.replace(".", " ").replace("_", " ").title()
    return UserInfo(email=current_user, role=role, display_name=display_name)


# ---- 1. POST /api/configs (createConfig) ---------------------------------

@router.post(
    "/configs",
    response_model=ConfigOut,
    status_code=201,
    operation_id="createConfig",
)
def create_config(
    config_in: ConfigIn,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    current_user: str = Depends(get_current_user),
) -> ConfigOut:
    """Create a new DevMirror config.

    US-34: the server assigns the DR ID.  A caller that supplies ``dr_id``
    is rejected with HTTP 400; the allocated ID is returned in the
    response body (and shows up in the client-side URL thereafter).
    """
    # US-34 acceptance criterion 4: reject caller-supplied dr_id.
    if config_in.dr_id is not None:
        raise HTTPException(
            status_code=400,
            detail="DR ID is auto-generated; do not supply",
        )

    # Allocate the next ID before any validation so that
    # ``_validate_config`` / ``to_devmirror_config`` see a concrete value.
    from devmirror.utils.id_generator import next_dr_id

    generated_id = next_dr_id(db_client, settings)
    config_in.dr_id = generated_id

    status, all_errors, _dm_config = _validate_config(config_in)

    config_json = config_in.model_dump_json()
    config_yaml = _build_yaml(config_in)
    errors_json = json.dumps([e.model_dump() for e in all_errors])

    repo = _get_repo(settings, db_client)
    repo.insert(
        db_client,
        dr_id=generated_id,
        config_json=config_json,
        config_yaml=config_yaml,
        status=status,
        validation_errors=errors_json,
        created_by=current_user,
        expiration_date=config_in.expiration_date,
        description=config_in.description,
    )

    # Auto-scan if config is valid
    if status == "valid" and _dm_config is not None:
        _auto_scan(db_client, settings, config_in, _dm_config, repo, generated_id)

    row = repo.get(db_client, dr_id=generated_id)
    if row is None:
        raise HTTPException(status_code=500, detail="Failed to read back created config")
    return _row_to_config_out(row)


# ---- 2. GET /api/configs (listConfigs) ------------------------------------

@router.get(
    "/configs",
    response_model=ConfigListResponse,
    operation_id="listConfigs",
)
def list_configs(
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    current_user: str = Depends(get_current_user),
    role: str = Depends(get_user_role),
) -> ConfigListResponse:
    """List all DevMirror configs."""
    repo = _get_repo(settings, db_client)
    rows = repo.list_all(db_client)
    if role != "admin":
        rows = [r for r in rows if r.get("created_by") == current_user]
    items = [_row_to_list_item(r) for r in rows]
    return ConfigListResponse(configs=items, total=len(items))


# ---- 3. GET /api/configs/{dr_id} (getConfig) -----------------------------

@router.get(
    "/configs/{dr_id}",
    response_model=ConfigOut,
    operation_id="getConfig",
)
def get_config(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    current_user: str = Depends(get_current_user),
    role: str = Depends(get_user_role),
) -> ConfigOut:
    """Get a single DevMirror config by DR ID."""
    repo = _get_repo(settings, db_client)
    row = repo.get(db_client, dr_id=dr_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")
    require_owner_or_admin(row, current_user, role)
    return _row_to_config_out(row)


# ---- 4. PUT /api/configs/{dr_id} (updateConfig) --------------------------

@router.put(
    "/configs/{dr_id}",
    response_model=ConfigOut,
    operation_id="updateConfig",
)
def update_config(
    dr_id: str,
    config_in: ConfigIn,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    current_user: str = Depends(get_current_user),
    role: str = Depends(get_user_role),
) -> ConfigOut:
    """Update an existing DevMirror config."""
    repo = _get_repo(settings, db_client)
    existing = repo.get(db_client, dr_id=dr_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")
    require_owner_or_admin(existing, current_user, role)

    was_provisioned = existing.get("status") == "provisioned"

    status, all_errors, _dm_config = _validate_config(config_in)

    # If the config was provisioned, keep the provisioned status after editing
    if was_provisioned and status == "valid":
        status = "provisioned"
    config_json = config_in.model_dump_json()
    config_yaml = _build_yaml(config_in)
    errors_json = json.dumps([e.model_dump() for e in all_errors])

    repo.update(
        db_client,
        dr_id=dr_id,
        config_json=config_json,
        config_yaml=config_yaml,
        status=status,
        validation_errors=errors_json,
        expiration_date=config_in.expiration_date,
        description=config_in.description,
    )

    # Auto-scan if config is valid (not provisioned -- provisioned configs use re-provision)
    if status == "valid" and _dm_config is not None:
        _auto_scan(db_client, settings, config_in, _dm_config, repo, dr_id)

    row = repo.get(db_client, dr_id=dr_id)
    if row is None:
        raise HTTPException(status_code=500, detail="Failed to read back updated config")
    return _row_to_config_out(row)


# ---- 5. DELETE /api/configs/{dr_id} (deleteConfig) -----------------------

@router.delete(
    "/configs/{dr_id}",
    status_code=204,
    operation_id="deleteConfig",
)
def delete_config(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    current_user: str = Depends(get_current_user),
    role: str = Depends(get_user_role),
) -> Response:
    """Delete a DevMirror config."""
    repo = _get_repo(settings, db_client)
    existing = repo.get(db_client, dr_id=dr_id)
    if existing is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")
    require_owner_or_admin(existing, current_user, role)
    if existing.get("status") == "provisioned":
        raise HTTPException(
            status_code=409,
            detail=f"Config {dr_id} is provisioned and cannot be deleted",
        )
    repo.delete(db_client, dr_id=dr_id)
    return Response(status_code=204)


# ---- 6. POST /api/configs/{dr_id}/validate (revalidateConfig) -----------

@router.post(
    "/configs/{dr_id}/validate",
    response_model=ValidationResult,
    operation_id="revalidateConfig",
)
def revalidate_config(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    current_user: str = Depends(get_current_user),
    role: str = Depends(get_user_role),
) -> ValidationResult:
    """Re-validate an existing config and update its status/errors."""
    repo = _get_repo(settings, db_client)
    row = repo.get(db_client, dr_id=dr_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")
    require_owner_or_admin(row, current_user, role)

    config_in = _parse_config_in(row["config_json"])
    status, all_errors, _dm_config = _validate_config(config_in)
    errors_json = json.dumps([e.model_dump() for e in all_errors])
    config_yaml = _build_yaml(config_in)

    repo.update(
        db_client,
        dr_id=dr_id,
        config_json=row["config_json"],
        config_yaml=config_yaml,
        status=status,
        validation_errors=errors_json,
        expiration_date=row["expiration_date"],
        description=row.get("description"),
    )

    return ValidationResult(status=status, errors=all_errors)


# ---- 7. GET /api/configs/{dr_id}/yaml (exportConfigYaml) ----------------

@router.get(
    "/configs/{dr_id}/yaml",
    operation_id="exportConfigYaml",
)
def export_config_yaml(
    dr_id: str,
    db_client: DbClient = Depends(get_db_client),
    settings: Settings = Depends(get_settings),
    current_user: str = Depends(get_current_user),
    role: str = Depends(get_user_role),
) -> Response:
    """Export config as a downloadable YAML file."""
    repo = _get_repo(settings, db_client)
    row = repo.get(db_client, dr_id=dr_id)
    if row is None:
        raise HTTPException(status_code=404, detail=f"Config {dr_id} not found")
    require_owner_or_admin(row, current_user, role)

    yaml_content = row.get("config_yaml", "")
    return Response(
        content=yaml_content,
        media_type="text/yaml",
        headers={"Content-Disposition": f'attachment; filename="{dr_id}.yaml"'},
    )


# ---- 8. GET /api/streams/search (searchStreams) --------------------------

@router.get(
    "/streams/search",
    response_model=StreamSearchResponse,
    operation_id="searchStreams",
)
def search_streams(
    q: str = Query(..., min_length=1, description="Search term for stream names"),
    db_client: DbClient = Depends(get_db_client),
) -> StreamSearchResponse:
    """Search for Databricks jobs and pipelines by name."""
    results: list[StreamSearchResult] = []

    try:
        ws_client = db_client.client

        # Search jobs
        try:
            for job in ws_client.jobs.list(name=q):
                job_name = getattr(job.settings, "name", None) if job.settings else None
                if job_name:
                    results.append(StreamSearchResult(name=job_name, type="job"))
                if len(results) >= 20:
                    break
        except Exception:
            logger.warning("Failed to search jobs", exc_info=True)

        # Search pipelines -- escape single quotes in user input to prevent injection
        if len(results) < 20:
            try:
                safe_q = q.replace("'", "\\'")
                for pipeline in ws_client.pipelines.list_pipelines(
                    filter=f"name LIKE '%{safe_q}%'"
                ):
                    pipeline_name = pipeline.name
                    if pipeline_name:
                        results.append(
                            StreamSearchResult(name=pipeline_name, type="pipeline")
                        )
                    if len(results) >= 20:
                        break
            except Exception:
                logger.warning("Failed to search pipelines", exc_info=True)

    except Exception:
        logger.warning("Failed to initialize workspace client for search", exc_info=True)

    return StreamSearchResponse(results=results[:20])
