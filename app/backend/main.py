"""FastAPI application entry point for the DevMirror web app."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator  # noqa: TC003 -- used by asynccontextmanager return type
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from devmirror.settings import SettingsError, load_settings
from devmirror.utils.db_client import DbClient

from .router import router
from .router_admin import router_admin
from .router_stage2 import router_stage2
from .tasks import TaskTracker

logger = logging.getLogger(__name__)

# Resolve static directory
_static_dir = Path(__file__).resolve().parent / "static"
if not _static_dir.is_dir():
    _static_dir = Path(__file__).resolve().parent.parent / "static"


async def _background_cleanup_loop(app: FastAPI) -> None:
    """Periodically find and clean up expired DRs (every 6 hours)."""
    while True:
        await asyncio.sleep(6 * 3600)
        try:
            from devmirror.cleanup.cleanup_engine import cleanup_dr, find_expired_drs
            from devmirror.control.audit import AuditRepository
            from devmirror.control.control_table import (
                DrAccessRepository,
                DRRepository,
                DrObjectRepository,
            )

            db_client: DbClient = app.state.db_client
            settings = app.state.settings
            fqn = settings.control_fqn_prefix

            dr_repo = DRRepository(fqn)
            expired = find_expired_drs(db_client, dr_repo)
            for dr_row in expired:
                try:
                    cleanup_dr(
                        dr_row["dr_id"],
                        db_client=db_client,
                        dr_repo=dr_repo,
                        obj_repo=DrObjectRepository(fqn),
                        access_repo=DrAccessRepository(fqn),
                        audit_repo=AuditRepository(fqn),
                    )
                    logger.info("Background cleanup completed for %s", dr_row["dr_id"])
                except Exception:
                    logger.error(
                        "Background cleanup failed for %s",
                        dr_row["dr_id"],
                        exc_info=True,
                    )
        except Exception:
            logger.error("Background cleanup loop failed", exc_info=True)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Application lifespan: initialise DbClient, TaskTracker.

    DDL bootstrapping (ensure_table) is deferred to the first request that
    touches the repository, keeping startup fast and avoiding a blocking SQL
    warehouse call during cold start.
    """
    try:
        settings = load_settings()
    except SettingsError as exc:
        logger.critical("DevMirror startup aborted: %s", exc)
        raise
    db_client = DbClient()
    task_tracker = TaskTracker()

    app.state.db_client = db_client
    app.state.settings = settings
    app.state.task_tracker = task_tracker
    app.state.table_ensured = False

    # Start background cleanup loop
    cleanup_task = asyncio.create_task(_background_cleanup_loop(app))

    try:
        yield
    finally:
        # Graceful shutdown: drain running tasks (SIGTERM gives 15s)
        logger.info("Shutting down: draining background tasks")
        task_tracker.wait_for_running(timeout=10.0)
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass
        logger.info("Shutdown complete")


app = FastAPI(title="DevMirror", version="0.1.0", lifespan=lifespan)

# API routes (must be registered BEFORE the static file mount)
app.include_router(router, prefix="/api")
app.include_router(router_stage2, prefix="/api")
app.include_router(router_admin, prefix="/api")


@app.get("/api/health", operation_id="healthCheck")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


# Serve static assets (JS, CSS) at /assets/*
if _static_dir.is_dir():
    app.mount("/assets", StaticFiles(directory=str(_static_dir / "assets")), name="assets")

    # SPA catch-all: serve index.html for any non-API, non-asset path
    @app.get("/{full_path:path}", include_in_schema=False)
    async def spa_fallback(full_path: str) -> FileResponse:
        return FileResponse(str(_static_dir / "index.html"))
