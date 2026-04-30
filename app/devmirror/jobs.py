"""Scheduled job entrypoints for DevMirror.

Callable from Databricks jobs without CLI.  Each function loads settings,
builds repositories, executes the relevant engine, and guards with
try/except for job observability.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def _apply_overrides_from_argv() -> None:
    """Translate task-level named_parameters into DEVMIRROR_* env vars so
    load_settings() picks them up.  Used by serverless job tasks where the
    bundle cannot inject env vars via the environment spec
    (compute.Environment only allows client + dependencies).
    parse_known_args() is permissive, so existing CLI calls without these
    flags are unaffected.
    """
    import argparse
    import os

    p = argparse.ArgumentParser(allow_abbrev=False, add_help=False)
    p.add_argument("--catalog")
    p.add_argument("--schema")
    p.add_argument("--warehouse-id", "--warehouse_id", dest="warehouse_id")
    args, _ = p.parse_known_args()
    if args.catalog:
        os.environ["DEVMIRROR_CONTROL_CATALOG"] = args.catalog
    if args.schema:
        os.environ["DEVMIRROR_CONTROL_SCHEMA"] = args.schema
    if args.warehouse_id:
        os.environ["DEVMIRROR_WAREHOUSE_ID"] = args.warehouse_id


def _build_context() -> tuple:
    """Build shared context: (db_client, settings, dr_repo, obj_repo, access_repo, audit_repo)."""
    _apply_overrides_from_argv()

    from databricks.sdk import WorkspaceClient

    from devmirror.control.audit import AuditRepository
    from devmirror.control.control_table import (
        DrAccessRepository,
        DrObjectRepository,
        DRRepository,
    )
    from devmirror.settings import load_settings
    from devmirror.utils.db_client import DbClient

    settings = load_settings()
    client = WorkspaceClient()
    db_client = DbClient(client=client)
    fqn_prefix = settings.control_fqn_prefix
    return (
        db_client, settings,
        DRRepository(fqn_prefix), DrObjectRepository(fqn_prefix),
        DrAccessRepository(fqn_prefix), AuditRepository(fqn_prefix),
    )


def run_notifications() -> None:
    """Find DRs approaching expiration and send notifications."""
    try:
        db_client, settings, dr_repo, obj_repo, _access, audit_repo = _build_context()
        from devmirror.cleanup.notifier import LoggingBackend, notify_expiring_drs

        result = notify_expiring_drs(
            db_client=db_client, dr_repo=dr_repo, obj_repo=obj_repo,
            audit_repo=audit_repo, backend=LoggingBackend(),
            notification_days=settings.default_notification_days,
        )
        logger.info("Notifications: notified=%d failed=%d skipped=%d",
                     result.notified, len(result.failed), result.skipped)
        for dr_id, error in result.failed:
            logger.error("Notification failed for %s: %s", dr_id, error)
    except Exception:
        logger.exception("run_notifications failed")
        raise


def run_cleanup() -> None:
    """Find expired DRs and clean up each one."""
    try:
        db_client, _settings, dr_repo, obj_repo, access_repo, audit_repo = _build_context()
        from devmirror.cleanup.cleanup_engine import cleanup_dr, find_expired_drs
        from devmirror.control.control_table import DRStatus

        expired_drs = find_expired_drs(db_client, dr_repo)
        if not expired_drs:
            logger.info("No expired DRs found for cleanup.")
            return

        logger.info("Found %d DR(s) for cleanup.", len(expired_drs))
        for dr_row in expired_drs:
            dr_id = dr_row.get("dr_id", "")
            try:
                result = cleanup_dr(
                    dr_id, db_client=db_client, dr_repo=dr_repo,
                    obj_repo=obj_repo, access_repo=access_repo,
                    audit_repo=audit_repo,
                    current_status=DRStatus(dr_row.get("status", "")),
                )
                if result.fully_cleaned:
                    logger.info("Cleanup complete for %s", dr_id)
                else:
                    logger.warning(
                        "Partial cleanup for %s: %d obj / %d revoke / %d schema failures",
                        dr_id, len(result.objects_failed),
                        len(result.revokes_failed), len(result.schemas_failed),
                    )
            except Exception:
                logger.exception("Cleanup failed for %s", dr_id)
    except Exception:
        logger.exception("run_cleanup failed")
        raise


def run_audit_purge() -> None:
    """Purge audit log entries older than the configured retention window."""
    try:
        db_client, settings, *_, audit_repo = _build_context()
        deleted = audit_repo.purge_old_entries(
            db_client, retention_days=settings.audit_retention_days,
        )
        logger.info("Audit purge: %d entries removed (retention=%d days).",
                     deleted, settings.audit_retention_days)
    except Exception:
        logger.exception("run_audit_purge failed")
        raise
