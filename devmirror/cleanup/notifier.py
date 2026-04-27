"""Pre-expiry notification engine."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from devmirror.utils import now_iso

if TYPE_CHECKING:
    from devmirror.control.audit import AuditRepository
    from devmirror.control.control_table import DrObjectRepository, DRRepository
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)


@dataclass
class NotificationContent:
    """Pre-expiry notification payload for a single DR."""

    dr_id: str
    description: str
    expiration_date: str
    object_count: int
    recipients: list[str]
    subject: str
    body: str


def build_notification(
    dr_row: dict[str, Any],
    object_count: int,
    recipients: list[str],
) -> NotificationContent:
    """Build notification content from a DR row."""
    dr_id = dr_row.get("dr_id", "")
    description = dr_row.get("description", "") or ""
    expiration_date = str(dr_row.get("expiration_date", ""))

    body = (
        f"Development Request {dr_id} is approaching its expiration date.\n\n"
        f"  DR ID:           {dr_id}\n"
        f"  Description:     {description}\n"
        f"  Expiration Date: {expiration_date}\n"
        f"  Objects:         {object_count}\n\n"
        f"To extend this DR, use:\n"
        f"  devmirror modify --config <config.yaml>\n"
        f"with an updated expiration_date.\n\n"
        f"If no action is taken, all dev objects, schemas, and access grants\n"
        f"will be automatically removed after the expiration date.\n"
    )

    return NotificationContent(
        dr_id=dr_id, description=description, expiration_date=expiration_date,
        object_count=object_count, recipients=recipients,
        subject=f"[DevMirror] DR {dr_id} expiring on {expiration_date}",
        body=body,
    )


@runtime_checkable
class NotificationBackend(Protocol):
    """Protocol for notification delivery backends."""

    def send(self, notification: NotificationContent) -> bool:
        """Send a notification. Returns True on success."""
        ...


class LoggingBackend:
    """Fallback backend that logs notifications instead of sending them."""

    def send(self, notification: NotificationContent) -> bool:
        logger.info(
            "PRE-EXPIRY NOTIFICATION for %s:\n%s",
            notification.dr_id,
            notification.body,
        )
        return True


@dataclass
class NotifyResult:
    """Outcome of a notification run."""

    notified: int = 0
    failed: list[tuple[str, str]] = field(default_factory=list)
    skipped: int = 0


def find_drs_needing_notification(
    db_client: DbClient,
    dr_repo: DRRepository,
    notification_days: int = 7,
) -> list[dict[str, Any]]:
    """Find active DRs in the notification window that have not been notified."""
    table = dr_repo.table_fqn
    # `notification_days` is int-cast and safe to interpolate. DATE_SUB's
    # second argument must be an integer literal in Spark SQL; the Statement
    # Execution API stringifies bind params, which Spark won't coerce here.
    sql = (
        f"SELECT * FROM {table} "
        f"WHERE DATE_SUB(expiration_date, {int(notification_days)}) <= CURRENT_DATE() "
        "AND notification_sent_at IS NULL "
        "AND status = :status"
    )
    return db_client.sql_with_params(sql, {"status": "ACTIVE"})


def notify_expiring_drs(
    *,
    db_client: DbClient,
    dr_repo: DRRepository,
    obj_repo: DrObjectRepository,
    audit_repo: AuditRepository,
    backend: NotificationBackend | None = None,
    notification_days: int = 7,
) -> NotifyResult:
    """Run the pre-expiry notification loop."""
    if backend is None:
        backend = LoggingBackend()

    result = NotifyResult()

    drs = find_drs_needing_notification(
        db_client, dr_repo, notification_days=notification_days
    )

    for dr_row in drs:
        dr_id = dr_row.get("dr_id", "")

        if dr_row.get("notification_sent_at"):
            result.skipped += 1
            continue

        objects = obj_repo.list_by_dr_id(db_client, dr_id=dr_id)
        object_count = len(objects)
        recipients = _extract_recipients(dr_row)
        notification = build_notification(dr_row, object_count, recipients)

        try:
            success = backend.send(notification)
        except Exception as exc:
            logger.error("Notification send failed for %s: %s", dr_id, exc)
            result.failed.append((dr_id, str(exc)))
            continue

        if success:
            now = now_iso()
            try:
                dr_repo.update_notification_sent(
                    db_client,
                    dr_id=dr_id,
                    notification_sent_at=now,
                )
            except Exception as exc:
                logger.error(
                    "Failed to update notification_sent_at for %s: %s", dr_id, exc
                )

            audit_repo.append(
                db_client,
                dr_id=dr_id,
                action="NOTIFY",
                performed_by="SYSTEM",
                performed_at=now,
                status="SUCCESS",
                action_detail=json.dumps({
                    "recipients": recipients,
                    "object_count": object_count,
                    "expiration_date": notification.expiration_date,
                }),
            )
            result.notified += 1
        else:
            result.failed.append((dr_id, "Backend returned False"))

    return result


def _extract_recipients(dr_row: dict[str, Any]) -> list[str]:
    """Extract notification recipients from a DR row."""
    import yaml

    config_yaml_str = dr_row.get("config_yaml", "")
    if config_yaml_str:
        try:
            config = yaml.safe_load(config_yaml_str)
            if isinstance(config, dict):
                dr_section = config.get("development_request", config)
                lifecycle = dr_section.get("lifecycle", {})
                recipients = lifecycle.get("notification_recipients")
                if recipients and isinstance(recipients, list):
                    return recipients
                access = dr_section.get("access", {})
                developers = access.get("developers")
                if developers and isinstance(developers, list):
                    return developers
        except Exception:
            pass

    created_by = dr_row.get("created_by", "")
    if created_by:
        return [created_by]
    return []
