"""Cross-cutting validation helpers beyond Pydantic schema validation."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import TYPE_CHECKING

from devmirror.config.schema import DataRevision, DevMirrorConfig  # noqa: TC001 - used at runtime

if TYPE_CHECKING:
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)


class ConfigValidationError(Exception):
    """Raised when a config passes schema validation but violates policy rules."""


def validate_expiration(
    expiration_date: date,
    *,
    max_duration_days: int = 90,
    today: date | None = None,
) -> None:
    """Validate that the expiration date is in the future and within the max allowed duration."""
    today = today or date.today()

    if expiration_date <= today:
        raise ConfigValidationError(
            f"expiration_date must be in the future. "
            f"Got {expiration_date.isoformat()}, today is {today.isoformat()}."
        )

    delta = (expiration_date - today).days
    if delta > max_duration_days:
        latest = today + timedelta(days=max_duration_days)
        raise ConfigValidationError(
            f"expiration_date is {delta} days from today, "
            f"which exceeds the maximum allowed duration of {max_duration_days} days. "
            f"Latest allowed: {latest.isoformat()}."
        )


def validate_config_for_submission(
    config: DevMirrorConfig,
    *,
    max_duration_days: int = 90,
    today: date | None = None,
) -> list[str]:
    """Run all policy-level validations on a parsed config before submission.

    Returns a list of human-readable error strings. An empty list means the
    config is valid for submission.

    This is the single entry point for pre-network validation. It does NOT
    check whether streams resolve in PROD (that requires network calls and
    belongs in the scan module).
    """
    errors: list[str] = []
    dr = config.development_request
    today = today or date.today()

    # Expiration: reuse validate_expiration, collecting its error as a string
    try:
        validate_expiration(
            dr.lifecycle.expiration_date,
            max_duration_days=max_duration_days,
            today=today,
        )
    except ConfigValidationError as exc:
        errors.append(str(exc))

    # At least one developer (also enforced by Pydantic, but belt-and-suspenders)
    if not dr.access.developers:
        errors.append("At least one developer must be specified in access.developers.")

    # QA users should be specified if QA env is enabled
    if (
        dr.environments.qa is not None
        and dr.environments.qa.enabled
        and (dr.access.qa_users is None or len(dr.access.qa_users) == 0)
    ):
        errors.append(
            "QA environment is enabled but no qa_users are specified in access.qa_users."
        )

    return errors


def validate_delta_retention(
    db_client: DbClient,
    table_fqns: list[str],
    data_revision: DataRevision,
) -> list[str]:
    """Validate that a data revision is within the Delta table retention window.

    For each table FQN, runs ``DESCRIBE HISTORY {fqn} LIMIT 1`` (oldest-first
    is not guaranteed, but LIMIT 1 returns the earliest available entry after
    the retention window) to check whether the requested version or timestamp
    falls within the retained history.

    Args:
        db_client: Unified Databricks client.
        table_fqns: Fully qualified table names to check.
        data_revision: The requested data revision policy.

    Returns:
        A list of human-readable warning strings for tables where the
        revision is outside the retention window.  An empty list means
        all tables pass (or mode is ``latest``).
    """
    if data_revision.mode == "latest":
        return []

    errors: list[str] = []

    for fqn in table_fqns:
        try:
            rows = db_client.sql(f"DESCRIBE HISTORY {fqn} LIMIT 1")
        except Exception as exc:
            msg = (
                f"Could not check Delta retention for {fqn}: {exc}. "
                "Proceeding without retention validation for this table."
            )
            logger.warning(msg)
            errors.append(msg)
            continue

        if not rows:
            # No history available -- nothing to validate against
            continue

        oldest = rows[0]

        if data_revision.mode == "version":
            oldest_version_raw = oldest.get("version")
            if oldest_version_raw is not None:
                try:
                    oldest_version = int(oldest_version_raw)
                except (ValueError, TypeError):
                    continue
                if data_revision.version is not None and data_revision.version < oldest_version:
                    errors.append(
                        f"Table {fqn}: requested version {data_revision.version} "
                        f"is older than the earliest available version {oldest_version}. "
                        "It may be outside the Delta retention window."
                    )

        elif data_revision.mode == "timestamp":
            oldest_timestamp = oldest.get("timestamp")
            if oldest_timestamp is not None and data_revision.timestamp is not None:
                try:
                    # Compare as strings -- both are ISO 8601 sortable
                    if data_revision.timestamp < str(oldest_timestamp):
                        errors.append(
                            f"Table {fqn}: requested timestamp {data_revision.timestamp!r} "
                            f"is older than the earliest available timestamp "
                            f"{oldest_timestamp!r}. "
                            "It may be outside the Delta retention window."
                        )
                except (TypeError, ValueError):
                    # Can't compare -- skip gracefully
                    pass

    return errors
