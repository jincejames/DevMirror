"""Access control provisioning and revocation for dev/qa schemas."""

from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_]+$")
# Principals can be email addresses, group names, etc. -- validated loosely.
_SAFE_PRINCIPAL = re.compile(r"^[a-zA-Z0-9_.@\-]+$")
_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

# Existence-check cache: principal -> (exists_bool, timestamp)
# Refuse to grant access to principals that don't exist in the workspace,
# since Databricks may silently accept the grant and create an orphan
# that becomes valid once a real account with that name is added.
_principal_cache: dict[str, tuple[bool, float]] = {}
_principal_cache_lock = threading.Lock()
_PRINCIPAL_CACHE_TTL = 300  # 5 minutes


class PrincipalNotFoundError(Exception):
    """Raised when a principal can't be resolved in the workspace SCIM directory."""


def _principal_exists(principal: str, ws_client: object | None = None) -> bool:
    """Return True if the principal resolves to a user or group via SCIM.

    SDK errors (network, permissions) are treated as "exists=True" so we
    don't block legitimate grants on a transient lookup failure.  A real
    miss (the lookup succeeded and returned no results) is treated as
    "exists=False" and the caller raises ``PrincipalNotFoundError``.
    """
    now = time.time()
    with _principal_cache_lock:
        cached = _principal_cache.get(principal)
        if cached is not None:
            exists, ts = cached
            if now - ts < _PRINCIPAL_CACHE_TTL:
                return exists

    try:
        if ws_client is None:
            from databricks.sdk import WorkspaceClient
            ws_client = WorkspaceClient()
        is_email = bool(_EMAIL_RE.match(principal))
        if is_email:
            users = list(ws_client.users.list(filter=f"userName eq '{principal}'"))
            exists = len(users) > 0
        else:
            groups = list(ws_client.groups.list(filter=f"displayName eq '{principal}'"))
            exists = len(groups) > 0
    except Exception:
        # Lookup failure -> assume exists (don't block legitimate grants
        # on transient errors).  A real miss returns exists=False above.
        logger.warning(
            "SCIM existence check failed for principal %r; assuming exists",
            principal,
        )
        exists = True

    with _principal_cache_lock:
        _principal_cache[principal] = (exists, time.time())
    return exists


class AccessManagerError(Exception):
    """Raised when access grant/revoke operations fail."""


def _validate_schema_fqn(schema_fqn: str) -> None:
    """Validate a two-part schema FQN."""
    parts = schema_fqn.split(".")
    if len(parts) != 2:
        raise AccessManagerError(
            f"Schema FQN must be two-part (catalog.schema), got: {schema_fqn!r}"
        )
    for part in parts:
        if not _SAFE_IDENTIFIER.match(part):
            raise AccessManagerError(
                f"Unsafe identifier in schema FQN: {part!r}. "
                "Only alphanumeric characters and underscores are allowed."
            )


def _validate_principal(principal: str) -> None:
    """Validate a principal identifier for safe SQL interpolation.

    A principal may be a user email (``alice@co.com``), a Databricks account
    group name (``data-engineers``), or a service principal application ID.
    """
    if not _SAFE_PRINCIPAL.match(principal):
        raise AccessManagerError(
            f"Unsafe principal identifier: {principal!r}. "
            "Expected a user email (e.g. 'alice@co.com'), a Databricks account "
            "group name (e.g. 'data-engineers'), or a service principal "
            "application ID. Only alphanumeric characters, dots, underscores, "
            "hyphens, and @ are allowed."
        )


def _grant_sql(schema_fqn: str, principal: str, privileges: str) -> str:
    """Generate a GRANT/REVOKE statement after validating identifiers."""
    _validate_schema_fqn(schema_fqn)
    _validate_principal(principal)
    return f"{privileges} ON SCHEMA {schema_fqn} TO `{principal}`"


def grant_schema_usage_sql(schema_fqn: str, principal: str) -> str:
    """Generate ``GRANT USAGE ON SCHEMA ... TO `principal``` SQL."""
    return _grant_sql(schema_fqn, principal, "GRANT USAGE")


def grant_schema_rw_sql(schema_fqn: str, principal: str) -> str:
    """Generate ``GRANT SELECT, MODIFY ON SCHEMA ... TO `principal``` SQL."""
    return _grant_sql(schema_fqn, principal, "GRANT SELECT, MODIFY")


def revoke_schema_sql(schema_fqn: str, principal: str) -> str:
    """Generate SQL to revoke all grants on a schema for a principal."""
    _validate_schema_fqn(schema_fqn)
    _validate_principal(principal)
    return f"REVOKE ALL PRIVILEGES ON SCHEMA {schema_fqn} FROM `{principal}`"


def generate_grant_statements(
    schema_fqns: list[str],
    principals: list[str],
) -> list[str]:
    """Generate all GRANT USAGE + GRANT SELECT,MODIFY statements for schemas x principals."""
    statements: list[str] = []
    for schema_fqn in schema_fqns:
        for principal in principals:
            statements.append(grant_schema_usage_sql(schema_fqn, principal))
            statements.append(grant_schema_rw_sql(schema_fqn, principal))
    return statements


@dataclass
class AccessGrantResult:
    """Result of access grant/revoke operations."""

    granted: int
    failed: list[tuple[str, str]]

    @property
    def all_succeeded(self) -> bool:
        return len(self.failed) == 0


def apply_grants(
    db_client: DbClient,
    schema_fqns: list[str],
    principals: list[str],
) -> AccessGrantResult:
    """Execute schema grants via the SDK grants API, capturing per-operation failures.

    Each principal is verified to exist in the workspace SCIM directory
    before granting.  Non-existent principals are recorded as failures so
    admins see them at provision/approval time, instead of creating an
    orphan grant that becomes valid if someone later registers that
    email/group name.
    """
    from databricks.sdk.service.catalog import Privilege, SecurableType

    granted = 0
    failed: list[tuple[str, str]] = []

    # Pre-check principals once each so we don't hammer SCIM per schema.
    ws_client = db_client.client
    valid_principals: list[str] = []
    for principal in principals:
        _validate_principal(principal)
        if _principal_exists(principal, ws_client=ws_client):
            valid_principals.append(principal)
        else:
            msg = (
                f"Principal {principal!r} not found in workspace SCIM directory; "
                "refusing to grant."
            )
            logger.error(msg)
            failed.append((principal, msg))

    for schema_fqn in schema_fqns:
        _validate_schema_fqn(schema_fqn)
        for principal in valid_principals:
            # Grant USE_SCHEMA
            try:
                logger.info("Granting USE_SCHEMA on %s to %s", schema_fqn, principal)
                db_client.grant(
                    SecurableType.SCHEMA, schema_fqn, principal,
                    [Privilege.USE_SCHEMA],
                )
                granted += 1
            except Exception as exc:
                sql_repr = f"GRANT USE_SCHEMA ON SCHEMA {schema_fqn} TO `{principal}`"
                logger.error("Grant failed: %s -- %s", sql_repr, exc)
                failed.append((sql_repr, str(exc)))
            # Grant SELECT, MODIFY
            try:
                logger.info("Granting SELECT, MODIFY on %s to %s", schema_fqn, principal)
                db_client.grant(
                    SecurableType.SCHEMA, schema_fqn, principal,
                    [Privilege.SELECT, Privilege.MODIFY],
                )
                granted += 1
            except Exception as exc:
                sql_repr = f"GRANT SELECT, MODIFY ON SCHEMA {schema_fqn} TO `{principal}`"
                logger.error("Grant failed: %s -- %s", sql_repr, exc)
                failed.append((sql_repr, str(exc)))

    return AccessGrantResult(granted=granted, failed=failed)


def apply_revokes(
    db_client: DbClient,
    schema_fqns: list[str],
    principals: list[str],
) -> AccessGrantResult:
    """Execute revoke operations via the SDK grants API."""
    from databricks.sdk.service.catalog import Privilege, SecurableType

    granted = 0
    failed: list[tuple[str, str]] = []

    for schema_fqn in schema_fqns:
        for principal in principals:
            try:
                logger.info("Revoking all on %s from %s", schema_fqn, principal)
                db_client.revoke(
                    SecurableType.SCHEMA, schema_fqn, principal,
                    [Privilege.USE_SCHEMA, Privilege.SELECT, Privilege.MODIFY],
                )
                granted += 1
            except Exception as exc:
                msg = f"REVOKE on {schema_fqn} for {principal}"
                logger.error("Revoke failed for %s on %s: %s", principal, schema_fqn, exc)
                failed.append((msg, str(exc)))

    return AccessGrantResult(granted=granted, failed=failed)
