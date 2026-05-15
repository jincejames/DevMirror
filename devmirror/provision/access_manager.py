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


def _scim_check_bypassed() -> bool:
    """Return True when ``DEVMIRROR_SKIP_PRINCIPAL_SCIM_CHECK`` is truthy.

    Workspaces where the app SP can't list arbitrary users via SCIM
    (LH/Azure: the SP only sees members of its own groups) need this
    escape hatch -- otherwise every developer/qa_user filter-list comes
    back empty and `_principal_exists` rejects them, dropping ALL
    grants silently.  When the check is bypassed, principals are
    trusted; UC's grants API still rejects genuinely non-existent
    principals at grant-execution time with a loud error that gets
    recorded in `grants_failed`, so we don't create orphan grants for
    typos -- we just stop pre-filtering them.
    """
    import os
    return os.environ.get(
        "DEVMIRROR_SKIP_PRINCIPAL_SCIM_CHECK", "",
    ).strip().lower() in ("true", "1", "yes")


def _principal_exists(principal: str, ws_client: object | None = None) -> bool:
    """Return True if the principal resolves to a user or group via SCIM.

    SDK errors (network, permissions) are treated as "exists=True" so we
    don't block legitimate grants on a transient lookup failure.  A real
    miss (the lookup succeeded and returned no results) is treated as
    "exists=False" and the caller raises ``PrincipalNotFoundError``.

    When ``DEVMIRROR_SKIP_PRINCIPAL_SCIM_CHECK`` is set this short-circuits
    to True -- needed for workspaces where the app SP cannot read SCIM
    for individual users (the bypass is documented in
    `_scim_check_bypassed`).
    """
    if _scim_check_bypassed():
        return True

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
    *,
    writable: bool = True,
) -> AccessGrantResult:
    """Execute schema grants via the SDK grants API.

    ``writable=True`` (default) grants ``USE_SCHEMA + SELECT + MODIFY``
    — the read-write mode used for developers and historically for QA
    users too.  ``writable=False`` drops ``MODIFY`` so the principal
    can read but not mutate the schema's tables/views — used for QA
    users under the new grant matrix (developers RW on dev+QA,
    QA users RO on QA).  USE_SCHEMA is granted in both modes so the
    principal can enumerate the schema at all.

    Each principal is verified to exist in the workspace SCIM directory
    before granting.  Non-existent principals are recorded as failures so
    admins see them at provision/approval time, instead of creating an
    orphan grant that becomes valid if someone later registers that
    email/group name.
    """
    from databricks.sdk.service.catalog import Privilege, SecurableType

    granted = 0
    failed: list[tuple[str, str]] = []

    rw_privileges = [Privilege.SELECT]
    if writable:
        rw_privileges.append(Privilege.MODIFY)
    rw_label = ", ".join(p.value for p in rw_privileges)

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
            # Grant SELECT (and MODIFY if writable)
            try:
                logger.info("Granting %s on %s to %s", rw_label, schema_fqn, principal)
                db_client.grant(
                    SecurableType.SCHEMA, schema_fqn, principal,
                    rw_privileges,
                )
                granted += 1
            except Exception as exc:
                sql_repr = f"GRANT {rw_label} ON SCHEMA {schema_fqn} TO `{principal}`"
                logger.error("Grant failed: %s -- %s", sql_repr, exc)
                failed.append((sql_repr, str(exc)))

    return AccessGrantResult(granted=granted, failed=failed)


def _validate_volume_fqn(volume_fqn: str) -> None:
    """Validate a three-part volume FQN (``<catalog>.<schema>.<volume>``)."""
    parts = volume_fqn.split(".")
    if len(parts) != 3:
        raise AccessManagerError(
            f"Volume FQN must be three-part (catalog.schema.volume), got: {volume_fqn!r}"
        )
    for part in parts:
        if not _SAFE_IDENTIFIER.match(part):
            raise AccessManagerError(
                f"Unsafe identifier in volume FQN: {part!r}. "
                "Only alphanumeric characters and underscores are allowed."
            )


def apply_volume_grants(
    db_client: DbClient,
    volume_fqns: list[str],
    principals: list[str],
    *,
    writable: bool,
) -> AccessGrantResult:
    """Grant READ_VOLUME (and optionally WRITE_VOLUME) per (volume, principal).

    Used by the import-schema feature so developers can read AND write the
    DR's per-catalog ``main_volume`` (sideload artifacts), while QA users
    get read-only access to the same data.  Schema-level USE_SCHEMA is
    granted separately by ``apply_grants`` for the schema containing the
    volume -- that grant is required for the principal to even see the
    volume in UC.

    Principal existence is verified via SCIM (reused cache from
    ``apply_grants``) before any grant fires.  Per-grant failures are
    captured in ``AccessGrantResult.failed`` rather than aborting the
    whole pass.
    """
    from databricks.sdk.service.catalog import Privilege, SecurableType

    granted = 0
    failed: list[tuple[str, str]] = []

    if not volume_fqns or not principals:
        return AccessGrantResult(granted=granted, failed=failed)

    ws_client = db_client.client
    valid_principals: list[str] = []
    for principal in principals:
        _validate_principal(principal)
        if _principal_exists(principal, ws_client=ws_client):
            valid_principals.append(principal)
        else:
            msg = (
                f"Principal {principal!r} not found in workspace SCIM directory; "
                "refusing to grant volume access."
            )
            logger.error(msg)
            failed.append((principal, msg))

    privileges = [Privilege.READ_VOLUME]
    if writable:
        privileges.append(Privilege.WRITE_VOLUME)
    priv_label = ", ".join(p.value for p in privileges)

    for volume_fqn in volume_fqns:
        _validate_volume_fqn(volume_fqn)
        for principal in valid_principals:
            try:
                logger.info("Granting %s on VOLUME %s to %s", priv_label, volume_fqn, principal)
                db_client.grant(
                    SecurableType.VOLUME, volume_fqn, principal, privileges,
                )
                granted += 1
            except Exception as exc:
                sql_repr = f"GRANT {priv_label} ON VOLUME {volume_fqn} TO `{principal}`"
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
