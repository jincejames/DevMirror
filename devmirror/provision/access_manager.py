"""Access control provisioning and revocation for dev/qa schemas."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)

_SAFE_IDENTIFIER = re.compile(r"^[a-zA-Z0-9_]+$")
# Principals can be email addresses, group names, etc. -- validated loosely.
_SAFE_PRINCIPAL = re.compile(r"^[a-zA-Z0-9_.@\-]+$")


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
    """Validate a principal identifier for safe SQL interpolation."""
    if not _SAFE_PRINCIPAL.match(principal):
        raise AccessManagerError(
            f"Unsafe principal identifier: {principal!r}. "
            "Only alphanumeric characters, dots, underscores, hyphens, and @ are allowed."
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
    """Execute schema grants via the SDK grants API, capturing per-operation failures."""
    from databricks.sdk.service.catalog import Privilege, SecurableType

    granted = 0
    failed: list[tuple[str, str]] = []

    for schema_fqn in schema_fqns:
        _validate_schema_fqn(schema_fqn)
        for principal in principals:
            _validate_principal(principal)
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
