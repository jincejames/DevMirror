"""Repository layer for the devmirror_configs table."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from devmirror.utils.db_client import DbClient


def _param_or_null(params: dict, key: str, value: str | None) -> str:
    """Return :key placeholder if value is non-None, else literal NULL."""
    if value is None:
        return "NULL"
    params[key] = value
    return f":{key}"


class ConfigRepository:
    """CRUD operations for ``devmirror_configs``."""

    def __init__(self, fqn_prefix: str) -> None:
        self._table = f"{fqn_prefix}.devmirror_configs"

    @property
    def table_fqn(self) -> str:
        return self._table

    def ensure_table(self, db_client: DbClient) -> None:
        """Create the configs table if it doesn't exist, and add Stage 2 columns."""
        db_client.sql_exec(
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            "dr_id STRING, config_json STRING, config_yaml STRING, "
            "status STRING, validation_errors STRING, created_at STRING, "
            "created_by STRING, updated_at STRING, expiration_date STRING, "
            "description STRING, manifest_json STRING, scanned_at STRING)"
        )
        # Migrate existing tables that lack the new columns
        for col in ("manifest_json STRING", "scanned_at STRING"):
            try:
                db_client.sql_exec(
                    f"ALTER TABLE {self._table} ADD COLUMNS ({col})"
                )
            except Exception:  # noqa: BLE001
                pass  # Column already exists

    def insert(
        self,
        db_client: DbClient,
        *,
        dr_id: str,
        config_json: str,
        config_yaml: str,
        status: str,
        validation_errors: str,
        created_by: str,
        expiration_date: str,
        description: str | None,
    ) -> None:
        """Insert a new config row."""
        now = datetime.now(UTC).isoformat()
        params: dict[str, str] = {}
        desc_expr = _param_or_null(params, "description", description)
        params.update({
            "dr_id": dr_id,
            "config_json": config_json,
            "config_yaml": config_yaml,
            "status": status,
            "validation_errors": validation_errors,
            "created_at": now,
            "created_by": created_by,
            "expiration_date": expiration_date,
        })
        sql = (
            f"INSERT INTO {self._table} "
            "(dr_id, config_json, config_yaml, status, validation_errors, "
            "created_at, created_by, updated_at, expiration_date, description) "
            "VALUES ("
            ":dr_id, :config_json, :config_yaml, :status, :validation_errors, "
            f":created_at, :created_by, NULL, :expiration_date, {desc_expr})"
        )
        db_client.sql_exec_with_params(sql, params)

    def update(
        self,
        db_client: DbClient,
        *,
        dr_id: str,
        config_json: str,
        config_yaml: str,
        status: str,
        validation_errors: str,
        expiration_date: str,
        description: str | None,
    ) -> None:
        """Update an existing config row."""
        now = datetime.now(UTC).isoformat()
        params: dict[str, str] = {}
        desc_expr = _param_or_null(params, "description", description)
        params.update({
            "dr_id": dr_id,
            "config_json": config_json,
            "config_yaml": config_yaml,
            "status": status,
            "validation_errors": validation_errors,
            "updated_at": now,
            "expiration_date": expiration_date,
        })
        sql = (
            f"UPDATE {self._table} SET "
            "config_json = :config_json, "
            "config_yaml = :config_yaml, "
            "status = :status, "
            "validation_errors = :validation_errors, "
            "updated_at = :updated_at, "
            "expiration_date = :expiration_date, "
            f"description = {desc_expr} "
            "WHERE dr_id = :dr_id"
        )
        db_client.sql_exec_with_params(sql, params)

    def get(self, db_client: DbClient, *, dr_id: str) -> dict[str, Any] | None:
        """Fetch a single config row by dr_id, or None if not found."""
        sql = f"SELECT * FROM {self._table} WHERE dr_id = :dr_id"
        rows = db_client.sql_with_params(sql, {"dr_id": dr_id})
        return rows[0] if rows else None

    def list_all(self, db_client: DbClient) -> list[dict[str, Any]]:
        """Return all config rows."""
        sql = f"SELECT * FROM {self._table} ORDER BY created_at DESC"
        return db_client.sql(sql)

    def update_status(
        self,
        db_client: DbClient,
        *,
        dr_id: str,
        status: str,
    ) -> None:
        """Update only the status column of a config row."""
        now = datetime.now(UTC).isoformat()
        sql = (
            f"UPDATE {self._table} SET "
            "status = :status, "
            "updated_at = :updated_at "
            "WHERE dr_id = :dr_id"
        )
        db_client.sql_exec_with_params(sql, {"dr_id": dr_id, "status": status, "updated_at": now})

    def update_manifest(
        self,
        db_client: DbClient,
        *,
        dr_id: str,
        manifest_json: str,
        scanned_at: str,
    ) -> None:
        """Store the scan manifest and timestamp on a config row."""
        now = datetime.now(UTC).isoformat()
        sql = (
            f"UPDATE {self._table} SET "
            "manifest_json = :manifest_json, "
            "scanned_at = :scanned_at, "
            "updated_at = :updated_at "
            "WHERE dr_id = :dr_id"
        )
        db_client.sql_exec_with_params(sql, {
            "dr_id": dr_id, "manifest_json": manifest_json,
            "scanned_at": scanned_at, "updated_at": now,
        })

    def get_manifest(
        self,
        db_client: DbClient,
        *,
        dr_id: str,
    ) -> dict[str, Any] | None:
        """Retrieve the stored manifest for a config, or None if not scanned."""
        row = self.get(db_client, dr_id=dr_id)
        if row is None:
            return None
        manifest_raw = row.get("manifest_json")
        if not manifest_raw:
            return None
        import json

        return {
            "manifest": json.loads(manifest_raw),
            "scanned_at": row.get("scanned_at"),
        }

    def delete(self, db_client: DbClient, *, dr_id: str) -> bool:
        """Delete a config row. Returns False if not found or status is provisioned."""
        existing = self.get(db_client, dr_id=dr_id)
        if existing is None:
            return False
        if existing.get("status") == "provisioned":
            return False
        sql = f"DELETE FROM {self._table} WHERE dr_id = :dr_id"
        db_client.sql_exec_with_params(sql, {"dr_id": dr_id})
        return True
