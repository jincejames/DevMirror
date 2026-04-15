"""Repository layer for the devmirror_configs table."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from devmirror.utils.sql_executor import escape_sql_string as _escape

if TYPE_CHECKING:
    from devmirror.utils.db_client import DbClient


def _sql_val(v: Any) -> str:
    """Format a value for SQL: NULL if None, else escaped string literal."""
    return "NULL" if v is None else f"'{_escape(str(v))}'"


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
        sql = (
            f"INSERT INTO {self._table} "
            f"(dr_id, config_json, config_yaml, status, validation_errors, "
            f"created_at, created_by, updated_at, expiration_date, description) "
            f"VALUES ("
            f"'{_escape(dr_id)}', '{_escape(config_json)}', '{_escape(config_yaml)}', "
            f"'{_escape(status)}', '{_escape(validation_errors)}', "
            f"'{_escape(now)}', '{_escape(created_by)}', NULL, "
            f"'{_escape(expiration_date)}', {_sql_val(description)})"
        )
        db_client.sql_exec(sql)

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
        sql = (
            f"UPDATE {self._table} SET "
            f"config_json = '{_escape(config_json)}', "
            f"config_yaml = '{_escape(config_yaml)}', "
            f"status = '{_escape(status)}', "
            f"validation_errors = '{_escape(validation_errors)}', "
            f"updated_at = '{_escape(now)}', "
            f"expiration_date = '{_escape(expiration_date)}', "
            f"description = {_sql_val(description)} "
            f"WHERE dr_id = '{_escape(dr_id)}'"
        )
        db_client.sql_exec(sql)

    def get(self, db_client: DbClient, *, dr_id: str) -> dict[str, Any] | None:
        """Fetch a single config row by dr_id, or None if not found."""
        sql = f"SELECT * FROM {self._table} WHERE dr_id = '{_escape(dr_id)}'"
        rows = db_client.sql(sql)
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
            f"status = '{_escape(status)}', "
            f"updated_at = '{_escape(now)}' "
            f"WHERE dr_id = '{_escape(dr_id)}'"
        )
        db_client.sql_exec(sql)

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
            f"manifest_json = '{_escape(manifest_json)}', "
            f"scanned_at = '{_escape(scanned_at)}', "
            f"updated_at = '{_escape(now)}' "
            f"WHERE dr_id = '{_escape(dr_id)}'"
        )
        db_client.sql_exec(sql)

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
        sql = f"DELETE FROM {self._table} WHERE dr_id = '{_escape(dr_id)}'"
        db_client.sql_exec(sql)
        return True
