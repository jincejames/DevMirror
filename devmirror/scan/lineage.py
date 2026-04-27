"""Query Unity Catalog lineage tables and optional enrichment for dependency discovery."""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from devmirror.scan.stream_resolver import ResolvedStream
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)

DEFAULT_ROW_LIMIT = 10_000


@dataclass
class LineageEdge:
    """A single lineage edge from the system lineage table."""

    source_table_fqn: str | None
    target_table_fqn: str | None
    source_type: str | None = None
    target_type: str | None = None
    entity_id: str | None = None


@dataclass
class LineageResult:
    """Container for lineage query results."""

    edges: list[LineageEdge]
    row_limit_hit: bool = False


@dataclass
class EnrichmentResult:
    """Result of enrichment query."""

    edges: list[LineageEdge]
    enabled: bool = False


def _build_lineage_sql(
    lineage_table: str,
    entity_ids: list[str],
    row_limit: int,
) -> str:
    """Build the SQL query for the lineage table."""
    escaped_ids = [eid.replace("'", "''") for eid in entity_ids]
    id_list = ", ".join(f"'{eid}'" for eid in escaped_ids)

    return f"""\
SELECT
    source_table_full_name,
    target_table_full_name,
    source_type,
    target_type,
    entity_id
FROM {lineage_table}
WHERE entity_id IN ({id_list})
LIMIT {row_limit + 1}
"""


def query_lineage(
    db_client: DbClient,
    streams: list[ResolvedStream],
    lineage_table: str = "system.access.table_lineage",
    row_limit: int = DEFAULT_ROW_LIMIT,
) -> LineageResult:
    """Query the lineage system table for edges associated with resolved streams."""
    entity_ids = [s.resource_id for s in streams]
    if not entity_ids:
        return LineageResult(edges=[], row_limit_hit=False)

    sql = _build_lineage_sql(lineage_table, entity_ids, row_limit)
    rows = db_client.sql(sql)

    row_limit_hit = len(rows) > row_limit
    rows = rows[:row_limit] if row_limit_hit else rows

    edges: list[LineageEdge] = []
    for row in rows:
        edges.append(
            LineageEdge(
                source_table_fqn=row.get("source_table_full_name"),
                target_table_fqn=row.get("target_table_full_name"),
                source_type=row.get("source_type"),
                target_type=row.get("target_type"),
                entity_id=row.get("entity_id"),
            )
        )

    return LineageResult(edges=edges, row_limit_hit=row_limit_hit)


_BYTES_PER_GB = 1_073_741_824  # 1024^3


# Strict identifier regex for catalog names that go into FROM clauses.
# Spark SQL doesn't allow binding identifiers as parameters, so we gate
# the interpolation behind a tight whitelist.
_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def query_table_sizes(
    db_client: DbClient,
    table_fqns: list[str],
) -> dict[str, float]:
    """Query ``information_schema.tables`` for the size of each table in GB."""
    if not table_fqns:
        return {}

    groups: dict[tuple[str, str], list[str]] = {}
    for fqn in table_fqns:
        parts = fqn.split(".")
        if len(parts) != 3:
            continue
        catalog, schema, table_name = parts
        # Reject catalog identifiers that don't match the strict whitelist
        # (anything with quotes, spaces, semicolons, etc.).  Defense in
        # depth -- upstream FQN validation should already have rejected
        # these, but we don't trust transitively.
        if not _IDENT_RE.match(catalog):
            logger.warning("Refusing to query lineage for unsafe catalog %r", catalog)
            continue
        key = (catalog, schema)
        groups.setdefault(key, []).append(table_name)

    sizes: dict[str, float] = {}
    for (catalog, schema), table_names in groups.items():
        # Table names go into IN (:t0, :t1, ...) via named parameters.
        # Schema goes through :schema_name.  Catalog must be interpolated
        # (it's an identifier in FROM, not bindable) -- already gated by
        # _IDENT_RE above.
        params: dict[str, str] = {"schema_name": schema}
        placeholders: list[str] = []
        for i, name in enumerate(table_names):
            key = f"t{i}"
            params[key] = name
            placeholders.append(f":{key}")
        sql = (
            "SELECT table_name, data_size_in_bytes "
            f"FROM {catalog}.information_schema.tables "
            "WHERE table_schema = :schema_name "
            f"AND table_name IN ({', '.join(placeholders)})"
        )
        try:
            rows = db_client.sql_with_params(sql, params)
        except Exception:
            logger.debug(
                "Failed to query table sizes for %s.%s, skipping",
                catalog,
                schema,
            )
            continue

        for row in rows:
            tbl_name = row.get("table_name", "")
            raw_bytes = row.get("data_size_in_bytes")
            if tbl_name and raw_bytes is not None:
                fqn = f"{catalog}.{schema}.{tbl_name}"
                sizes[fqn] = round(float(raw_bytes) / _BYTES_PER_GB, 6)

    return sizes


# ---- Enrichment (merged from scan/enrichment.py) ----


def get_enrichment_table() -> str | None:
    """Read the enrichment table name from the environment."""
    return os.environ.get("DEVMIRROR_LINEAGE_ENRICHMENT_TABLE", "").strip() or None


def query_enrichment(
    db_client: DbClient,
    stream_keys: list[str],
    enrichment_table: str | None = None,
) -> EnrichmentResult:
    """Query the curated enrichment table for additional lineage edges."""
    table = enrichment_table or get_enrichment_table()
    if not table or not stream_keys:
        return EnrichmentResult(edges=[], enabled=False)

    escaped_keys = [k.replace("'", "''") for k in stream_keys]
    key_list = ", ".join(f"'{k}'" for k in escaped_keys)

    sql = f"""\
SELECT stream_key, object_fqn, access_hint
FROM {table}
WHERE stream_key IN ({key_list})
"""

    rows = db_client.sql(sql)

    edges: list[LineageEdge] = []
    for row in rows:
        access_hint = (row.get("access_hint") or "READ").upper()
        fqn = row.get("object_fqn")
        if not fqn:
            continue

        if access_hint in ("WRITE", "READ_WRITE"):
            edges.append(
                LineageEdge(
                    source_table_fqn=fqn if access_hint == "READ_WRITE" else None,
                    target_table_fqn=fqn,
                    entity_id=row.get("stream_key"),
                )
            )
        else:
            edges.append(
                LineageEdge(
                    source_table_fqn=fqn,
                    target_table_fqn=None,
                    entity_id=row.get("stream_key"),
                )
            )

    return EnrichmentResult(edges=edges, enabled=True)
