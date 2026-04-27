"""Unit tests for devmirror.scan.lineage (including enrichment)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from devmirror.scan.lineage import (
    LineageResult,
    _build_lineage_sql,
    get_enrichment_table,
    query_enrichment,
    query_lineage,
    query_table_sizes,
)
from devmirror.scan.stream_resolver import ResolvedStream


def _mock_db(rows=None) -> MagicMock:
    m = MagicMock()
    m.sql_exec = MagicMock()
    m.sql = MagicMock(return_value=rows or [])
    # query_table_sizes (Sec finding #14) now uses sql_with_params; route
    # the mock to sql so existing return_value setups still work.
    m.sql_with_params = MagicMock(return_value=rows or [])
    return m


class TestBuildLineageSQL:
    def test_single_entity(self) -> None:
        sql = _build_lineage_sql("system.access.table_lineage", ["123"], 100)
        assert "entity_id IN ('123')" in sql
        assert "LIMIT 101" in sql

    def test_multiple_entities(self) -> None:
        sql = _build_lineage_sql("system.access.table_lineage", ["a", "b", "c"], 50)
        assert "entity_id IN ('a', 'b', 'c')" in sql
        assert "LIMIT 51" in sql

    def test_escapes_single_quotes(self) -> None:
        sql = _build_lineage_sql("system.access.table_lineage", ["it's"], 10)
        assert "it''s" in sql

    def test_custom_lineage_table(self) -> None:
        sql = _build_lineage_sql("my_catalog.my_schema.my_lineage", ["x"], 5)
        assert "FROM my_catalog.my_schema.my_lineage" in sql


class TestQueryLineage:
    def test_returns_edges(self) -> None:
        db = _mock_db([
            {
                "source_table_full_name": "cat.sch.src_table",
                "target_table_full_name": "cat.sch.tgt_table",
                "source_type": "TABLE",
                "target_type": "TABLE",
                "entity_id": "job-1",
            },
            {
                "source_table_full_name": "cat.sch.dim_table",
                "target_table_full_name": None,
                "source_type": "VIEW",
                "target_type": None,
                "entity_id": "job-1",
            },
        ])

        streams = [
            ResolvedStream(name="my_job", resource_type="job", resource_id="job-1"),
        ]

        result = query_lineage(db, streams)
        assert isinstance(result, LineageResult)
        assert len(result.edges) == 2
        assert result.row_limit_hit is False

        assert result.edges[0].source_table_fqn == "cat.sch.src_table"
        assert result.edges[0].target_table_fqn == "cat.sch.tgt_table"
        assert result.edges[1].source_type == "VIEW"
        assert result.edges[1].target_table_fqn is None

    def test_row_limit_hit(self) -> None:
        db = _mock_db([
            {
                "source_table_full_name": f"cat.sch.t{i}",
                "target_table_full_name": None,
                "source_type": "TABLE",
                "target_type": None,
                "entity_id": "j1",
            }
            for i in range(6)
        ])

        streams = [
            ResolvedStream(name="job", resource_type="job", resource_id="j1"),
        ]

        result = query_lineage(db, streams, row_limit=5)
        assert result.row_limit_hit is True
        assert len(result.edges) == 5

    def test_empty_streams(self) -> None:
        db = _mock_db()
        result = query_lineage(db, [])
        assert result.edges == []
        assert result.row_limit_hit is False
        db.sql.assert_not_called()

    def test_no_rows_returned(self) -> None:
        db = _mock_db()

        streams = [
            ResolvedStream(name="job", resource_type="job", resource_id="j1"),
        ]
        result = query_lineage(db, streams)
        assert result.edges == []
        assert result.row_limit_hit is False

    def test_uses_custom_lineage_table(self) -> None:
        db = _mock_db()

        streams = [
            ResolvedStream(name="job", resource_type="job", resource_id="j1"),
        ]
        query_lineage(
            db,
            streams,
            lineage_table="custom.schema.lineage",
        )

        sql_arg = db.sql.call_args[0][0]
        assert "custom.schema.lineage" in sql_arg


class TestQueryTableSizes:
    def test_returns_sizes_in_gb(self) -> None:
        db = _mock_db([
            {"table_name": "orders", "data_size_in_bytes": 1_073_741_824},  # exactly 1 GB
            {"table_name": "customers", "data_size_in_bytes": 536_870_912},  # 0.5 GB
        ])

        result = query_table_sizes(
            db,
            ["prod.sales.orders", "prod.sales.customers"],
        )

        assert result["prod.sales.orders"] == 1.0
        assert result["prod.sales.customers"] == 0.5

    def test_empty_list_returns_empty(self) -> None:
        db = _mock_db()
        result = query_table_sizes(db, [])
        assert result == {}
        db.sql.assert_not_called()

    def test_groups_by_catalog_schema(self) -> None:
        db = _mock_db()

        query_table_sizes(
            db,
            [
                "cat1.sch1.t1",
                "cat1.sch1.t2",
                "cat2.sch2.t3",
            ],
        )

        # Should have been called twice (once per catalog.schema group)
        assert db.sql_with_params.call_count == 2

    def test_skips_invalid_fqn(self) -> None:
        db = _mock_db([
            {"table_name": "t1", "data_size_in_bytes": 0},
        ])

        result = query_table_sizes(
            db,
            ["invalid_fqn", "cat.sch.t1"],
        )

        # Only the valid FQN should be queried
        assert db.sql_with_params.call_count == 1
        assert "cat.sch.t1" in result

    def test_sql_error_is_silently_skipped(self) -> None:
        db = _mock_db()
        db.sql_with_params.side_effect = RuntimeError("access denied")

        result = query_table_sizes(
            db,
            ["cat.sch.t1"],
        )

        assert result == {}

    def test_queries_correct_information_schema(self) -> None:
        db = _mock_db()

        query_table_sizes(db, ["my_catalog.my_schema.my_table"])

        # Catalog is interpolated (identifier in FROM); schema and table_name
        # are now bound via :schema_name and :t0 parameters.
        sql_arg, params = db.sql_with_params.call_args[0]
        assert "my_catalog.information_schema.tables" in sql_arg
        assert "table_schema = :schema_name" in sql_arg
        assert "table_name IN (:t0)" in sql_arg
        assert params["schema_name"] == "my_schema"
        assert params["t0"] == "my_table"

    def test_rejects_unsafe_catalog(self) -> None:
        """Sec finding #14: catalog with quotes/special chars is dropped."""
        db = _mock_db([])
        # A malicious FQN whose first segment looks like a SQL injection
        # attempt is rejected by _IDENT_RE before the SQL is built.
        result = query_table_sizes(
            db,
            ["bad'cat.sch.tbl", "good_cat.sch.tbl"],
        )
        assert result == {}
        # Only the safe catalog led to a SQL call.
        assert db.sql_with_params.call_count == 1
        sql_arg, _params = db.sql_with_params.call_args[0]
        assert "good_cat.information_schema.tables" in sql_arg


# ===========================================================================
# Enrichment tests (merged from test_enrichment.py)
# ===========================================================================


class TestGetEnrichmentTable:
    @patch.dict("os.environ", {"DEVMIRROR_LINEAGE_ENRICHMENT_TABLE": "cat.sch.enrich"})
    def test_returns_env_value(self) -> None:
        assert get_enrichment_table() == "cat.sch.enrich"

    @patch.dict("os.environ", {"DEVMIRROR_LINEAGE_ENRICHMENT_TABLE": ""})
    def test_empty_returns_none(self) -> None:
        assert get_enrichment_table() is None

    @patch.dict("os.environ", {}, clear=True)
    def test_missing_returns_none(self) -> None:
        assert get_enrichment_table() is None


class TestQueryEnrichment:
    def test_disabled_when_no_table(self) -> None:
        db = _mock_db()
        r = query_enrichment(db, ["s1"], enrichment_table=None)
        assert not r.enabled and r.edges == []

    def test_read_hint(self) -> None:
        db = _mock_db([{"stream_key": "j1", "object_fqn": "c.s.t", "access_hint": "READ"}])
        r = query_enrichment(db, ["j1"], enrichment_table="c.s.e")
        assert r.enabled and r.edges[0].source_table_fqn == "c.s.t" and r.edges[0].target_table_fqn is None

    def test_write_hint(self) -> None:
        db = _mock_db([{"stream_key": "j1", "object_fqn": "c.s.t", "access_hint": "WRITE"}])
        r = query_enrichment(db, ["j1"], enrichment_table="c.s.e")
        assert r.edges[0].target_table_fqn == "c.s.t" and r.edges[0].source_table_fqn is None

    def test_skips_empty_fqn(self) -> None:
        db = _mock_db([
            {"stream_key": "j1", "object_fqn": None, "access_hint": "READ"},
            {"stream_key": "j1", "object_fqn": "c.s.ok", "access_hint": "READ"},
        ])
        r = query_enrichment(db, ["j1"], enrichment_table="c.s.e")
        assert len(r.edges) == 1
