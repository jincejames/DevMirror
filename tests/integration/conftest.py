"""Shared fixtures for integration tests.

Provides a FakeSqlExecutor that tracks all SQL statements and maintains
an in-memory dict simulating control table state, allowing full lifecycle
tests without any Databricks connectivity.
"""

from __future__ import annotations

import re
from typing import Any

import pytest


class FakeSqlExecutor:
    """In-memory SQL executor that records statements and fakes responses.

    Tracks all SQL statements executed for assertion.  Maintains a simple
    in-memory store keyed by table name so that INSERT/UPDATE/SELECT round-trip
    correctly for the control tables.  Supports injecting failures for specific
    SQL patterns.
    """

    def __init__(self) -> None:
        self.executed: list[str] = []
        self._tables: dict[str, list[dict[str, Any]]] = {}
        self._failure_patterns: list[tuple[re.Pattern[str], str]] = []
        self._fetch_overrides: list[tuple[re.Pattern[str], list[dict[str, Any]]]] = []

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------

    def add_failure_pattern(self, pattern: str, error_msg: str = "Simulated failure") -> None:
        """Register a regex pattern; any matching SQL will raise an exception."""
        self._failure_patterns.append((re.compile(pattern, re.IGNORECASE), error_msg))

    def remove_failure_pattern(self, pattern: str) -> None:
        """Remove a previously registered failure pattern."""
        compiled = re.compile(pattern, re.IGNORECASE)
        self._failure_patterns = [
            (p, m) for p, m in self._failure_patterns if p.pattern != compiled.pattern
        ]

    def add_fetch_override(self, pattern: str, rows: list[dict[str, Any]]) -> None:
        """Return *rows* for any fetch_rows call whose SQL matches *pattern*."""
        self._fetch_overrides.append((re.compile(pattern, re.IGNORECASE), rows))

    def clear_fetch_overrides(self) -> None:
        """Remove all fetch overrides."""
        self._fetch_overrides.clear()

    # ------------------------------------------------------------------
    # Core API (matches SqlExecutor interface)
    # ------------------------------------------------------------------

    def execute(self, sql: str, **_kwargs: Any) -> None:
        """Execute SQL, recording the statement and checking for injected failures."""
        self.executed.append(sql)

        for pattern, error_msg in self._failure_patterns:
            if pattern.search(sql):
                raise RuntimeError(error_msg)

        # Best-effort in-memory tracking for INSERT statements
        self._maybe_track_insert(sql)
        self._maybe_track_update(sql)
        self._maybe_track_delete(sql)

    def fetch_rows(self, sql: str, **_kwargs: Any) -> list[dict[str, Any]]:
        """Execute a SELECT-like query and return matching rows."""
        self.executed.append(sql)

        for pattern, error_msg in self._failure_patterns:
            if pattern.search(sql):
                raise RuntimeError(error_msg)

        # Check overrides first
        for pattern, rows in self._fetch_overrides:
            if pattern.search(sql):
                return list(rows)

        # Best-effort in-memory SELECT
        return self._maybe_select(sql)

    # ------------------------------------------------------------------
    # Assertion helpers
    # ------------------------------------------------------------------

    def sql_containing(self, substring: str) -> list[str]:
        """Return all executed SQL statements containing *substring*."""
        sub_upper = substring.upper()
        return [s for s in self.executed if sub_upper in s.upper()]

    def sql_matching(self, pattern: str) -> list[str]:
        """Return all executed SQL statements matching a regex *pattern*."""
        compiled = re.compile(pattern, re.IGNORECASE)
        return [s for s in self.executed if compiled.search(s)]

    def count_sql(self, substring: str) -> int:
        """Count SQL statements containing *substring* (case-insensitive)."""
        return len(self.sql_containing(substring))

    def reset(self) -> None:
        """Clear all recorded statements and in-memory tables."""
        self.executed.clear()
        self._tables.clear()
        self._failure_patterns.clear()
        self._fetch_overrides.clear()

    # ------------------------------------------------------------------
    # In-memory table tracking (best-effort SQL parsing)
    # ------------------------------------------------------------------

    def _table_name_from_sql(self, sql: str, keyword: str) -> str | None:
        """Extract the table name after a SQL keyword (INSERT INTO, UPDATE, etc.)."""
        pattern = rf"{keyword}\s+(\S+)"
        m = re.search(pattern, sql, re.IGNORECASE)
        return m.group(1) if m else None

    def _maybe_track_insert(self, sql: str) -> None:
        upper = sql.upper().strip()
        if not upper.startswith("INSERT INTO"):
            return

        table = self._table_name_from_sql(sql, "INSERT INTO")
        if not table:
            return

        # Extract column names and values (simplified parser)
        col_match = re.search(r"\(([^)]+)\)\s*VALUES\s*\((.+)\)\s*$", sql, re.IGNORECASE | re.DOTALL)
        if not col_match:
            return

        col_names = [c.strip() for c in col_match.group(1).split(",")]
        raw_vals = self._split_values(col_match.group(2))

        row: dict[str, Any] = {}
        for name, val in zip(col_names, raw_vals, strict=False):
            row[name] = self._parse_value(val)

        self._tables.setdefault(table, []).append(row)

    def _maybe_track_update(self, sql: str) -> None:
        upper = sql.upper().strip()
        if not upper.startswith("UPDATE"):
            return

        table = self._table_name_from_sql(sql, "UPDATE")
        if not table:
            return
        if table not in self._tables:
            return

        # Extract SET clause
        set_match = re.search(r"SET\s+(.+?)\s+WHERE\s+(.+)$", sql, re.IGNORECASE | re.DOTALL)
        if not set_match:
            return

        set_clause = set_match.group(1)
        where_clause = set_match.group(2)

        # Parse SET assignments
        assignments: dict[str, Any] = {}
        for part in self._split_set_clause(set_clause):
            eq_parts = part.split("=", 1)
            if len(eq_parts) == 2:
                assignments[eq_parts[0].strip()] = self._parse_value(eq_parts[1].strip())

        # Parse WHERE conditions (simple equality only)
        conditions = self._parse_where(where_clause)

        for row in self._tables[table]:
            if self._row_matches(row, conditions):
                row.update(assignments)

    def _maybe_track_delete(self, sql: str) -> None:
        upper = sql.upper().strip()
        if not upper.startswith("DELETE FROM"):
            return

        table = self._table_name_from_sql(sql, "DELETE FROM")
        if not table or table not in self._tables:
            return

        where_match = re.search(r"WHERE\s+(.+)$", sql, re.IGNORECASE | re.DOTALL)
        if not where_match:
            self._tables[table] = []
            return

        conditions = self._parse_where(where_match.group(1))
        self._tables[table] = [
            row for row in self._tables[table] if not self._row_matches(row, conditions)
        ]

    def _maybe_select(self, sql: str) -> list[dict[str, Any]]:
        upper = sql.upper().strip()
        if not upper.startswith("SELECT"):
            return []

        table_match = re.search(r"FROM\s+(\S+)", sql, re.IGNORECASE)
        if not table_match:
            return []

        table = table_match.group(1)
        if table not in self._tables:
            return []

        where_match = re.search(r"WHERE\s+(.+?)(?:\s+ORDER\s|\s+LIMIT\s|$)", sql, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return list(self._tables[table])

        conditions = self._parse_where(where_match.group(1))
        return [row for row in self._tables[table] if self._row_matches(row, conditions)]

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _split_values(raw: str) -> list[str]:
        """Split a VALUES clause into individual value strings, respecting quotes."""
        values: list[str] = []
        current: list[str] = []
        in_quote = False
        for ch in raw:
            if ch == "'" and not in_quote:
                in_quote = True
                current.append(ch)
            elif ch == "'" and in_quote:
                current.append(ch)
                # Peek ahead not possible in single-char iteration, accept it
                in_quote = False
            elif ch == "," and not in_quote:
                values.append("".join(current).strip())
                current = []
            else:
                current.append(ch)
        if current:
            values.append("".join(current).strip())
        return values

    @staticmethod
    def _split_set_clause(clause: str) -> list[str]:
        """Split a SET clause by commas, respecting quoted strings."""
        parts: list[str] = []
        current: list[str] = []
        in_quote = False
        for ch in clause:
            if ch == "'":
                in_quote = not in_quote
                current.append(ch)
            elif ch == "," and not in_quote:
                parts.append("".join(current).strip())
                current = []
            else:
                current.append(ch)
        if current:
            parts.append("".join(current).strip())
        return parts

    @staticmethod
    def _parse_value(raw: str) -> Any:
        stripped = raw.strip()
        if stripped.upper() == "NULL":
            return None
        if stripped.startswith("'") and stripped.endswith("'"):
            return stripped[1:-1].replace("''", "'")
        try:
            return int(stripped)
        except ValueError:
            pass
        try:
            return float(stripped)
        except ValueError:
            pass
        return stripped

    @staticmethod
    def _parse_where(clause: str) -> list[tuple[str, str, Any]]:
        """Parse simple WHERE conditions (col = 'val' AND col = 'val')."""
        conditions: list[tuple[str, str, Any]] = []
        # Split on AND, handling both simple equality and IN clauses
        parts = re.split(r"\s+AND\s+", clause, flags=re.IGNORECASE)
        for part in parts:
            part = part.strip()
            # Handle IN clauses
            in_match = re.match(r"(\w+)\s+IN\s*\((.+)\)", part, re.IGNORECASE)
            if in_match:
                col = in_match.group(1)
                vals_raw = in_match.group(2)
                vals = [
                    v.strip().strip("'").replace("''", "'")
                    for v in vals_raw.split(",")
                ]
                conditions.append((col, "IN", vals))
                continue
            # Handle IS NULL
            null_match = re.match(r"(\w+)\s+IS\s+NULL", part, re.IGNORECASE)
            if null_match:
                conditions.append((null_match.group(1), "IS", None))
                continue
            # Handle <=
            lte_match = re.match(r"(\w+)\s*<=\s*(.+)", part)
            if lte_match:
                conditions.append((lte_match.group(1), "<=", lte_match.group(2).strip()))
                continue
            # Handle simple equality
            eq_match = re.match(r"(\w+)\s*=\s*(.+)", part)
            if eq_match:
                col = eq_match.group(1).strip()
                val = FakeSqlExecutor._parse_value(eq_match.group(2).strip())
                conditions.append((col, "=", val))
        return conditions

    @staticmethod
    def _row_matches(row: dict[str, Any], conditions: list[tuple[str, str, Any]]) -> bool:
        for col, op, val in conditions:
            row_val = row.get(col)
            if op == "=":
                if str(row_val) != str(val) if row_val is not None and val is not None else row_val != val:
                    return False
            elif op == "IN":
                if str(row_val) not in [str(v) for v in val]:
                    return False
            elif op == "IS":
                if val is None and row_val is not None:
                    return False
            elif op == "<=":
                pass  # Skip complex comparisons; let tests use overrides
        return True

    # ------------------------------------------------------------------
    # Direct table manipulation for test setup
    # ------------------------------------------------------------------

    def seed_table(self, table: str, rows: list[dict[str, Any]]) -> None:
        """Directly insert rows into the in-memory table (no SQL recorded)."""
        self._tables.setdefault(table, []).extend(rows)

    def get_table(self, table: str) -> list[dict[str, Any]]:
        """Return all rows from an in-memory table."""
        return list(self._tables.get(table, []))


@pytest.fixture()
def fake_executor() -> FakeSqlExecutor:
    """A fresh FakeSqlExecutor for each test."""
    return FakeSqlExecutor()


@pytest.fixture()
def control_fqn_prefix() -> str:
    """Standard control table FQN prefix for tests."""
    return "test_catalog.devmirror_admin"


@pytest.fixture()
def repos(control_fqn_prefix: str) -> dict:
    """Build real repository instances using the test FQN prefix."""
    from devmirror.control.audit import AuditRepository
    from devmirror.control.control_table import (
        DrAccessRepository,
        DrObjectRepository,
        DRRepository,
    )

    return {
        "dr_repo": DRRepository(control_fqn_prefix),
        "obj_repo": DrObjectRepository(control_fqn_prefix),
        "access_repo": DrAccessRepository(control_fqn_prefix),
        "audit_repo": AuditRepository(control_fqn_prefix),
    }
