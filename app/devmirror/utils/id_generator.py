"""Auto-generated DR ID support (Stage 4 US-34).

Provides:
  - :func:`format_dr_id` / :func:`is_legacy_dr_id` -- pure string helpers.
  - :class:`IdCounterRepository` -- Delta-backed counter with
    optimistic-retry atomic increment.
  - :func:`next_dr_id` -- high-level composition used by the API layer.

The counter row shape is ``(prefix STRING, last_value BIGINT, updated_at TIMESTAMP)``
with one row per prefix; the migration DDL lives at
``devmirror/migrations/003_id_counter.sql``.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from devmirror.settings import Settings
    from devmirror.utils.db_client import DbClient

logger = logging.getLogger(__name__)

_LEGACY_DR_ID_PATTERN = re.compile(r"^DR-[0-9]+$")

MAX_COUNTER_RETRIES = 3


def format_dr_id(prefix: str, counter: int, padding: int) -> str:
    """Return ``<prefix><zero-padded counter>``.

    Example: ``format_dr_id("DR", 23, 5) == "DR00023"``.

    The width is enforced via ``str.zfill``; if ``counter`` exceeds the
    configured ``padding`` width the full counter is still rendered (the
    caller is responsible for warning / bumping the padding).
    """
    if counter < 0:
        raise ValueError(f"counter must be non-negative, got {counter}")
    if padding < 1:
        raise ValueError(f"padding must be >= 1, got {padding}")
    return f"{prefix}{str(counter).zfill(padding)}"


def is_legacy_dr_id(dr_id: str) -> bool:
    """Return True if *dr_id* matches the legacy ``DR-<digits>`` format."""
    return bool(_LEGACY_DR_ID_PATTERN.match(dr_id))


class IdCounterRepository:
    """Atomic counter for DR IDs, backed by ``devmirror_id_counter``.

    Usage::

        repo = IdCounterRepository(settings.control_fqn_prefix)
        repo.ensure_table(db_client)
        n = repo.next_value(db_client, settings.dr_id_prefix)

    The :meth:`next_value` call bootstraps the per-prefix row on the first
    invocation and uses an optimistic-retry ``UPDATE`` loop to survive
    concurrent writers.  Under single-worker Uvicorn contention is
    effectively zero; the retry exists so multi-worker deployments remain
    correct.
    """

    def __init__(self, fqn_prefix: str) -> None:
        self._table = f"{fqn_prefix}.devmirror_id_counter"

    @property
    def table_fqn(self) -> str:
        return self._table

    def ensure_table(self, db_client: DbClient) -> None:
        """Create the counter table if it doesn't exist (idempotent)."""
        db_client.sql_exec(
            f"CREATE TABLE IF NOT EXISTS {self._table} ("
            "prefix STRING NOT NULL, "
            "last_value BIGINT NOT NULL, "
            "updated_at TIMESTAMP NOT NULL) USING DELTA"
        )

    def _select_current(
        self, db_client: DbClient, prefix: str
    ) -> int | None:
        sql = (
            f"SELECT last_value FROM {self._table} WHERE prefix = :prefix"
        )
        rows = db_client.sql_with_params(sql, {"prefix": prefix})
        if not rows:
            return None
        val = rows[0].get("last_value")
        # Statement Execution API returns values as strings in JSON_ARRAY;
        # spark.sql returns native ints.  Normalise.
        return int(val) if val is not None else None

    def _insert_initial(
        self, db_client: DbClient, prefix: str
    ) -> bool:
        """Insert the bootstrap row (last_value=1).  Returns True on success.

        A duplicate-key style collision (another worker inserted first) will
        raise; the caller falls back to the update path.
        """
        now = datetime.now(UTC).isoformat()
        sql = (
            f"INSERT INTO {self._table} (prefix, last_value, updated_at) "
            "VALUES (:prefix, 1, :updated_at)"
        )
        try:
            db_client.sql_exec_with_params(
                sql, {"prefix": prefix, "updated_at": now}
            )
            return True
        except Exception:
            logger.debug(
                "Bootstrap insert for prefix=%s collided; retrying via UPDATE",
                prefix,
                exc_info=True,
            )
            return False

    def _try_increment(
        self, db_client: DbClient, prefix: str, current: int
    ) -> bool:
        """Attempt a CAS-style UPDATE; return True if exactly one row changed.

        Delta does not expose affected-row counts through the Statement
        Execution API, so correctness is re-checked by reading back the row
        and comparing ``last_value`` to ``current + 1``.
        """
        now = datetime.now(UTC).isoformat()
        sql = (
            f"UPDATE {self._table} SET "
            "last_value = :new_value, updated_at = :updated_at "
            "WHERE prefix = :prefix AND last_value = :current"
        )
        db_client.sql_exec_with_params(
            sql,
            {
                "prefix": prefix,
                "current": str(current),
                "new_value": str(current + 1),
                "updated_at": now,
            },
        )
        # Read-back verification: if the value advanced to current+1, this
        # worker won the race.  If it advanced past that, another worker
        # slipped in -- we'll retry.
        fresh = self._select_current(db_client, prefix)
        return fresh == current + 1

    def next_value(self, db_client: DbClient, prefix: str) -> int:
        """Allocate and return the next integer for *prefix*.

        Raises :class:`RuntimeError` after 3 failed retries.
        """
        for attempt in range(MAX_COUNTER_RETRIES):
            current = self._select_current(db_client, prefix)
            if current is None:
                # No row yet -- try to bootstrap.
                if self._insert_initial(db_client, prefix):
                    return 1
                # Someone else bootstrapped; re-read on next iteration.
                continue
            if self._try_increment(db_client, prefix, current):
                return current + 1
            logger.warning(
                "IdCounter CAS retry %d/%d for prefix=%s (current=%d)",
                attempt + 1,
                MAX_COUNTER_RETRIES,
                prefix,
                current,
            )
        raise RuntimeError(
            f"Could not acquire next DR ID after {MAX_COUNTER_RETRIES} retries"
        )


def next_dr_id(db_client: DbClient, settings: Settings) -> str:
    """Return the next DR ID string for *settings*'s prefix and padding.

    Ensures the counter table exists (idempotent) and then allocates the
    next integer via :class:`IdCounterRepository`.
    """
    repo = IdCounterRepository(settings.control_fqn_prefix)
    repo.ensure_table(db_client)
    counter = repo.next_value(db_client, settings.dr_id_prefix)
    return format_dr_id(settings.dr_id_prefix, counter, settings.dr_id_padding)
