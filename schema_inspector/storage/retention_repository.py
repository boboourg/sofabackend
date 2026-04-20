"""PostgreSQL repository for housekeeping retention DELETE batches.

All DELETE statements:
  * scope rows to a time cutoff supplied by the caller;
  * cap each statement via an inner ``SELECT ... ORDER BY id LIMIT`` so a
    single call deletes at most ``batch_size`` rows, keeping locks short;
  * return the number of rows actually deleted so the housekeeping loop can
    stop once there is nothing left to do.

The repository intentionally exposes COUNT queries alongside the DELETEs so
that the loop can run in dry-run mode (observe how many rows would be
affected before enabling real deletes).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...
    async def fetchval(self, query: str, *args: object) -> Any: ...


@dataclass(frozen=True)
class RetentionResult:
    deleted: int
    exhausted: bool  # True when the last batch returned < batch_size rows


class RetentionRepository:
    """Batched DELETEs for append-only tables that need time-based retention."""

    # ------------------------------------------------------------------
    # api_request_log — append-only transport log
    # ------------------------------------------------------------------

    async def count_expired_request_logs(
        self,
        executor: SqlExecutor,
        *,
        cutoff: datetime,
    ) -> int:
        value = await executor.fetchval(
            "SELECT count(*) FROM api_request_log WHERE started_at < $1",
            cutoff,
        )
        return int(value or 0)

    async def delete_request_log_batch(
        self,
        executor: SqlExecutor,
        *,
        cutoff: datetime,
        batch_size: int,
    ) -> int:
        # The inner SELECT bounds the DELETE to at most `batch_size` rows.
        # id is PK (btree), so LIMIT + ORDER BY is a single index scan.
        # We read asyncpg's command tag (e.g., "DELETE 4931") to know how
        # many rows actually went.
        command_tag = await executor.execute(
            """
            WITH victims AS (
                SELECT id FROM api_request_log
                WHERE started_at < $1
                ORDER BY id
                LIMIT $2
            )
            DELETE FROM api_request_log
            USING victims
            WHERE api_request_log.id = victims.id
            """,
            cutoff,
            int(batch_size),
        )
        return _parse_delete_tag(command_tag)

    # ------------------------------------------------------------------
    # api_payload_snapshot — NULL-scope legacy rows only
    # ------------------------------------------------------------------
    #
    # Retention policy: delete rows that (a) have no scope_key (legacy,
    # pre-Fix #1) AND (b) are older than the cutoff AND (c) are NOT
    # referenced by any api_snapshot_head.latest_snapshot_id. Condition (c)
    # prevents the sweep from discarding the current snapshot for a scope
    # that is still "current".

    async def count_expired_legacy_snapshots(
        self,
        executor: SqlExecutor,
        *,
        cutoff: datetime,
    ) -> int:
        value = await executor.fetchval(
            """
            SELECT count(*)
            FROM api_payload_snapshot p
            WHERE p.scope_key IS NULL
              AND p.fetched_at < $1
              AND NOT EXISTS (
                  SELECT 1 FROM api_snapshot_head h
                  WHERE h.latest_snapshot_id = p.id
              )
            """,
            cutoff,
        )
        return int(value or 0)

    async def delete_legacy_snapshot_batch(
        self,
        executor: SqlExecutor,
        *,
        cutoff: datetime,
        batch_size: int,
    ) -> int:
        command_tag = await executor.execute(
            """
            WITH victims AS (
                SELECT p.id
                FROM api_payload_snapshot p
                WHERE p.scope_key IS NULL
                  AND p.fetched_at < $1
                  AND NOT EXISTS (
                      SELECT 1 FROM api_snapshot_head h
                      WHERE h.latest_snapshot_id = p.id
                  )
                ORDER BY p.id
                LIMIT $2
            )
            DELETE FROM api_payload_snapshot
            USING victims
            WHERE api_payload_snapshot.id = victims.id
            """,
            cutoff,
            int(batch_size),
        )
        return _parse_delete_tag(command_tag)

    # ------------------------------------------------------------------
    # event_live_state_history — rolling observation log
    # ------------------------------------------------------------------

    async def count_expired_live_state_history(
        self,
        executor: SqlExecutor,
        *,
        cutoff: datetime,
    ) -> int:
        value = await executor.fetchval(
            "SELECT count(*) FROM event_live_state_history WHERE observed_at < $1",
            cutoff,
        )
        return int(value or 0)

    async def delete_live_state_history_batch(
        self,
        executor: SqlExecutor,
        *,
        cutoff: datetime,
        batch_size: int,
    ) -> int:
        command_tag = await executor.execute(
            """
            WITH victims AS (
                SELECT id FROM event_live_state_history
                WHERE observed_at < $1
                ORDER BY id
                LIMIT $2
            )
            DELETE FROM event_live_state_history
            USING victims
            WHERE event_live_state_history.id = victims.id
            """,
            cutoff,
            int(batch_size),
        )
        return _parse_delete_tag(command_tag)


def _parse_delete_tag(command_tag: object) -> int:
    """Parse a ``DELETE <n>`` command tag returned by asyncpg's execute()."""

    if command_tag is None:
        return 0
    text = str(command_tag).strip()
    parts = text.split()
    if len(parts) >= 2 and parts[0].upper() == "DELETE":
        try:
            return int(parts[1])
        except ValueError:
            return 0
    # Fallback: some fakes return ints directly.
    try:
        return int(text)
    except ValueError:
        return 0
