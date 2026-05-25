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

import os
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol


# 2026-05-25 incident follow-up — see delete_legacy_snapshot_batch comment.
# The retention DELETE statements on multi-hundred-GB tables cannot finish
# inside the old hard-coded 120 s ceiling once tables are heavily
# fragmented (large TOAST + NOT EXISTS anti-joins + CASCADE fan-out).
# Operators can extend the per-statement timeout via env. Bounded to
# ``[60, 3600]`` so a misconfigured value can't disable retention or
# stretch one DELETE lock past an hour.
#
# Per-DELETE env keys allow different ceilings per table:
#   * api_payload_snapshot legacy  (164 GB TOAST, slow)         → LEGACY
#   * api_payload_snapshot scoped  (live versions, same table)  → LIVE_VERSIONS
#   * event_live_state_history     (50 MB but no observed_at idx) → STATE_HISTORY
#   * endpoint_capability_observation (13 GB, has observed_at idx) → CAPABILITY
# All fall back to the same generic default ``600`` when their specific
# env is absent.
_RETENTION_TIMEOUT_DEFAULT_SECONDS = 600
_RETENTION_TIMEOUT_MIN_SECONDS = 60
_RETENTION_TIMEOUT_MAX_SECONDS = 3600

_RETENTION_TIMEOUT_ENV_KEYS: dict[str, str] = {
    "legacy_snapshot": "SOFASCORE_RETENTION_LEGACY_SNAPSHOT_TIMEOUT_SECONDS",
    "live_snapshot_versions": "SOFASCORE_RETENTION_LIVE_VERSIONS_TIMEOUT_SECONDS",
    "live_state_history": "SOFASCORE_RETENTION_STATE_HISTORY_TIMEOUT_SECONDS",
    "capability_observation": "SOFASCORE_RETENTION_CAPABILITY_TIMEOUT_SECONDS",
}


def _resolve_retention_timeout_seconds(kind: str) -> int:
    """Resolve per-statement timeout (seconds) for one retention DELETE kind.

    Reads the kind-specific env var (see ``_RETENTION_TIMEOUT_ENV_KEYS``) and
    clamps to ``[60, 3600]``. Falls back to ``600`` on missing/invalid value
    so a typo never disables retention or stretches the lock past an hour.
    """
    env_key = _RETENTION_TIMEOUT_ENV_KEYS.get(kind)
    if env_key is None:
        return _RETENTION_TIMEOUT_DEFAULT_SECONDS
    raw = os.environ.get(env_key)
    if raw is None or not raw.strip():
        return _RETENTION_TIMEOUT_DEFAULT_SECONDS
    try:
        value = int(raw.strip())
    except ValueError:
        return _RETENTION_TIMEOUT_DEFAULT_SECONDS
    if value < _RETENTION_TIMEOUT_MIN_SECONDS:
        return _RETENTION_TIMEOUT_MIN_SECONDS
    if value > _RETENTION_TIMEOUT_MAX_SECONDS:
        return _RETENTION_TIMEOUT_MAX_SECONDS
    return value


def _resolve_legacy_snapshot_timeout_seconds() -> int:
    """Backwards-compatible wrapper used by existing tests."""
    return _resolve_retention_timeout_seconds("legacy_snapshot")


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
        # Phase 2.5 (2026-05-20 perf audit): the batched DELETE
        # legitimately takes ~60s on the 148 GB api_payload_snapshot
        # table (NOT EXISTS anti-join against api_snapshot_head plus
        # CASCADE / SET NULL FK fan-out). The global P0 fix sets
        # sofascore_user.statement_timeout = 30s — without escaping it
        # inside the txn, retention will fail mid-batch.
        #
        # 2026-05-25 incident follow-up: the 120 s ceiling proved too
        # tight once api_payload_snapshot grew past ~140 GB TOAST — every
        # tick hit TimeoutError and deleted 0 rows, letting the 5.37M-
        # row backlog snowball until /dev/md2 reached 100 %. Default
        # bumped to 600 s (operator-tunable via env). Lowering batch_size
        # via SOFASCORE_HOUSEKEEPING_BATCH_SIZE (e.g. 20000 → 2000) keeps
        # each statement well under the new ceiling while still draining
        # backlog at acceptable throughput.
        timeout_seconds = _resolve_retention_timeout_seconds("legacy_snapshot")
        async with executor.transaction():
            await executor.execute(
                f"SET LOCAL statement_timeout = '{timeout_seconds}s'"
            )
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
                      -- Task 2 Phase D (2026-05-20): pin final snapshots
                      -- of locked events. Once event_terminal_state
                      -- stamps locked_at the payload is the canonical
                      -- frozen body — retention must NEVER delete it,
                      -- otherwise the read-path loses ground truth.
                      AND NOT EXISTS (
                          SELECT 1 FROM event_terminal_state ets
                          WHERE ets.final_snapshot_id = p.id
                            AND ets.locked_at IS NOT NULL
                      )
                    -- 2026-05-25 perf fix: ORDER BY fetched_at lets the
                    -- planner use the partial index ``idx_aps_legacy_retention
                    -- btree (fetched_at) WHERE scope_key IS NULL`` instead of
                    -- walking the PK btree (which Filtered scope_key/fetched_at
                    -- post-scan — cost 25M for 1.5M matching rows ⇒ batch never
                    -- completed under the 600 s ceiling). The secondary
                    -- ``p.id`` keeps the order deterministic for concurrent
                    -- maintenance workers and stable across batches.
                    ORDER BY p.fetched_at, p.id
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
    # api_payload_snapshot - scoped live/event snapshot versions
    # ------------------------------------------------------------------
    #
    # Live/event payloads are rewritten frequently. The local API still
    # needs the latest raw payload for every scope, so this retention step
    # deletes only older scoped versions that are not referenced by
    # api_snapshot_head.latest_snapshot_id.

    async def count_expired_live_snapshot_versions(
        self,
        executor: SqlExecutor,
        *,
        cutoff: datetime,
    ) -> int:
        value = await executor.fetchval(
            """
            SELECT count(*)
            FROM api_payload_snapshot p
            WHERE p.scope_key IS NOT NULL
              AND p.fetched_at < $1
              AND (
                  p.endpoint_pattern LIKE '/api/v1/event/%'
                  OR p.endpoint_pattern LIKE '/api/v1/sport/%/events/live'
              )
              AND NOT EXISTS (
                  SELECT 1 FROM api_snapshot_head h
                  WHERE h.latest_snapshot_id = p.id
              )
            """,
            cutoff,
        )
        return int(value or 0)

    async def delete_live_snapshot_version_batch(
        self,
        executor: SqlExecutor,
        *,
        cutoff: datetime,
        batch_size: int,
    ) -> int:
        # 2026-05-25 incident follow-up: wrap DELETE in its own transaction
        # so ``SET LOCAL statement_timeout`` overrides the per-user 30 s
        # default that was killing this step mid-batch. See
        # ``delete_legacy_snapshot_batch`` for the full background.
        # Ordering is unchanged (``ORDER BY p.id``) — no partial index
        # currently targets ``scope_key IS NOT NULL`` so switching to
        # ``fetched_at`` would force a Seq Scan + Sort (worse than the
        # current PK Index Scan + filter early-terminating at LIMIT).
        timeout_seconds = _resolve_retention_timeout_seconds("live_snapshot_versions")
        async with executor.transaction():
            await executor.execute(
                f"SET LOCAL statement_timeout = '{timeout_seconds}s'"
            )
            command_tag = await executor.execute(
                """
                WITH victims AS (
                    SELECT p.id
                    FROM api_payload_snapshot p
                    WHERE p.scope_key IS NOT NULL
                      AND p.fetched_at < $1
                      AND (
                          p.endpoint_pattern LIKE '/api/v1/event/%'
                          OR p.endpoint_pattern LIKE '/api/v1/sport/%/events/live'
                      )
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
    # endpoint_capability_observation - append-only probe log
    # ------------------------------------------------------------------
    #
    # Aggregates roll up into endpoint_capability_rollup; the raw
    # observation row is only useful for short-term debugging.

    async def count_expired_capability_observations(
        self,
        executor: SqlExecutor,
        *,
        cutoff: datetime,
    ) -> int:
        value = await executor.fetchval(
            "SELECT count(*) FROM endpoint_capability_observation WHERE observed_at < $1",
            cutoff,
        )
        return int(value or 0)

    async def delete_capability_observation_batch(
        self,
        executor: SqlExecutor,
        *,
        cutoff: datetime,
        batch_size: int,
    ) -> int:
        # 2026-05-25: wrap in SET LOCAL statement_timeout for consistency
        # with the other retention steps. ORDER BY observed_at uses the
        # existing btree (observed_at) index so the inner SELECT is a
        # cheap ranged scan instead of a PK walk with filter.
        timeout_seconds = _resolve_retention_timeout_seconds("capability_observation")
        async with executor.transaction():
            await executor.execute(
                f"SET LOCAL statement_timeout = '{timeout_seconds}s'"
            )
            command_tag = await executor.execute(
                """
                WITH victims AS (
                    SELECT id FROM endpoint_capability_observation
                    WHERE observed_at < $1
                    ORDER BY observed_at, id
                    LIMIT $2
                )
                DELETE FROM endpoint_capability_observation
                USING victims
                WHERE endpoint_capability_observation.id = victims.id
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
        # 2026-05-25: wrap in SET LOCAL statement_timeout for the same
        # reasons as legacy snapshots. The table currently has only a
        # PK btree index — no index on observed_at — so the inner SELECT
        # is a PK Index Scan with post-scan filter (cost ~577K for the
        # 30-day cutoff). That fits comfortably in a 600 s timeout, but
        # the per-user 30 s cap was killing it. Keep ``ORDER BY id`` to
        # match the only available index.
        timeout_seconds = _resolve_retention_timeout_seconds("live_state_history")
        async with executor.transaction():
            await executor.execute(
                f"SET LOCAL statement_timeout = '{timeout_seconds}s'"
            )
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
