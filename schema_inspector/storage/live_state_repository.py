"""PostgreSQL repository for durable live-event lifecycle state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from ._temporal import coerce_timestamptz
from ..source_priority import should_existing_source_win


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


class SqlFetchExecutor(SqlExecutor, Protocol):
    async def fetch(self, query: str, *args: object) -> Any: ...


def _command_tag_affected(command_tag: object) -> int:
    """Extract the row count from asyncpg command-tag strings like
    ``"UPDATE 3"``, ``"DELETE 17"``, ``"INSERT 0 5"``. Fakes that
    return an int directly are also tolerated.
    """

    if command_tag is None:
        return 0
    if isinstance(command_tag, int):
        return int(command_tag)
    parts = str(command_tag).strip().split()
    if not parts:
        return 0
    # INSERT tag is "INSERT 0 N" — count is the LAST integer.
    for token in reversed(parts):
        try:
            return int(token)
        except ValueError:
            continue
    return 0


@dataclass(frozen=True)
class EventLiveStateHistoryRecord:
    event_id: int
    observed_status_type: str | None
    poll_profile: str | None
    home_score: int | None
    away_score: int | None
    period_label: str | None
    observed_at: str


@dataclass(frozen=True)
class EventTerminalStateRecord:
    event_id: int
    terminal_status: str
    finalized_at: str
    final_snapshot_id: int | None


class LiveStateRepository:
    """Writes durable live-state history and terminal snapshots."""

    async def insert_live_state_history(self, executor: SqlExecutor, record: EventLiveStateHistoryRecord) -> None:
        await executor.execute(
            """
            INSERT INTO event_live_state_history (
                event_id,
                observed_status_type,
                poll_profile,
                home_score,
                away_score,
                period_label,
                observed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            record.event_id,
            record.observed_status_type,
            record.poll_profile,
            record.home_score,
            record.away_score,
            record.period_label,
            coerce_timestamptz(record.observed_at),
        )

    async def upsert_terminal_state(
        self,
        executor: SqlExecutor,
        record: EventTerminalStateRecord,
        *,
        existing_source_slug: str | None = None,
        incoming_source_slug: str | None = None,
    ) -> bool:
        if existing_source_slug is not None or incoming_source_slug is not None:
            if should_existing_source_win(
                existing_source_slug=existing_source_slug,
                incoming_source_slug=incoming_source_slug,
            ):
                return False
        await executor.execute(
            """
            INSERT INTO event_terminal_state (
                event_id,
                terminal_status,
                finalized_at,
                final_snapshot_id
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (event_id) DO UPDATE SET
                terminal_status = EXCLUDED.terminal_status,
                finalized_at = EXCLUDED.finalized_at,
                final_snapshot_id = EXCLUDED.final_snapshot_id
                -- Task 2 Phase A: locked_at intentionally NOT touched.
                -- Once stamped by FinalSyncPlanner the lock is permanent;
                -- re-finalize events (zombie sweep, late-arrival
                -- corrections) must not silently unfreeze a frozen event.
            """,
            record.event_id,
            record.terminal_status,
            coerce_timestamptz(record.finalized_at),
            record.final_snapshot_id,
        )
        return True

    async def insert_terminal_state_if_missing(
        self,
        executor: SqlExecutor,
        record: EventTerminalStateRecord,
    ) -> None:
        """Record a terminal state only if the event has no prior terminal row.

        Used by the live-zombie sweeper to tag stuck matches without
        overwriting a genuine finalization that may have raced with the sweep.
        """
        await executor.execute(
            """
            INSERT INTO event_terminal_state (
                event_id,
                terminal_status,
                finalized_at,
                final_snapshot_id
            )
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (event_id) DO NOTHING
            """,
            record.event_id,
            record.terminal_status,
            coerce_timestamptz(record.finalized_at),
            record.final_snapshot_id,
        )

    # ------------------------------------------------------------------
    # Task 2 Phase A (2026-05-20) — final-sync lifecycle helpers
    # ------------------------------------------------------------------

    async def is_event_locked(
        self, executor: SqlExecutor, *, event_id: int
    ) -> bool:
        """True iff this event has been frozen (locked_at IS NOT NULL).

        Used by HydrateWorker / LiveWorkerService / read-path as a
        defensive gate to skip work on permanently-frozen events.
        Cheap index-only scan via ``idx_event_terminal_state_locked``.
        """
        value = await executor.fetchval(
            """
            SELECT 1
            FROM event_terminal_state
            WHERE event_id = $1
              AND locked_at IS NOT NULL
            LIMIT 1
            """,
            int(event_id),
        )
        return value is not None

    async def set_event_locked(
        self,
        executor: SqlExecutor,
        *,
        event_id: int,
    ) -> bool:
        """Stamp ``locked_at = now()`` on the event's terminal row.

        Idempotent: if ``locked_at`` is already set, leaves the
        existing timestamp untouched (preserves audit trail). Returns
        True iff a row was updated (i.e. the event had a terminal_state
        row and was not previously locked).

        Called by ``pilot_orchestrator.run_event`` after a successful
        ``scope="final_sync"`` run.
        """
        command_tag = await executor.execute(
            """
            UPDATE event_terminal_state
            SET locked_at = now()
            WHERE event_id = $1
              AND locked_at IS NULL
            """,
            int(event_id),
        )
        return _command_tag_affected(command_tag) > 0

    async def clear_event_lock(
        self,
        executor: SqlExecutor,
        *,
        event_id: int,
    ) -> bool:
        """Operator escape hatch — reset locked_at to NULL.

        Powers the ``unlock-event`` CLI command. After clearing the
        lock the event re-enters the normal flow: FinalSyncPlanner
        will eventually pick it up again (assuming its finalized_at is
        still old enough).
        """
        command_tag = await executor.execute(
            """
            UPDATE event_terminal_state
            SET locked_at = NULL
            WHERE event_id = $1
              AND locked_at IS NOT NULL
            """,
            int(event_id),
        )
        return _command_tag_affected(command_tag) > 0

    # terminal_status values that are NOT a genuine end-of-match and so
    # must never be frozen by the final-sync lock. ``zombie_stale`` is the
    # housekeeping zombie sweeper's tag (services/housekeeping.py:
    # ZOMBIE_TERMINAL_STATUS) and is explicitly NOT end-of-match
    # (CLAUDE rule #9 — the match may still be live). ``not_found`` is the
    # missing-root synthetic tag (pilot_orchestrator.MISSING_ROOT_TERMINAL_STATUS)
    # stamped when the upstream root 404s. Genuine finalize writes one of
    # planner.live.TERMINAL_STATUS_TYPES via _record_terminal_state, so
    # those rows still flow through the queue normally.
    _NON_TERMINAL_LOCK_STATUSES: tuple[str, ...] = ("zombie_stale", "not_found")

    async def pending_lock_event_ids(
        self,
        executor: SqlExecutor,
        *,
        delay_seconds: int,
        limit: int,
    ) -> list[int]:
        """FinalSyncPlanner-side queue feed: events that finalised more
        than ``delay_seconds`` ago and are not yet locked.

        Ordered by ``finalized_at`` ascending so the oldest get a final
        sync first. Index ``idx_event_terminal_state_pending_lock`` is
        partial on ``WHERE locked_at IS NULL`` — scan stays bounded to
        events still in flight.

        Synthetic terminal rows (``zombie_stale``, ``not_found``) are
        excluded: enqueuing them would let the orchestrator freeze a
        still-live match permanently (CLAUDE rule #9). They are filtered
        here AND defended at the stamp site in pilot_orchestrator (the
        terminal-status gate), so a zombie can never be frozen even if a
        row slips through one layer.
        """
        rows = await executor.fetch(
            """
            SELECT event_id
            FROM event_terminal_state
            WHERE locked_at IS NULL
              AND finalized_at <= now() - make_interval(secs => $1)
              AND terminal_status <> ALL($3::text[])
            ORDER BY finalized_at ASC, event_id ASC
            LIMIT $2
            """,
            int(delay_seconds),
            int(limit),
            list(self._NON_TERMINAL_LOCK_STATUSES),
        )
        return [int(row["event_id"]) for row in rows]

    async def mark_event_stale_live_retired(
        self,
        executor: SqlExecutor,
        *,
        event_id: int,
        retired_at: str,
    ) -> None:
        """Move a still-live normalized row out of the live status set.

        The upstream live surface is the source of truth, so this soft-retire
        only applies to rows that are still marked with an in-progress status.
        If a later root/event upsert sees the match again, normal ingestion can
        restore the authoritative status code.
        """

        await executor.execute(
            """
            UPDATE event
            SET status_code = 91,
                updated_at = GREATEST(
                    COALESCE(updated_at, $2::timestamptz),
                    $2::timestamptz
                )
            WHERE id = $1
              AND status_code = ANY($3::int[])
            """,
            event_id,
            coerce_timestamptz(retired_at),
            [6, 7, 8, 9, 30, 31, 32],
        )

    async def fetch_latest_live_state_history(
        self,
        executor: SqlFetchExecutor,
    ) -> tuple[EventLiveStateHistoryRecord, ...]:
        rows = await executor.fetch(
            """
            SELECT DISTINCT ON (event_id)
                event_id,
                observed_status_type,
                poll_profile,
                home_score,
                away_score,
                period_label,
                observed_at
            FROM event_live_state_history
            ORDER BY event_id, observed_at DESC
            """
        )
        return tuple(
            EventLiveStateHistoryRecord(
                event_id=int(row["event_id"]),
                observed_status_type=row["observed_status_type"],
                poll_profile=row["poll_profile"],
                home_score=_maybe_int(row["home_score"]),
                away_score=_maybe_int(row["away_score"]),
                period_label=row["period_label"],
                observed_at=str(row["observed_at"]),
            )
            for row in rows
        )

    async def fetch_terminal_states(
        self,
        executor: SqlFetchExecutor,
    ) -> tuple[EventTerminalStateRecord, ...]:
        rows = await executor.fetch(
            """
            SELECT event_id, terminal_status, finalized_at, final_snapshot_id
            FROM event_terminal_state
            """
        )
        return tuple(
            EventTerminalStateRecord(
                event_id=int(row["event_id"]),
                terminal_status=str(row["terminal_status"]),
                finalized_at=str(row["finalized_at"]),
                final_snapshot_id=_maybe_int(row["final_snapshot_id"]),
            )
            for row in rows
        )


def _maybe_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)
