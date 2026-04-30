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
