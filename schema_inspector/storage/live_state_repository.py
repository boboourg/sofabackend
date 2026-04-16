"""PostgreSQL repository for durable live-event lifecycle state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


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
            record.observed_at,
        )

    async def upsert_terminal_state(self, executor: SqlExecutor, record: EventTerminalStateRecord) -> None:
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
            record.finalized_at,
            record.final_snapshot_id,
        )
