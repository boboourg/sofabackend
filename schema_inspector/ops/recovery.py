"""Recovery helpers for replay and live-state rebuilds."""

from __future__ import annotations

from dataclasses import dataclass

from ..queue.live_state import LiveEventState


@dataclass(frozen=True)
class LiveStateRecoveryReport:
    restored_hot: int
    restored_warm: int
    restored_cold: int
    restored_terminal: int


def replay_snapshot_ids(orchestrator, snapshot_ids: tuple[int, ...]) -> tuple[object, ...]:
    return tuple(orchestrator.replay_snapshot(int(snapshot_id)) for snapshot_id in snapshot_ids)


async def rebuild_live_state_from_postgres(
    *,
    repository,
    sql_executor,
    live_state_store,
    now_ms: int,
) -> LiveStateRecoveryReport:
    latest_history = await repository.fetch_latest_live_state_history(sql_executor)
    terminal_rows = await repository.fetch_terminal_states(sql_executor)
    terminal_ids = {int(row.event_id) for row in terminal_rows}

    restored_hot = 0
    restored_warm = 0
    restored_cold = 0
    for row in _latest_rows_by_event(latest_history):
        if row.event_id in terminal_ids:
            continue
        lane = _normalized_lane(row.poll_profile)
        if lane == "hot":
            restored_hot += 1
        elif lane == "warm":
            restored_warm += 1
        else:
            restored_cold += 1
        next_poll_at = now_ms
        live_state_store.upsert(
            LiveEventState(
                event_id=row.event_id,
                sport_slug="recovered",
                status_type=row.observed_status_type,
                poll_profile=lane,
                last_seen_at=now_ms,
                last_ingested_at=now_ms,
                last_changed_at=now_ms,
                next_poll_at=next_poll_at,
                hot_until=next_poll_at if lane == "hot" else None,
                home_score=row.home_score,
                away_score=row.away_score,
                version_hint=None,
                is_finalized=False,
            ),
            lane=lane,
        )

    for row in terminal_rows:
        live_state_store.upsert(
            LiveEventState(
                event_id=row.event_id,
                sport_slug="recovered",
                status_type=row.terminal_status,
                poll_profile="terminal",
                last_seen_at=now_ms,
                last_ingested_at=now_ms,
                last_changed_at=now_ms,
                next_poll_at=None,
                hot_until=None,
                home_score=None,
                away_score=None,
                version_hint=None,
                is_finalized=True,
            ),
            lane=None,
        )
        member = str(row.event_id)
        live_state_store.backend.zrem(live_state_store.hot_zset_key, member)
        live_state_store.backend.zrem(live_state_store.warm_zset_key, member)
        live_state_store.backend.zrem(live_state_store.cold_zset_key, member)

    return LiveStateRecoveryReport(
        restored_hot=restored_hot,
        restored_warm=restored_warm,
        restored_cold=restored_cold,
        restored_terminal=len(tuple(terminal_rows)),
    )


def _latest_rows_by_event(rows) -> tuple[object, ...]:
    latest = {}
    for row in rows:
        latest[int(row.event_id)] = row
    return tuple(latest[event_id] for event_id in sorted(latest))


def _normalized_lane(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"hot", "warm"}:
        return normalized
    return "cold"
