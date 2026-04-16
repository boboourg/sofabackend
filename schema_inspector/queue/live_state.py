"""Live event polling state and lane indexes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

LIVE_HOT_ZSET = "zset:live:hot"
LIVE_WARM_ZSET = "zset:live:warm"
LIVE_COLD_ZSET = "zset:live:cold"


@dataclass(frozen=True)
class LiveEventState:
    event_id: int
    sport_slug: str
    status_type: str | None
    poll_profile: str | None
    last_seen_at: int | None
    last_ingested_at: int | None
    last_changed_at: int | None
    next_poll_at: int | None
    hot_until: int | None
    home_score: int | None
    away_score: int | None
    version_hint: str | None
    is_finalized: bool


class LiveEventStateStore:
    """Stores live event metadata and indexes by polling lane."""

    def __init__(
        self,
        backend: Any,
        *,
        hot_zset_key: str = LIVE_HOT_ZSET,
        warm_zset_key: str = LIVE_WARM_ZSET,
        cold_zset_key: str = LIVE_COLD_ZSET,
    ) -> None:
        self.backend = backend
        self.hot_zset_key = hot_zset_key
        self.warm_zset_key = warm_zset_key
        self.cold_zset_key = cold_zset_key

    def upsert(self, state: LiveEventState, *, lane: str | None = None) -> None:
        self.backend.hset(
            self._key(state.event_id),
            {
                "sport_slug": state.sport_slug,
                "status_type": state.status_type,
                "poll_profile": state.poll_profile,
                "last_seen_at": state.last_seen_at,
                "last_ingested_at": state.last_ingested_at,
                "last_changed_at": state.last_changed_at,
                "next_poll_at": state.next_poll_at,
                "hot_until": state.hot_until,
                "home_score": state.home_score,
                "away_score": state.away_score,
                "version_hint": state.version_hint,
                "is_finalized": int(state.is_finalized),
            },
        )
        if lane and state.next_poll_at is not None:
            self.move_lane(state.event_id, lane=lane, next_poll_at=state.next_poll_at)

    def move_lane(self, event_id: int, *, lane: str, next_poll_at: int) -> None:
        member = str(event_id)
        self.backend.zrem(self.hot_zset_key, member)
        self.backend.zrem(self.warm_zset_key, member)
        self.backend.zrem(self.cold_zset_key, member)
        self.backend.zadd(self._lane_key(lane), {member: float(next_poll_at)})

    def due_events(self, *, lane: str, now_ms: int) -> tuple[int, ...]:
        return tuple(int(value) for value in self.backend.zrangebyscore(self._lane_key(lane), float("-inf"), float(now_ms)))

    def _lane_key(self, lane: str) -> str:
        normalized = lane.strip().lower()
        if normalized == "hot":
            return self.hot_zset_key
        if normalized == "warm":
            return self.warm_zset_key
        return self.cold_zset_key

    @staticmethod
    def _key(event_id: int) -> str:
        return f"live:event:{event_id}"
