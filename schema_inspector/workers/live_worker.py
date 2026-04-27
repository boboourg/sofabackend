"""Live tracking helpers for live polling state and terminal finalization."""

from __future__ import annotations

import time
from dataclasses import dataclass

from ..live_dispatch_policy import poll_seconds_for_live_dispatch_tier, resolve_live_dispatch_tier
from ..queue.live_state import LiveEventState
from ..queue.streams import STREAM_LIVE_TIER_1, STREAM_LIVE_TIER_2, STREAM_LIVE_TIER_3, STREAM_LIVE_WARM
from ..planner.live import ACTIVE_LIVE_STATUS_TYPES, classify_live_polling


@dataclass(frozen=True)
class LiveTrackResult:
    decision: object
    next_poll_at: int | None
    stream: str | None
    job: object | None


class LiveWorker:
    def __init__(self, *, now_ms_factory=None) -> None:
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))

    def handle(
        self,
        *,
        status_type: str | None,
        minutes_to_start: int | None,
        sport_slug: str | None = None,
    ):
        return classify_live_polling(
            status_type=status_type,
            minutes_to_start=minutes_to_start,
            sport_slug=sport_slug,
        )

    def track_event(
        self,
        *,
        sport_slug: str,
        event_id: int,
        status_type: str | None,
        minutes_to_start: int | None,
        trace_id: str | None,
        detail_id: int | None = None,
        tournament_tier: int | None = None,
        tournament_user_count: int | None = None,
        live_state_store=None,
        stream_queue=None,
    ) -> LiveTrackResult:
        decision = self.handle(
            status_type=status_type,
            minutes_to_start=minutes_to_start,
            sport_slug=sport_slug,
        )
        now_ms = int(self.now_ms_factory())
        dispatch_tier = resolve_live_dispatch_tier(
            sport_slug=sport_slug,
            detail_id=detail_id,
            tournament_tier=tournament_tier,
            tournament_user_count=tournament_user_count,
        )
        next_poll_seconds = decision.next_poll_seconds
        normalized_status = str(status_type or "").strip().lower()
        if normalized_status in ACTIVE_LIVE_STATUS_TYPES:
            next_poll_seconds = poll_seconds_for_live_dispatch_tier(
                dispatch_tier,
                default_seconds=decision.next_poll_seconds,
            )
        next_poll_at = now_ms + (next_poll_seconds * 1000) if next_poll_seconds is not None else None

        if live_state_store is not None:
            live_state_store.upsert(
                LiveEventState(
                    event_id=event_id,
                    sport_slug=sport_slug,
                    status_type=status_type,
                    poll_profile=decision.lane,
                    last_seen_at=now_ms,
                    last_ingested_at=now_ms,
                    last_changed_at=now_ms,
                    next_poll_at=next_poll_at,
                    hot_until=next_poll_at if decision.lane == "hot" else None,
                    home_score=None,
                    away_score=None,
                    version_hint=None,
                    is_finalized=False,
                    dispatch_tier=dispatch_tier,
                ),
                lane=decision.lane if decision.lane in {"hot", "warm", "cold"} else None,
            )
            clear_claim = getattr(live_state_store, "clear_dispatch_claim", None)
            if callable(clear_claim):
                clear_claim(event_id)

        stream = _stream_for_lane(decision.lane, dispatch_tier=dispatch_tier)
        del stream_queue, trace_id
        job = None

        return LiveTrackResult(
            decision=decision,
            next_poll_at=next_poll_at,
            stream=stream,
            job=job,
        )

    def finalize_event(
        self,
        *,
        sport_slug: str,
        event_id: int,
        status_type: str | None,
        live_state_store=None,
    ) -> None:
        if live_state_store is None:
            return

        now_ms = int(self.now_ms_factory())
        live_state_store.upsert(
            LiveEventState(
                event_id=event_id,
                sport_slug=sport_slug,
                status_type=status_type,
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
                dispatch_tier=None,
            ),
            lane=None,
        )
        member = str(event_id)
        live_state_store.backend.zrem(live_state_store.hot_zset_key, member)
        live_state_store.backend.zrem(live_state_store.warm_zset_key, member)
        live_state_store.backend.zrem(live_state_store.cold_zset_key, member)
        clear_claim = getattr(live_state_store, "clear_dispatch_claim", None)
        if callable(clear_claim):
            clear_claim(event_id)


def _stream_for_lane(lane: str | None, *, dispatch_tier: str | None) -> str | None:
    normalized = str(lane or "").strip().lower()
    if normalized == "hot":
        if dispatch_tier == "tier_1":
            return STREAM_LIVE_TIER_1
        if dispatch_tier == "tier_2":
            return STREAM_LIVE_TIER_2
        return STREAM_LIVE_TIER_3
    if normalized == "warm":
        return STREAM_LIVE_WARM
    return None
