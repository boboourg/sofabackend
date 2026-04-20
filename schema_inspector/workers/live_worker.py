"""Live tracking helpers for queueing refresh jobs and terminal finalization."""

from __future__ import annotations

import time
from dataclasses import dataclass

from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_REFRESH_LIVE_EVENT
from ..queue.live_state import LiveEventState
from ..queue.streams import STREAM_LIVE_HOT, STREAM_LIVE_WARM
from ..planner.live import classify_live_polling


@dataclass(frozen=True)
class LiveTrackResult:
    decision: object
    next_poll_at: int | None
    stream: str | None
    job: JobEnvelope | None


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
        live_state_store=None,
        stream_queue=None,
    ) -> LiveTrackResult:
        decision = self.handle(
            status_type=status_type,
            minutes_to_start=minutes_to_start,
            sport_slug=sport_slug,
        )
        now_ms = int(self.now_ms_factory())
        next_poll_at = now_ms + (decision.next_poll_seconds * 1000) if decision.next_poll_seconds is not None else None

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
                ),
                lane=decision.lane if decision.lane in {"hot", "warm", "cold"} else None,
            )

        stream = _stream_for_lane(decision.lane)
        job = None
        if stream is not None and next_poll_at is not None and stream_queue is not None:
            job = JobEnvelope.create(
                job_type=JOB_REFRESH_LIVE_EVENT,
                sport_slug=sport_slug,
                entity_type="event",
                entity_id=event_id,
                scope=decision.lane,
                params={"status_type": status_type, "next_poll_at": next_poll_at},
                priority=0 if decision.lane == "hot" else 1,
                trace_id=trace_id,
            )
            stream_queue.publish(
                stream,
                {
                    "job_id": job.job_id,
                    "job_type": job.job_type,
                    "sport_slug": sport_slug,
                    "event_id": event_id,
                    "lane": decision.lane,
                    "next_poll_at": next_poll_at,
                    "trace_id": trace_id or "",
                },
            )

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
            ),
            lane=None,
        )
        member = str(event_id)
        live_state_store.backend.zrem(live_state_store.hot_zset_key, member)
        live_state_store.backend.zrem(live_state_store.warm_zset_key, member)
        live_state_store.backend.zrem(live_state_store.cold_zset_key, member)


def _stream_for_lane(lane: str | None) -> str | None:
    normalized = str(lane or "").strip().lower()
    if normalized == "hot":
        return STREAM_LIVE_HOT
    if normalized == "warm":
        return STREAM_LIVE_WARM
    return None
