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
    def __init__(self, *, now_ms_factory=None, tier_override_registry=None) -> None:
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        # A3 Phase 0 (2026-05-16): optional registry for per-UT live-tier
        # overrides. When provided, ``track_event`` consults it before the
        # detail_id/user_count heuristics in resolve_live_dispatch_tier.
        # None keeps backwards-compat for unit tests and callers that have
        # not been wired through the new path yet.
        self.tier_override_registry = tier_override_registry

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
        unique_tournament_id: int | None = None,
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
            unique_tournament_id=unique_tournament_id,
            tier_override_registry=self.tier_override_registry,
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
            # F-8 Fix B (defense-in-depth, complements Phase 1 + 1.5
            # SQL guards): if the live state store already has the event
            # marked terminal, do NOT re-liven it. Without this guard,
            # a stale upstream payload returning "inprogress" (Sofascore
            # CDN flap) would call track_event after the match was
            # legitimately finalized, putting it back into hot/warm
            # zsets and burning tier_1 polling resources on an ended
            # match. The Phase 1 + 1.5 SQL guards already prevent the
            # event row from regressing in DB; this guard prevents the
            # same regression in Redis live state.
            #
            # Defensive on three levels:
            #   1. fetch attribute may be absent (older test fakes /
            #      alternate stores) — fall through.
            #   2. fetch may raise (e.g., backend lacking hgetall in
            #      a fake) — fall through, guard is best-effort.
            #   3. existing may be None (no prior state — first track
            #      for the event) — proceed with normal upsert.
            fetch = getattr(live_state_store, "fetch", None)
            if callable(fetch):
                try:
                    existing = fetch(event_id)
                except Exception:
                    existing = None
                if existing is not None and existing.is_finalized:
                    return LiveTrackResult(
                        decision=decision,
                        next_poll_at=None,
                        stream=None,
                        job=None,
                    )

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
                clear_claim(event_id, tier=dispatch_tier)

        stream = _stream_for_lane(decision.lane, dispatch_tier=dispatch_tier)
        del stream_queue, trace_id
        job = None

        return LiveTrackResult(
            decision=decision,
            next_poll_at=next_poll_at,
            stream=stream,
            job=job,
        )

    def reschedule_after_transient_failure(
        self,
        *,
        sport_slug: str,
        event_id: int,
        live_state_store=None,
        min_backoff_seconds: int = 15,
    ) -> int | None:
        """Push ``next_poll_at`` forward after a *transient* (retryable) live
        refresh failure so the event reschedules on a sane cadence instead of
        freezing.

        Why this exists (2026-05-29 live audit): on a retryable root
        ``/event/{id}`` fetch failure (network timeout / 403 / 429),
        ``run_event`` raises ``RetryableJobError`` *before* it reaches
        ``track_event`` — and ``track_event`` is the ONLY place ``next_poll_at``
        (the hot/warm/cold zset score) is advanced. So a persistently
        timing-out event keeps its old, already-due score: the planner
        re-dispatches it every single tick (a coalesced refresh storm), and
        ``oldest_hot_score_age_seconds`` (= ``now - min(zset score)``) grows
        unbounded and breaches the 900 s SLO — even though the event is
        genuinely live and merely hitting a slow proxy.

        This helper re-scores the event ``min_backoff_seconds`` (floored up to
        the event's tier poll cadence) into the future, in its CURRENT lane, so
        the planner backs off to a sane retry cadence, the freshness metric
        reflects the real next-retry time, and the event self-heals once the
        upstream/proxy recovers.

        It deliberately does NOT touch the data-freshness fields
        (``last_changed_at`` / ``last_ingested_at``): no data was fetched, so
        the per-event hash must keep telling the truth that the payload is
        stale. Only the schedule (the zset score, which is what the planner's
        ``due_events`` and the freshness metric both read) moves.

        Best-effort and fully defensive — this runs on the failure path of the
        single hydration point for the entire live fleet, so it must NEVER
        mask the original error: any missing attribute / raise / absent state
        is a silent no-op. Terminal (``is_finalized``) events are left alone so
        a finished match is never re-livened. Returns the new ``next_poll_at``,
        or ``None`` when nothing was rescheduled.
        """
        if live_state_store is None:
            return None
        fetch = getattr(live_state_store, "fetch", None)
        move_lane = getattr(live_state_store, "move_lane", None)
        if not callable(fetch) or not callable(move_lane):
            return None
        try:
            existing = fetch(event_id)
        except Exception:
            return None
        if existing is None or existing.is_finalized:
            return None
        lane = str(existing.poll_profile or "").strip().lower()
        if lane not in {"hot", "warm", "cold"}:
            return None
        backoff_seconds = poll_seconds_for_live_dispatch_tier(
            existing.dispatch_tier,
            default_seconds=min_backoff_seconds,
        )
        backoff_seconds = max(
            int(backoff_seconds or min_backoff_seconds), int(min_backoff_seconds)
        )
        next_poll_at = int(self.now_ms_factory()) + backoff_seconds * 1000
        try:
            move_lane(event_id, lane=lane, next_poll_at=next_poll_at)
        except Exception:
            return None
        # Drop the dispatch claim so the planner can re-dispatch when the
        # backed-off score comes due (track_event clears it the same way on
        # the success path). Without this the ~90 s claim lease would block
        # the retry until it expired even after next_poll_at fell due.
        clear_claim = getattr(live_state_store, "clear_dispatch_claim", None)
        if callable(clear_claim):
            try:
                clear_claim(event_id, tier=existing.dispatch_tier)
            except Exception:
                pass
        del sport_slug  # accepted for call-site symmetry / future logging
        return next_poll_at

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
