"""Continuous planner loop that publishes jobs into Redis Streams."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..jobs.envelope import JobEnvelope
from ..jobs.types import (
    JOB_DISCOVER_SPORT_SURFACE,
    JOB_ENRICH_TOURNAMENT_ARCHIVE,
    JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH,
    JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH,
    JOB_REFRESH_LIVE_EVENT,
    JOB_SYNC_TOURNAMENT_ARCHIVE,
)
from ..queue.streams import (
    STREAM_DISCOVERY,
    STREAM_HISTORICAL_DISCOVERY,
    STREAM_HISTORICAL_ENRICHMENT,
    STREAM_HISTORICAL_HYDRATE,
    STREAM_HISTORICAL_TOURNAMENT,
    STREAM_HYDRATE,
    STREAM_LIVE_HOT,
    STREAM_LIVE_WARM,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ScheduledPlanningTarget:
    sport_slug: str
    interval_ms: int
    priority: int = 50
    date_factory: Callable[[int], str] | None = None


class PlannerDaemon:
    """Publishes scheduled, delayed, and live refresh jobs without performing hydration."""

    def __init__(
        self,
        *,
        queue: Any,
        delayed_scheduler: Any,
        delayed_job_loader,
        live_state_store: Any,
        scheduled_targets: tuple[ScheduledPlanningTarget, ...] = (),
        now_ms_factory=None,
        loop_interval_s: float = 5.0,
        live_dispatch_lease_ms: int = 90_000,
        scheduled_backpressure=None,
        live_backpressure=None,
    ) -> None:
        self.queue = queue
        self.delayed_scheduler = delayed_scheduler
        self.delayed_job_loader = delayed_job_loader
        self.live_state_store = live_state_store
        self.scheduled_targets = tuple(scheduled_targets)
        self.now_ms_factory = now_ms_factory or _default_now_ms
        self.loop_interval_s = float(loop_interval_s)
        self.live_dispatch_lease_ms = max(int(live_dispatch_lease_ms), 1)
        self.scheduled_backpressure = scheduled_backpressure
        self.live_backpressure = live_backpressure
        self.shutdown_requested = False
        self._last_planned_at_ms: dict[str, int] = {}

    def request_shutdown(self) -> None:
        self.shutdown_requested = True

    async def run_forever(self) -> None:
        while not self.shutdown_requested:
            await self.tick()
            if self.shutdown_requested:
                break
            await asyncio.sleep(self.loop_interval_s)

    async def tick(self, *, now_ms: int | None = None) -> None:
        observed_now = int(now_ms if now_ms is not None else self.now_ms_factory())
        scheduled_reason = _blocking_reason(self.scheduled_backpressure)
        if scheduled_reason is None:
            await self._publish_scheduled_targets(observed_now)
        else:
            logger.info("Planner paused scheduled planning by backpressure: %s", scheduled_reason)
        await self._publish_due_delayed_jobs(observed_now)
        live_reason = _blocking_reason(self.live_backpressure)
        if live_reason is None:
            await self._publish_live_refreshes(observed_now)
        else:
            logger.info("Planner paused live refresh planning by backpressure: %s", live_reason)

    async def _publish_scheduled_targets(self, now_ms: int) -> None:
        for target in self.scheduled_targets:
            if not self._scheduled_target_due(target, now_ms):
                continue
            date_value = target.date_factory(now_ms) if target.date_factory is not None else _date_from_epoch_ms(now_ms)
            job = JobEnvelope.create(
                job_type=JOB_DISCOVER_SPORT_SURFACE,
                sport_slug=target.sport_slug,
                entity_type="sport",
                entity_id=None,
                scope="scheduled",
                params={"date": date_value},
                priority=target.priority,
                trace_id=None,
            )
            self._publish_job(STREAM_DISCOVERY, job)
            self._last_planned_at_ms[target.sport_slug] = now_ms

    async def _publish_due_delayed_jobs(self, now_ms: int) -> None:
        for delayed_job in self.delayed_scheduler.pop_due(now_epoch_ms=now_ms):
            job = await _maybe_await(self.delayed_job_loader(delayed_job.job_id))
            if job is None:
                continue
            self._publish_job(_stream_for_job(job), job)

    async def _publish_live_refreshes(self, now_ms: int) -> None:
        for lane in ("hot", "warm"):
            stream = STREAM_LIVE_HOT if lane == "hot" else STREAM_LIVE_WARM
            priority = 0 if lane == "hot" else 1
            for event_id in self.live_state_store.due_events(lane=lane, now_ms=now_ms):
                state = self.live_state_store.fetch(int(event_id))
                if state is None or state.is_finalized:
                    continue
                claim_dispatch = getattr(self.live_state_store, "claim_dispatch", None)
                if callable(claim_dispatch) and not claim_dispatch(
                    int(event_id),
                    now_ms=now_ms,
                    lease_ms=self.live_dispatch_lease_ms,
                ):
                    continue
                job = JobEnvelope.create(
                    job_type=JOB_REFRESH_LIVE_EVENT,
                    sport_slug=state.sport_slug,
                    entity_type="event",
                    entity_id=int(event_id),
                    scope=lane,
                    params={
                        "status_type": state.status_type,
                        "next_poll_at": state.next_poll_at,
                    },
                    priority=priority,
                    trace_id=None,
                )
                try:
                    self._publish_job(stream, job)
                except Exception:
                    clear_claim = getattr(self.live_state_store, "clear_dispatch_claim", None)
                    if callable(clear_claim):
                        clear_claim(int(event_id))
                    raise

    def _scheduled_target_due(self, target: ScheduledPlanningTarget, now_ms: int) -> bool:
        last_planned = self._last_planned_at_ms.get(target.sport_slug)
        if last_planned is None:
            return True
        return (now_ms - last_planned) >= int(target.interval_ms)

    def _publish_job(self, stream: str, job: JobEnvelope) -> None:
        self.queue.publish(stream, _job_to_stream_payload(job))


def _stream_for_job(job: JobEnvelope) -> str:
    if job.job_type == JOB_REFRESH_LIVE_EVENT:
        if str(job.scope or "").strip().lower() == "warm":
            return STREAM_LIVE_WARM
        return STREAM_LIVE_HOT
    if _is_historical_scope(job.scope):
        if job.job_type == JOB_SYNC_TOURNAMENT_ARCHIVE:
            return STREAM_HISTORICAL_TOURNAMENT
        if job.job_type in {
            JOB_ENRICH_TOURNAMENT_ARCHIVE,
            JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH,
            JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH,
        }:
            return STREAM_HISTORICAL_ENRICHMENT
        if job.job_type.startswith("discover_") or job.job_type.startswith("sync_"):
            return STREAM_HISTORICAL_DISCOVERY
        return STREAM_HISTORICAL_HYDRATE
    if job.job_type.startswith("discover_") or job.job_type.startswith("sync_"):
        return STREAM_DISCOVERY
    return STREAM_HYDRATE


def _is_historical_scope(scope: str | None) -> bool:
    normalized = str(scope or "").strip().lower()
    return normalized == "historical" or normalized.startswith("historical_")


def _job_to_stream_payload(job: JobEnvelope) -> dict[str, object]:
    return {
        "job_id": job.job_id,
        "job_type": job.job_type,
        "sport_slug": job.sport_slug or "",
        "entity_type": job.entity_type or "",
        "entity_id": job.entity_id if job.entity_id is not None else "",
        "scope": job.scope or "",
        "params_json": json.dumps(job.params, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        "priority": job.priority,
        "scheduled_at": job.scheduled_at,
        "attempt": job.attempt,
        "parent_job_id": job.parent_job_id or "",
        "trace_id": job.trace_id or "",
        "capability_hint": job.capability_hint or "",
        "idempotency_key": job.idempotency_key,
    }


async def _maybe_await(value: object) -> object:
    if asyncio.iscoroutine(value) or isinstance(value, asyncio.Future):
        return await value
    return value


def _default_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _date_from_epoch_ms(now_ms: int) -> str:
    return datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).date().isoformat()


def _blocking_reason(backpressure: object | None) -> str | None:
    if backpressure is None:
        return None
    blocking_reason = getattr(backpressure, "blocking_reason", None)
    if not callable(blocking_reason):
        return None
    return blocking_reason()
