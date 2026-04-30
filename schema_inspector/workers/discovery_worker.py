"""Continuous discovery worker that expands planner jobs into hydrate jobs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from ..jobs.types import JOB_DISCOVER_SPORT_SURFACE, JOB_HYDRATE_EVENT_ROOT
from ..live_dispatch_policy import LIVE_TIER_3, resolve_live_dispatch_tier
from ..queue.streams import (
    GROUP_DISCOVERY,
    STREAM_DISCOVERY,
    STREAM_HYDRATE,
    STREAM_LIVE_TIER_1,
    STREAM_LIVE_TIER_2,
    STREAM_LIVE_TIER_3,
    StreamEntry,
)
from ..services.retry_policy import AdmissionDeferredError
from ..services.surface_correction_detector import SurfaceCorrection
from ..services.worker_runtime import WorkerRuntime
from ._stream_jobs import decode_stream_job, encode_stream_job

logger = logging.getLogger(__name__)


class DiscoveryWorker:
    def __init__(
        self,
        *,
        orchestrator,
        queue,
        consumer: str,
        group: str = GROUP_DISCOVERY,
        stream: str = STREAM_DISCOVERY,
        hydrate_stream: str = STREAM_HYDRATE,
        block_ms: int = 5_000,
        timeout_s: float = 20.0,
        delayed_scheduler=None,
        delayed_payload_store=None,
        completion_store=None,
        freshness_policy=None,
        hydrate_backpressure=None,
        defer_on_backpressure: bool = False,
        admission_delay_ms: int = 30_000,
        now_ms_factory=None,
        job_audit_logger=None,
    ) -> None:
        self.orchestrator = orchestrator
        self.queue = queue
        self.consumer = consumer
        self.group = group
        self.stream = stream
        self.hydrate_stream = hydrate_stream
        self.timeout_s = float(timeout_s)
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
        self.freshness_policy = freshness_policy
        self.hydrate_backpressure = hydrate_backpressure
        self.defer_on_backpressure = bool(defer_on_backpressure)
        self.admission_delay_ms = int(admission_delay_ms)
        self.now_ms_factory = now_ms_factory or (lambda: int(datetime.now(timezone.utc).timestamp() * 1000))
        self.runtime = WorkerRuntime(
            name="discovery-worker",
            queue=queue,
            stream=stream,
            group=group,
            consumer=consumer,
            handler=self.handle,
            retry_handler=self.retry_later if delayed_scheduler is not None else None,
            completion_store=completion_store,
            block_ms=block_ms,
            now_ms_factory=self.now_ms_factory,
            job_audit_logger=job_audit_logger,
        )

    async def handle(self, entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        if job.job_type != JOB_DISCOVER_SPORT_SURFACE:
            return "ignored"

        sport_slug = str(job.sport_slug or "").strip().lower()
        if not sport_slug:
            raise RuntimeError("Discovery worker requires sport_slug in stream payload.")

        scope = str(job.scope or "").strip().lower() or "scheduled"
        if scope == "live":
            discovery = await self._discover_live_surface(sport_slug=sport_slug)
            hydration_mode = "live_delta"
        else:
            observed_date = str(job.params.get("date") or _utc_today())
            discovery = await self._discover_scheduled_surface(sport_slug=sport_slug, observed_date=observed_date)
            hydration_mode = "core"

        published = 0
        corrections = {int(item.event_id): item for item in discovery.corrections}
        blocking_reason = None if scope == "live" else self._blocking_reason()
        if blocking_reason is not None:
            non_force_event_count = sum(
                1
                for event_id in discovery.event_ids
                if corrections.get(int(event_id)) is None
            )
            if non_force_event_count and self.defer_on_backpressure:
                logger.info(
                    "Discovery worker deferred hydrate fanout by backpressure: scope=%s sport=%s events=%s reason=%s",
                    scope,
                    sport_slug,
                    non_force_event_count,
                    blocking_reason,
                )
                raise AdmissionDeferredError(
                    f"hydrate admission deferred: {blocking_reason}",
                    delay_ms=self.admission_delay_ms,
                )
        skipped_due_backpressure = 0
        skip_reasons: set[str] = set()
        for surface_event in discovery.events:
            event_id = int(surface_event.event_id)
            correction = corrections.get(int(event_id))
            force_rehydrate = correction is not None and scope != "live"
            if scope == "live":
                resolved_mode = "live_delta"
            else:
                resolved_mode = "full" if force_rehydrate else hydration_mode
            params = {"hydration_mode": resolved_mode}
            if scope == "live" and not force_rehydrate:
                params["live_bootstrap"] = True
                params["live_dispatch_tier"] = resolve_live_dispatch_tier(
                    sport_slug=sport_slug,
                    detail_id=surface_event.detail_id,
                    tournament_tier=surface_event.tournament_tier,
                    tournament_user_count=surface_event.tournament_user_count,
                )
            if correction is not None and force_rehydrate:
                params["force_rehydrate"] = True
                params["correction_reason"] = correction.reason
            target_stream = (
                _stream_for_live_dispatch_tier(str(params.get("live_dispatch_tier") or LIVE_TIER_3))
                if scope == "live"
                else self.hydrate_stream
            )
            event_blocking_reason = (
                self._blocking_reason_for_stream(target_stream)
                if scope == "live"
                else blocking_reason
            )
            if event_blocking_reason is not None and not force_rehydrate:
                skipped_due_backpressure += 1
                skip_reasons.add(str(event_blocking_reason))
                continue
            if self.freshness_policy is not None and not force_rehydrate:
                if not self.freshness_policy.claim_event_hydration(
                    event_id=int(event_id),
                    hydration_mode=resolved_mode,
                    force_rehydrate=False,
                    now_ms=int(self.now_ms_factory()),
                ):
                    continue
            hydrate_job = job.spawn_child(
                job_type=JOB_HYDRATE_EVENT_ROOT,
                entity_type="event",
                entity_id=int(event_id),
                scope=scope,
                params=params,
                priority=job.priority,
            )
            self.queue.publish(target_stream, encode_stream_job(hydrate_job))
            published += 1
        if skipped_due_backpressure:
            logger.info(
                "Discovery worker skipped hydrate fanout by backpressure: scope=%s sport=%s skipped=%s reason=%s",
                scope,
                sport_slug,
                skipped_due_backpressure,
                "; ".join(sorted(skip_reasons)) if skip_reasons else blocking_reason,
            )
        return f"published:{published}"

    async def retry_later(self, entry: StreamEntry, exc: Exception, *, delay_ms: int) -> str:
        del exc
        if self.delayed_scheduler is None:
            return "ignored"
        job = decode_stream_job(entry)
        if self.delayed_payload_store is not None:
            self.delayed_payload_store.save_entry(entry)
        self.delayed_scheduler.schedule(
            job.job_id,
            run_at_epoch_ms=int(self.now_ms_factory()) + int(delay_ms),
        )
        return "requeued"

    async def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        await self.runtime.run_forever(install_signal_handlers=install_signal_handlers)

    def request_shutdown(self) -> None:
        self.runtime.request_shutdown()

    async def _discover_live_surface(self, *, sport_slug: str) -> "_SurfaceDiscovery":
        resolver = getattr(self.orchestrator, "discover_live_events", None)
        if callable(resolver):
            result = await resolver(sport_slug=sport_slug, timeout=self.timeout_s)
        else:
            result = await self.orchestrator.discover_live_event_ids(
                sport_slug=sport_slug,
                timeout=self.timeout_s,
            )
        return _normalize_surface_result(result)

    async def _discover_scheduled_surface(self, *, sport_slug: str, observed_date: str) -> "_SurfaceDiscovery":
        resolver = getattr(self.orchestrator, "discover_scheduled_events", None)
        if callable(resolver):
            result = await resolver(
                sport_slug=sport_slug,
                date=observed_date,
                timeout=self.timeout_s,
            )
        else:
            result = await self.orchestrator.discover_scheduled_event_ids(
                sport_slug=sport_slug,
                date=observed_date,
                timeout=self.timeout_s,
            )
        return _normalize_surface_result(result)

    def _blocking_reason(self) -> str | None:
        backpressure = self.hydrate_backpressure
        if backpressure is None:
            return None
        blocking_reason = getattr(backpressure, "blocking_reason", None)
        if not callable(blocking_reason):
            return None
        return blocking_reason()

    def _blocking_reason_for_stream(self, stream: str) -> str | None:
        backpressure = self.hydrate_backpressure
        if backpressure is None:
            return None

        targeted_reason = getattr(backpressure, "blocking_reason_for_stream", None)
        if callable(targeted_reason):
            return targeted_reason(stream)

        targeted_reason = getattr(backpressure, "blocking_reason_for", None)
        if callable(targeted_reason):
            try:
                return targeted_reason(stream=stream)
            except TypeError:
                pass

        limits = tuple(getattr(backpressure, "limits", ()) or ())
        queue = getattr(backpressure, "queue", None)
        group_info = getattr(queue, "group_info", None)
        if callable(group_info):
            for limit in limits:
                limit_stream = getattr(limit, "stream", None)
                if str(limit_stream or "") != str(stream):
                    continue
                info = group_info(limit.stream, limit.group)
                if info is None:
                    continue
                lag = getattr(info, "lag", None)
                max_lag = getattr(limit, "max_lag", None)
                if lag is None or max_lag is None:
                    continue
                if int(lag) > int(max_lag):
                    return f"{limit.stream}:{limit.group}:lag={int(lag)}>{int(max_lag)}"
            return None

        return self._blocking_reason()


@dataclass(frozen=True)
class _SurfaceEvent:
    event_id: int
    detail_id: int | None = None
    unique_tournament_id: int | None = None
    tournament_tier: int | None = None
    tournament_user_count: int | None = None


class _SurfaceDiscovery:
    def __init__(self, *, events: tuple[_SurfaceEvent, ...], corrections: tuple[SurfaceCorrection, ...]) -> None:
        self.events = tuple(events)
        self.event_ids = tuple(int(item.event_id) for item in self.events)
        self.corrections = tuple(corrections)


def _normalize_surface_result(value: Any) -> _SurfaceDiscovery:
    if isinstance(value, (tuple, list)):
        return _SurfaceDiscovery(
            events=tuple(_SurfaceEvent(event_id=int(item)) for item in value),
            corrections=(),
        )
    parsed = getattr(value, "parsed", None)
    if parsed is not None and hasattr(parsed, "events"):
        tournaments_by_id = {
            int(getattr(item, "id")): item
            for item in getattr(parsed, "unique_tournaments", ())
            if getattr(item, "id", None) is not None
        }
        events = []
        for item in getattr(parsed, "events", ()):
            if getattr(item, "id", None) is None:
                continue
            unique_tournament_id = _as_int(getattr(item, "unique_tournament_id", None))
            tournament = tournaments_by_id.get(int(unique_tournament_id)) if unique_tournament_id is not None else None
            events.append(
                _SurfaceEvent(
                    event_id=int(getattr(item, "id")),
                    detail_id=_as_int(getattr(item, "detail_id", None)),
                    unique_tournament_id=unique_tournament_id,
                    tournament_tier=_as_int(getattr(tournament, "tier", None)) if tournament is not None else None,
                    tournament_user_count=(
                        _as_int(getattr(tournament, "user_count", None)) if tournament is not None else None
                    ),
                )
            )
        corrections = tuple(getattr(value, "corrections", ()) or ())
        return _SurfaceDiscovery(events=tuple(events), corrections=corrections)
    raise TypeError(f"Unsupported discovery result: {value!r}")


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _stream_for_live_dispatch_tier(dispatch_tier: str) -> str:
    normalized = str(dispatch_tier or "").strip().lower()
    if normalized == "tier_1":
        return STREAM_LIVE_TIER_1
    if normalized == "tier_2":
        return STREAM_LIVE_TIER_2
    return STREAM_LIVE_TIER_3


def _as_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
