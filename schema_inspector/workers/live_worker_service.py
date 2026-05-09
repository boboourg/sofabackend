"""Continuous live refresh worker services for hot and warm lanes."""

from __future__ import annotations

import json
import logging
import os
import time

from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_REFRESH_LIVE_EVENT, JOB_REFRESH_LIVE_EVENT_DETAILS
from ..live_hydration_mode import resolve_live_hydration_mode
from ..queue.streams import (
    GROUP_LIVE_HOT,
    GROUP_LIVE_TIER_1,
    GROUP_LIVE_TIER_2,
    GROUP_LIVE_TIER_3,
    GROUP_LIVE_WARM,
    STREAM_LIVE_DETAILS,
    STREAM_LIVE_HOT,
    STREAM_LIVE_TIER_1,
    STREAM_LIVE_TIER_2,
    STREAM_LIVE_TIER_3,
    STREAM_LIVE_WARM,
    StreamEntry,
)
from ..services.worker_runtime import BatchDispatchPlan, WorkerRuntime, resolve_worker_max_concurrency
from ._stream_jobs import decode_stream_job

_HOT_PREFETCH_COUNT = 25
logger = logging.getLogger(__name__)


class LiveWorkerService:
    def __init__(
        self,
        *,
        orchestrator,
        delayed_scheduler,
        delayed_payload_store=None,
        completion_store=None,
        queue,
        lane: str,
        consumer: str,
        block_ms: int = 5_000,
        now_ms_factory=None,
        default_sport_slug: str = "football",
        job_audit_logger=None,
        max_concurrency: int | None = None,
        in_flight_store=None,
        details_throttle=None,
        details_backpressure_limit: int | None = None,
    ) -> None:
        normalized_lane = str(lane).strip().lower()
        if normalized_lane not in {"hot", "warm", "tier_1", "tier_2", "tier_3"}:
            raise ValueError(f"Unsupported live lane: {lane!r}")

        self.orchestrator = orchestrator
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
        self.in_flight_store = in_flight_store
        self.queue = queue
        # P0(a) split-details rollout: per-event rate-limiter + optional
        # backpressure on the details stream. ``details_throttle`` is the
        # ``LiveDetailsThrottle`` instance (or None for tests / legacy).
        # ``details_backpressure_limit`` (env
        # ``LIVE_DETAILS_STREAM_BACKPRESSURE_LIMIT``) caps the
        # ``stream:etl:live_details`` length; above the cap, live-tier
        # workers skip the details enqueue and log
        # "details_backpressure_skip" rather than letting the details
        # backlog grow unbounded.
        self.details_throttle = details_throttle
        env_backpressure = _env_optional_positive_int("LIVE_DETAILS_STREAM_BACKPRESSURE_LIMIT")
        self.details_backpressure_limit = (
            details_backpressure_limit
            if details_backpressure_limit is not None
            else env_backpressure
        )
        self.lane = normalized_lane
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.default_sport_slug = default_sport_slug
        stream, group = _lane_stream_group(normalized_lane)
        hot_like_lane = normalized_lane in {"hot", "tier_1", "tier_2", "tier_3"}
        resolved_max_concurrency = resolve_worker_max_concurrency(
            default=1 if hot_like_lane else 2,
            explicit=max_concurrency,
            env_names=_lane_concurrency_env_names(normalized_lane),
        )
        batch_preprocessor = _coalesce_live_refresh_batch if hot_like_lane else None
        prefetch_count = _HOT_PREFETCH_COUNT if hot_like_lane else resolved_max_concurrency
        self.runtime = WorkerRuntime(
            name=f"live-{normalized_lane}-worker",
            queue=queue,
            stream=stream,
            group=group,
            consumer=consumer,
            handler=self.handle,
            retry_handler=self.retry_later,
            completion_store=completion_store,
            block_ms=block_ms,
            now_ms_factory=self.now_ms_factory,
            job_audit_logger=job_audit_logger,
            max_concurrency=resolved_max_concurrency,
            prefetch_count=prefetch_count,
            batch_preprocessor=batch_preprocessor,
        )

    async def handle(self, entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        if job.entity_id is None:
            raise RuntimeError("Live worker requires event entity_id in stream payload.")
        event_id = int(job.entity_id)
        lock_owner: str | None = None
        if self.in_flight_store is not None:
            lock_owner = f"{self.runtime.consumer}:{entry.message_id}:{job.job_id}"
            if not self.in_flight_store.claim(event_id=event_id, owner=lock_owner):
                logger.info(
                    "Coalesced in-flight live refresh: lane=%s event_id=%s stream=%s message_id=%s consumer=%s",
                    self.lane,
                    event_id,
                    entry.stream,
                    entry.message_id,
                    self.runtime.consumer,
                )
                return "coalesced_inflight"
        sport_slug = job.sport_slug or self.default_sport_slug
        report = None
        try:
            report = await self.orchestrator.run_event(
                event_id=event_id,
                sport_slug=sport_slug,
                hydration_mode=resolve_live_hydration_mode(
                    requested_mode=job.params.get("hydration_mode") or "live_delta",
                    sport_slug=sport_slug,
                    scope=job.scope,
                ),
            )
        finally:
            # Release critical lock IMMEDIATELY after run_event returns.
            # Under split-details (P0(a)) the orchestrator returns after
            # ROOT + edges only — releasing now lets the next root poll
            # for this event proceed without waiting for details fanout.
            if self.in_flight_store is not None and lock_owner is not None:
                self.in_flight_store.release(event_id=event_id, owner=lock_owner)

        # P0(a): if split-details fanout is enabled, enqueue a standalone
        # ``refresh_live_event_details`` job onto ``stream:etl:live_details``.
        # Skipped when:
        #   - report missing (test/early-return paths) or details_pending=False
        #   - event finalized this tick (final sweep already covered details)
        #   - throttle says we just enqueued for this event (rate-limit window)
        #   - details stream length > backpressure cap
        # Enqueue failure is logged at WARNING but the parent
        # refresh_live_event job still returns "completed" — the details
        # path must not feed back into root retry budget.
        if (
            report is not None
            and getattr(report, "details_pending", False)
            and not getattr(report, "finalized", False)
        ):
            self._maybe_enqueue_details(event_id=event_id, sport_slug=sport_slug, job=job, report=report)
        return "completed"

    def _maybe_enqueue_details(self, *, event_id: int, sport_slug: str, job, report) -> None:
        if self.details_throttle is not None:
            try:
                allowed = self.details_throttle.should_enqueue(event_id=event_id)
            except Exception as exc:
                logger.warning(
                    "Details throttle check failed for event_id=%s: %s — skipping enqueue (fail-closed to avoid duplicate flooding)",
                    event_id,
                    exc,
                )
                return
            if not allowed:
                logger.debug(
                    "Skipping details enqueue (throttled): event_id=%s",
                    event_id,
                )
                return
        if self.details_backpressure_limit is not None:
            try:
                stream_len = self._stream_length(STREAM_LIVE_DETAILS)
            except Exception as exc:
                logger.warning(
                    "Details backpressure XLEN check failed: %s — skipping (fail-closed)",
                    exc,
                )
                return
            if stream_len is not None and stream_len >= self.details_backpressure_limit:
                logger.info(
                    "details_backpressure_skip: event_id=%s stream_len=%s limit=%s",
                    event_id,
                    stream_len,
                    self.details_backpressure_limit,
                )
                return
        details_context = getattr(report, "details_context", None) or {}
        details_job = JobEnvelope.create(
            job_type=JOB_REFRESH_LIVE_EVENT_DETAILS,
            sport_slug=sport_slug,
            entity_type="event",
            entity_id=event_id,
            scope="details",
            params={
                "details_context": details_context,
                "live_dispatch_tier": job.params.get("live_dispatch_tier"),
                "parent_job_id": job.job_id,
            },
            priority=2,
            trace_id=job.trace_id,
        )
        try:
            self.queue.publish(STREAM_LIVE_DETAILS, _job_to_stream_payload(details_job))
        except Exception as exc:
            logger.warning(
                "Failed to enqueue refresh_live_event_details for event_id=%s: %s — root job still completes",
                event_id,
                exc,
            )

    def _stream_length(self, stream_name: str) -> int | None:
        backend = getattr(self.queue, "backend", None) or getattr(self.queue, "_backend", None)
        if backend is None:
            return None
        xlen = getattr(backend, "xlen", None)
        if not callable(xlen):
            return None
        try:
            value = xlen(stream_name)
        except Exception:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    async def retry_later(self, entry: StreamEntry, exc: Exception, *, delay_ms: int) -> str:
        del exc
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


def _coalesce_live_refresh_batch(entries: tuple[StreamEntry, ...]) -> BatchDispatchPlan:
    decoded_jobs = tuple(decode_stream_job(entry) for entry in entries)
    latest_index_by_event_id: dict[int, int] = {}
    for index, job in enumerate(decoded_jobs):
        if job.job_type != JOB_REFRESH_LIVE_EVENT or job.entity_id is None:
            continue
        latest_index_by_event_id[int(job.entity_id)] = index

    kept_entries: list[StreamEntry] = []
    stale_entries: list[StreamEntry] = []
    coalesced_counts: dict[str, int] = {}
    for index, (entry, job) in enumerate(zip(entries, decoded_jobs, strict=False)):
        event_id = job.entity_id
        if (
            job.job_type == JOB_REFRESH_LIVE_EVENT
            and event_id is not None
            and latest_index_by_event_id.get(int(event_id)) != index
        ):
            stale_entries.append(entry)
            event_key = str(int(event_id))
            coalesced_counts[event_key] = coalesced_counts.get(event_key, 0) + 1
            continue
        kept_entries.append(entry)

    return BatchDispatchPlan(
        entries_to_process=tuple(kept_entries),
        stale_entries_to_ack=tuple(stale_entries),
        coalesced_counts=tuple(sorted(coalesced_counts.items())),
    )


def _lane_stream_group(lane: str) -> tuple[str, str]:
    normalized = str(lane).strip().lower()
    if normalized == "hot":
        return STREAM_LIVE_HOT, GROUP_LIVE_HOT
    if normalized == "warm":
        return STREAM_LIVE_WARM, GROUP_LIVE_WARM
    if normalized == "tier_1":
        return STREAM_LIVE_TIER_1, GROUP_LIVE_TIER_1
    if normalized == "tier_2":
        return STREAM_LIVE_TIER_2, GROUP_LIVE_TIER_2
    if normalized == "tier_3":
        return STREAM_LIVE_TIER_3, GROUP_LIVE_TIER_3
    raise ValueError(f"Unsupported live lane: {lane!r}")


def _lane_concurrency_env_names(lane: str) -> tuple[str, ...]:
    normalized = str(lane).strip().lower()
    if normalized == "tier_1":
        return ("SOFASCORE_LIVE_TIER_1_WORKER_MAX_CONCURRENCY", "SOFASCORE_LIVE_HOT_WORKER_MAX_CONCURRENCY")
    if normalized == "tier_2":
        return ("SOFASCORE_LIVE_TIER_2_WORKER_MAX_CONCURRENCY",)
    if normalized == "tier_3":
        return ("SOFASCORE_LIVE_TIER_3_WORKER_MAX_CONCURRENCY",)
    if normalized == "hot":
        return ("SOFASCORE_LIVE_HOT_WORKER_MAX_CONCURRENCY",)
    return ("SOFASCORE_LIVE_WARM_WORKER_MAX_CONCURRENCY",)


def _job_to_stream_payload(job: JobEnvelope) -> dict[str, object]:
    """Serialise a JobEnvelope into the same flat dict shape the planner uses
    when XADDing to a stream. Kept locally (rather than imported from
    services.planner_daemon) so the live worker module has no service-layer
    dependency."""
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


def _env_optional_positive_int(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return None
    try:
        value = int(str(raw))
    except ValueError:
        logger.warning("Invalid %s=%r; ignoring (no backpressure cap)", name, raw)
        return None
    if value < 1:
        return None
    return value
