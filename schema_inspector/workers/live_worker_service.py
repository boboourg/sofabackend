"""Continuous live refresh worker services for hot and warm lanes."""

from __future__ import annotations

import time

from ..jobs.types import JOB_REFRESH_LIVE_EVENT
from ..live_hydration_mode import resolve_live_hydration_mode
from ..queue.streams import (
    GROUP_LIVE_HOT,
    GROUP_LIVE_TIER_1,
    GROUP_LIVE_TIER_2,
    GROUP_LIVE_TIER_3,
    GROUP_LIVE_WARM,
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
    ) -> None:
        normalized_lane = str(lane).strip().lower()
        if normalized_lane not in {"hot", "warm", "tier_1", "tier_2", "tier_3"}:
            raise ValueError(f"Unsupported live lane: {lane!r}")

        self.orchestrator = orchestrator
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
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
        sport_slug = job.sport_slug or self.default_sport_slug
        await self.orchestrator.run_event(
            event_id=int(job.entity_id),
            sport_slug=sport_slug,
            hydration_mode=resolve_live_hydration_mode(
                requested_mode=job.params.get("hydration_mode") or "live_delta",
                sport_slug=sport_slug,
                scope=job.scope,
            ),
        )
        return "completed"

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
