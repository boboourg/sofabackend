"""Continuous hydrate worker backed by the shared worker runtime."""

from __future__ import annotations

import time

from ..live_hydration_mode import resolve_live_hydration_mode
from ..queue.streams import GROUP_HYDRATE, STREAM_HISTORICAL_HYDRATE, STREAM_HYDRATE, StreamEntry
from ..services.worker_runtime import WorkerRuntime, resolve_worker_max_concurrency
from ._stream_jobs import decode_stream_job


class HydrateWorker:
    def __init__(
        self,
        *,
        orchestrator,
        delayed_scheduler,
        delayed_payload_store=None,
        completion_store=None,
        queue,
        consumer: str,
        group: str = GROUP_HYDRATE,
        stream: str = STREAM_HYDRATE,
        block_ms: int = 5_000,
        now_ms_factory=None,
        default_sport_slug: str = "football",
        job_audit_logger=None,
        max_concurrency: int | None = None,
    ) -> None:
        self.orchestrator = orchestrator
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
        self.queue = queue
        self.consumer = consumer
        self.group = group
        self.stream = stream
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.default_sport_slug = default_sport_slug
        env_names = ("SOFASCORE_HYDRATE_WORKER_MAX_CONCURRENCY",)
        if stream == STREAM_HISTORICAL_HYDRATE:
            env_names = ("SOFASCORE_HISTORICAL_HYDRATE_WORKER_MAX_CONCURRENCY", *env_names)
        self.runtime = WorkerRuntime(
            name="hydrate-worker",
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
            max_concurrency=resolve_worker_max_concurrency(
                default=2,
                explicit=max_concurrency,
                env_names=env_names,
            ),
        )

    async def handle(self, entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        if job.entity_id is None:
            raise RuntimeError("Hydrate worker requires event entity_id in stream payload.")
        sport_slug = job.sport_slug or self.default_sport_slug
        await self.orchestrator.run_event(
            event_id=int(job.entity_id),
            sport_slug=sport_slug,
            hydration_mode=resolve_live_hydration_mode(
                requested_mode=job.params.get("hydration_mode", "full"),
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
