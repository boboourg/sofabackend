"""Continuous hydrate worker backed by the shared worker runtime."""

from __future__ import annotations

import time

from ..queue.streams import STREAM_HYDRATE, StreamEntry
from ..services.worker_runtime import WorkerRuntime
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
        group: str = "cg:hydrate",
        stream: str = STREAM_HYDRATE,
        block_ms: int = 5_000,
        now_ms_factory=None,
        default_sport_slug: str = "football",
        job_audit_logger=None,
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
        )

    async def handle(self, entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        if job.entity_id is None:
            raise RuntimeError("Hydrate worker requires event entity_id in stream payload.")
        await self.orchestrator.run_event(
            event_id=int(job.entity_id),
            sport_slug=job.sport_slug or self.default_sport_slug,
            hydration_mode=str(job.params.get("hydration_mode", "full")),
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
