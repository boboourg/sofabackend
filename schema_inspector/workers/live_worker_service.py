"""Continuous live refresh worker services for hot and warm lanes."""

from __future__ import annotations

import time

from ..queue.streams import STREAM_LIVE_HOT, STREAM_LIVE_WARM, StreamEntry
from ..services.worker_runtime import WorkerRuntime
from ._stream_jobs import decode_stream_job


class LiveWorkerService:
    def __init__(
        self,
        *,
        orchestrator,
        delayed_scheduler,
        delayed_payload_store=None,
        queue,
        lane: str,
        consumer: str,
        block_ms: int = 5_000,
        now_ms_factory=None,
        default_sport_slug: str = "football",
    ) -> None:
        normalized_lane = str(lane).strip().lower()
        if normalized_lane not in {"hot", "warm"}:
            raise ValueError(f"Unsupported live lane: {lane!r}")

        self.orchestrator = orchestrator
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
        self.lane = normalized_lane
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.default_sport_slug = default_sport_slug
        stream = STREAM_LIVE_HOT if normalized_lane == "hot" else STREAM_LIVE_WARM
        group = f"cg:live_{normalized_lane}"
        self.runtime = WorkerRuntime(
            name=f"live-{normalized_lane}-worker",
            queue=queue,
            stream=stream,
            group=group,
            consumer=consumer,
            handler=self.handle,
            retry_handler=self.retry_later,
            block_ms=block_ms,
        )

    async def handle(self, entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        if job.entity_id is None:
            raise RuntimeError("Live worker requires event entity_id in stream payload.")
        await self.orchestrator.run_event(
            event_id=int(job.entity_id),
            sport_slug=job.sport_slug or self.default_sport_slug,
            hydration_mode="full",
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
