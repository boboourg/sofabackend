"""Continuous maintenance worker backed by the shared worker runtime."""

from __future__ import annotations

import inspect
import time

from ..queue.streams import STREAM_MAINTENANCE, StreamEntry
from ..services.worker_runtime import WorkerRuntime
from ._stream_jobs import decode_stream_job


class MaintenanceWorker:
    def __init__(
        self,
        *,
        handler,
        queue,
        consumer: str,
        delayed_scheduler=None,
        group: str = "cg:maintenance",
        stream: str = STREAM_MAINTENANCE,
        block_ms: int = 5_000,
        now_ms_factory=None,
    ) -> None:
        self.handler = handler
        self.delayed_scheduler = delayed_scheduler
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.runtime = WorkerRuntime(
            name="maintenance-worker",
            queue=queue,
            stream=stream,
            group=group,
            consumer=consumer,
            handler=self.handle,
            retry_handler=self.retry_later if delayed_scheduler is not None else None,
            block_ms=block_ms,
        )

    async def handle(self, item: StreamEntry):
        result = self.handler(item)
        if inspect.isawaitable(result):
            return await result
        return result

    async def retry_later(self, entry: StreamEntry, exc: Exception, *, delay_ms: int) -> str:
        del exc
        if self.delayed_scheduler is None:
            return "ignored"
        job = decode_stream_job(entry)
        self.delayed_scheduler.schedule(
            job.job_id,
            run_at_epoch_ms=int(self.now_ms_factory()) + int(delay_ms),
        )
        return "requeued"

    async def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        await self.runtime.run_forever(install_signal_handlers=install_signal_handlers)

    def request_shutdown(self) -> None:
        self.runtime.request_shutdown()
