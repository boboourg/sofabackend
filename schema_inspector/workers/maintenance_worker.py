"""Continuous maintenance worker backed by the shared worker runtime."""

from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass

from ..queue.streams import STREAM_DISCOVERY, STREAM_DLQ, STREAM_HYDRATE, STREAM_LIVE_HOT, STREAM_LIVE_WARM, STREAM_MAINTENANCE, StreamEntry
from ..services.worker_runtime import WorkerRuntime
from ._stream_jobs import decode_stream_job


@dataclass(frozen=True)
class ReclaimTarget:
    stream: str
    group: str


@dataclass(frozen=True)
class ReclaimReport:
    reclaimed: int = 0
    requeued: int = 0
    dlq: int = 0
    skipped_completed: int = 0


class MaintenanceWorker:
    def __init__(
        self,
        *,
        handler,
        queue,
        consumer: str,
        delayed_scheduler=None,
        delayed_payload_store=None,
        completion_store=None,
        group: str = "cg:maintenance",
        stream: str = STREAM_MAINTENANCE,
        block_ms: int = 5_000,
        reclaim_targets: tuple[ReclaimTarget, ...] = (
            ReclaimTarget(stream=STREAM_DISCOVERY, group="cg:discovery"),
            ReclaimTarget(stream=STREAM_HYDRATE, group="cg:hydrate"),
            ReclaimTarget(stream=STREAM_LIVE_HOT, group="cg:live_hot"),
            ReclaimTarget(stream=STREAM_LIVE_WARM, group="cg:live_warm"),
        ),
        reclaim_interval_s: float = 15.0,
        reclaim_min_idle_ms: int = 30_000,
        max_delivery_count: int = 5,
        reclaim_consumer: str | None = None,
        now_ms_factory=None,
        job_audit_logger=None,
    ) -> None:
        self.handler = handler
        self.queue = queue
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
        self.completion_store = completion_store
        self.reclaim_targets = tuple(reclaim_targets)
        self.reclaim_interval_s = float(reclaim_interval_s)
        self.reclaim_min_idle_ms = int(reclaim_min_idle_ms)
        self.max_delivery_count = int(max_delivery_count)
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.reclaim_consumer = reclaim_consumer or consumer
        self.runtime = WorkerRuntime(
            name="maintenance-worker",
            queue=queue,
            stream=stream,
            group=group,
            consumer=consumer,
            handler=self.handle,
            retry_handler=self.retry_later if delayed_scheduler is not None else None,
            block_ms=block_ms,
            completion_store=completion_store,
            now_ms_factory=self.now_ms_factory,
            job_audit_logger=job_audit_logger,
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
        if self.delayed_payload_store is not None:
            self.delayed_payload_store.save_entry(entry)
        self.delayed_scheduler.schedule(
            job.job_id,
            run_at_epoch_ms=int(self.now_ms_factory()) + int(delay_ms),
        )
        return "requeued"

    async def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        background = None
        try:
            background = asyncio.create_task(self.run_reclaim_loop())
            await self.runtime.run_forever(install_signal_handlers=install_signal_handlers)
        finally:
            self.request_shutdown()
            if background is not None:
                background.cancel()
                try:
                    await background
                except asyncio.CancelledError:
                    pass

    def request_shutdown(self) -> None:
        self.runtime.request_shutdown()

    async def run_reclaim_loop(self) -> None:
        while not self.runtime.shutdown_requested:
            await self.reclaim_once()
            if self.runtime.shutdown_requested:
                break
            await asyncio.sleep(self.reclaim_interval_s)

    async def reclaim_once(self) -> ReclaimReport:
        reclaimed = 0
        requeued = 0
        dlq = 0
        skipped_completed = 0
        for target in self.reclaim_targets:
            claimed = self.queue.claim_stale(
                target.stream,
                target.group,
                self.reclaim_consumer,
                min_idle_ms=self.reclaim_min_idle_ms,
                count=100,
            )
            if not claimed:
                continue
            reclaimed += len(claimed)
            pending = {
                row.message_id: row
                for row in self.queue.pending_entries(
                    target.stream,
                    target.group,
                    count=100,
                    consumer=self.reclaim_consumer,
                )
            }
            for entry in claimed:
                if self.runtime.is_entry_completed(entry):
                    self.queue.ack(target.stream, target.group, (entry.message_id,))
                    skipped_completed += 1
                    continue
                pending_row = pending.get(entry.message_id)
                delivery_count = int(getattr(pending_row, "delivery_count", 0) or 0)
                if delivery_count >= self.max_delivery_count:
                    self.queue.publish(
                        STREAM_DLQ,
                        {
                            **entry.values,
                            "source_stream": target.stream,
                            "source_group": target.group,
                            "source_message_id": entry.message_id,
                            "dlq_reason": "max_delivery_exceeded",
                        },
                    )
                    self.queue.ack(target.stream, target.group, (entry.message_id,))
                    dlq += 1
                    continue
                self.queue.publish(target.stream, entry.values)
                self.queue.ack(target.stream, target.group, (entry.message_id,))
                requeued += 1
        return ReclaimReport(
            reclaimed=reclaimed,
            requeued=requeued,
            dlq=dlq,
            skipped_completed=skipped_completed,
        )
