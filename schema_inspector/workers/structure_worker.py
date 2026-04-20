"""Structural-sync worker.

Consumes ``STREAM_STRUCTURE_SYNC`` and dispatches skeleton-only syncs via
``orchestrator.run_structure_sync_for_tournament(...)``. Deliberately does
NOT spawn enrichment, statistics, standings, or leaderboards work — those
remain the responsibility of the live/historical/hydrate contours.
"""

from __future__ import annotations

import time

from ..jobs.types import JOB_SYNC_TOURNAMENT_STRUCTURE
from ..queue.streams import (
    GROUP_STRUCTURE_SYNC,
    STREAM_STRUCTURE_SYNC,
    StreamEntry,
)
from ..services.worker_runtime import WorkerRuntime
from ._stream_jobs import decode_stream_job


class StructureSyncWorker:
    def __init__(
        self,
        *,
        orchestrator,
        queue,
        consumer: str,
        group: str = GROUP_STRUCTURE_SYNC,
        stream: str = STREAM_STRUCTURE_SYNC,
        block_ms: int = 5_000,
        delayed_scheduler=None,
        delayed_payload_store=None,
        completion_store=None,
        now_ms_factory=None,
        job_audit_logger=None,
    ) -> None:
        self.orchestrator = orchestrator
        self.queue = queue
        self.consumer = consumer
        self.group = group
        self.stream = stream
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.runtime = WorkerRuntime(
            name="structure-sync-worker",
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
        if job.job_type != JOB_SYNC_TOURNAMENT_STRUCTURE:
            return "ignored"
        if job.entity_id is None:
            raise RuntimeError("Structure worker requires unique_tournament entity_id.")
        sport_slug = str(job.sport_slug or "").strip().lower()
        result = await self.orchestrator.run_structure_sync_for_tournament(
            unique_tournament_id=int(job.entity_id),
            sport_slug=sport_slug,
        )
        if getattr(result, "success", True):
            return "completed"
        reason = getattr(result, "reason", None) or "unknown structure-sync failure"
        raise RuntimeError(f"structure sync failed: {reason}")

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


__all__ = ["StructureSyncWorker"]
