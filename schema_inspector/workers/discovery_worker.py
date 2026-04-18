"""Continuous discovery worker that expands planner jobs into hydrate jobs."""

from __future__ import annotations

from datetime import datetime, timezone

from ..jobs.types import JOB_DISCOVER_SPORT_SURFACE, JOB_HYDRATE_EVENT_ROOT
from ..queue.streams import GROUP_DISCOVERY, STREAM_DISCOVERY, STREAM_HYDRATE, StreamEntry
from ..services.worker_runtime import WorkerRuntime
from ._stream_jobs import decode_stream_job, encode_stream_job


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
            event_ids = await self.orchestrator.discover_live_event_ids(
                sport_slug=sport_slug,
                timeout=self.timeout_s,
            )
            hydration_mode = "full"
        else:
            observed_date = str(job.params.get("date") or _utc_today())
            event_ids = await self.orchestrator.discover_scheduled_event_ids(
                sport_slug=sport_slug,
                date=observed_date,
                timeout=self.timeout_s,
            )
            hydration_mode = "core"

        for event_id in event_ids:
            hydrate_job = job.spawn_child(
                job_type=JOB_HYDRATE_EVENT_ROOT,
                entity_type="event",
                entity_id=int(event_id),
                scope=scope,
                params={"hydration_mode": hydration_mode},
                priority=job.priority,
            )
            self.queue.publish(self.hydrate_stream, encode_stream_job(hydrate_job))
        return f"published:{len(event_ids)}"

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


def _utc_today() -> str:
    return datetime.now(timezone.utc).date().isoformat()
