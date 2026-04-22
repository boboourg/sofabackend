"""Historical tournament and enrichment workers."""

from __future__ import annotations

import time

from ..jobs.types import (
    JOB_ENRICH_TOURNAMENT_ARCHIVE,
    JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH,
    JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH,
    JOB_SYNC_TOURNAMENT_ARCHIVE,
)
from ..queue.streams import (
    GROUP_HISTORICAL_ENRICHMENT,
    GROUP_HISTORICAL_TOURNAMENT,
    STREAM_HISTORICAL_ENRICHMENT,
    STREAM_HISTORICAL_TOURNAMENT,
    StreamEntry,
)
from ..services.worker_runtime import WorkerRuntime
from ._stream_jobs import decode_stream_job, encode_stream_job


class HistoricalTournamentWorker:
    def __init__(
        self,
        *,
        orchestrator,
        queue,
        consumer: str,
        group: str = GROUP_HISTORICAL_TOURNAMENT,
        stream: str = STREAM_HISTORICAL_TOURNAMENT,
        enrichment_stream: str = STREAM_HISTORICAL_ENRICHMENT,
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
        self.enrichment_stream = enrichment_stream
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.runtime = WorkerRuntime(
            name="historical-tournament-worker",
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
        if job.job_type != JOB_SYNC_TOURNAMENT_ARCHIVE:
            return "ignored"
        if job.entity_id is None:
            raise RuntimeError("Historical tournament worker requires unique_tournament entity_id.")
        sport_slug = str(job.sport_slug or "").strip().lower()
        result = await self.orchestrator.run_historical_tournament_archive(
            unique_tournament_id=int(job.entity_id),
            sport_slug=sport_slug,
        )
        season_ids = tuple(int(item) for item in result.get("season_ids", ()) or ())
        for child_job_type in (
            JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH,
            JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH,
        ):
            enrich_job = job.spawn_child(
                job_type=child_job_type,
                entity_type="unique_tournament",
                entity_id=int(job.entity_id),
                scope="historical",
                params={"season_ids": list(season_ids)},
                priority=job.priority,
            )
            self.queue.publish(self.enrichment_stream, encode_stream_job(enrich_job))
        return "completed"

    async def retry_later(self, entry: StreamEntry, exc: Exception, *, delay_ms: int) -> str:
        del exc
        job = decode_stream_job(entry)
        if self.delayed_payload_store is not None:
            self.delayed_payload_store.save_entry(entry)
        self.delayed_scheduler.schedule(job.job_id, run_at_epoch_ms=int(self.now_ms_factory()) + int(delay_ms))
        return "requeued"

    async def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        await self.runtime.run_forever(install_signal_handlers=install_signal_handlers)


class HistoricalEnrichmentWorker:
    def __init__(
        self,
        *,
        orchestrator,
        queue,
        consumer: str,
        group: str = GROUP_HISTORICAL_ENRICHMENT,
        stream: str = STREAM_HISTORICAL_ENRICHMENT,
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
            name="historical-enrichment-worker",
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
        if job.job_type not in {
            JOB_ENRICH_TOURNAMENT_ARCHIVE,
            JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH,
            JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH,
        }:
            return "ignored"
        if job.entity_id is None:
            raise RuntimeError("Historical enrichment worker requires unique_tournament entity_id.")
        sport_slug = str(job.sport_slug or "").strip().lower()
        season_ids = tuple(int(item) for item in job.params.get("season_ids", ()) or ())
        if job.job_type == JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH:
            await self.orchestrator.run_historical_tournament_event_detail_batch(
                unique_tournament_id=int(job.entity_id),
                sport_slug=sport_slug,
                season_ids=season_ids,
            )
            return "completed"
        if job.job_type == JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH:
            await self.orchestrator.run_historical_tournament_entities_batch(
                unique_tournament_id=int(job.entity_id),
                sport_slug=sport_slug,
                season_ids=season_ids,
            )
            return "completed"
        for child_job_type in (
            JOB_ENRICH_TOURNAMENT_EVENT_DETAIL_BATCH,
            JOB_ENRICH_TOURNAMENT_ENTITIES_BATCH,
        ):
            child_job = job.spawn_child(
                job_type=child_job_type,
                entity_type="unique_tournament",
                entity_id=int(job.entity_id),
                scope="historical",
                params={"season_ids": list(season_ids)},
                priority=job.priority,
            )
            self.queue.publish(self.stream, encode_stream_job(child_job))
        return "completed"

    async def retry_later(self, entry: StreamEntry, exc: Exception, *, delay_ms: int) -> str:
        del exc
        job = decode_stream_job(entry)
        if self.delayed_payload_store is not None:
            self.delayed_payload_store.save_entry(entry)
        self.delayed_scheduler.schedule(job.job_id, run_at_epoch_ms=int(self.now_ms_factory()) + int(delay_ms))
        return "requeued"

    async def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        await self.runtime.run_forever(install_signal_handlers=install_signal_handlers)
