"""Stream-driven snapshot normalize worker.

Consumes ``stream:etl:normalize`` envelopes published by the resource
refresh worker (and any other producer that wants its raw snapshots
turned into normalized rows). For each envelope:

1. Loads the ``api_payload_snapshot`` row pointed to by ``snapshot_id``
   into a :class:`RawSnapshot`.
2. Hands it to the shared :class:`ParserRegistry` to produce a
   :class:`ParseResult`.
3. Persists the parse result via :class:`DurableNormalizeSink` inside
   the same transaction.

Failures are classified by ``WorkerRuntime.retry_handler`` (transport-
ish errors → delayed retry; permanent shape failures still return a
status string from ``handle`` and the entry is acked).

Why this is a thin worker: every parser is replayable from the raw
snapshot row, so we never need to re-fetch upstream from the normalize
path. Decoupling fetch from parse this way makes both sides scale
independently — the fetch lane can be cold while the normalize lane
chews through a backlog.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from ..normalizers.sink import DurableNormalizeSink
from ..parsers.base import RawSnapshot
from ..queue.streams import (
    GROUP_NORMALIZE,
    STREAM_NORMALIZE,
    StreamEntry,
)
from ..services.worker_runtime import WorkerRuntime, resolve_worker_max_concurrency
from ._stream_jobs import decode_stream_job

logger = logging.getLogger(__name__)


class NormalizeStreamWorker:
    """Stream consumer that runs the parser registry against snapshots."""

    def __init__(
        self,
        *,
        database: Any,
        raw_repository: Any,
        normalize_repository: Any,
        parser_registry: Any,
        delayed_scheduler: Any,
        delayed_payload_store: Any | None = None,
        completion_store: Any | None = None,
        queue: Any,
        consumer: str,
        group: str = GROUP_NORMALIZE,
        stream: str = STREAM_NORMALIZE,
        block_ms: int = 5_000,
        now_ms_factory=None,
        job_audit_logger=None,
        max_concurrency: int | None = None,
        skip_entity_parser_families: tuple[str, ...] = (),
    ) -> None:
        self.database = database
        self.raw_repository = raw_repository
        self.normalize_repository = normalize_repository
        self.parser_registry = parser_registry
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
        self.queue = queue
        self.consumer = consumer
        self.group = group
        self.stream = stream
        self.skip_entity_parser_families = tuple(str(f) for f in skip_entity_parser_families)
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.runtime = WorkerRuntime(
            name="normalize-stream-worker",
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
                default=4,
                explicit=max_concurrency,
                env_names=("SOFASCORE_NORMALIZE_WORKER_MAX_CONCURRENCY",),
            ),
        )

    async def handle(self, entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        params = dict(job.params or {})
        snapshot_id = params.get("snapshot_id")
        try:
            snapshot_id_int = int(snapshot_id)
        except (TypeError, ValueError):
            logger.warning(
                "normalize: dropping envelope without snapshot_id job_id=%s params=%r",
                job.job_id,
                params,
            )
            return "completed"

        try:
            async with self.database.transaction() as connection:
                snapshot = await self._load_snapshot(connection, snapshot_id_int)
                if snapshot is None:
                    logger.warning(
                        "normalize: snapshot %s missing; dropping envelope",
                        snapshot_id_int,
                    )
                    return "completed"
                result = self.parser_registry.parse(snapshot)
                sink = DurableNormalizeSink(
                    self.normalize_repository,
                    connection,
                    skip_entity_parser_families=self.skip_entity_parser_families,
                )
                await sink(result)
        except Exception:
            logger.exception(
                "normalize: parse/persist failed snapshot_id=%s pattern=%s",
                snapshot_id_int,
                params.get("endpoint_pattern", ""),
            )
            raise
        return "completed"

    async def _load_snapshot(self, connection, snapshot_id: int) -> RawSnapshot | None:
        try:
            return await self.raw_repository.fetch_payload_snapshot(connection, snapshot_id)
        except KeyError:
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


__all__ = ["NormalizeStreamWorker"]
