"""Dedicated worker for the bootstrap historical-archive lane.

Reuses ``HistoricalTournamentWorker.handle`` (which already inspects
catalog state and dispatches in bootstrap_mode=True for pending rows),
but consumes from ``stream:etl:historical_bootstrap`` /
``cg:historical_bootstrap`` so bootstrap publishes do not lose the
FIFO race against heavy cursor walks in the shared lane.

Adds structured logging: ``bootstrap_claim``, ``bootstrap_job_done``,
``bootstrap_job_failed`` so operators can grep journalctl for
per-job throughput without parsing the existing event-detail noise.
"""

from __future__ import annotations

import logging
import time

from ..queue.streams import (
    GROUP_HISTORICAL_BOOTSTRAP,
    STREAM_HISTORICAL_BOOTSTRAP,
    StreamEntry,
)
from ..workers._stream_jobs import decode_stream_job
from ..workers.historical_archive_worker import HistoricalTournamentWorker

logger = logging.getLogger(__name__)


def make_historical_bootstrap_worker(
    *,
    orchestrator,
    queue,
    consumer: str,
    block_ms: int = 5_000,
    delayed_scheduler=None,
    delayed_payload_store=None,
    completion_store=None,
    now_ms_factory=None,
    job_audit_logger=None,
) -> HistoricalTournamentWorker:
    """Construct a HistoricalTournamentWorker pinned to the bootstrap
    stream + group, with a logging wrapper around its ``handle`` method.

    Named ``make_*`` (not ``build_*``) so it does not shadow the
    ``ServiceApp.build_historical_bootstrap_worker`` method that imports
    and calls this factory.
    """

    worker = HistoricalTournamentWorker(
        orchestrator=orchestrator,
        queue=queue,
        consumer=consumer,
        group=GROUP_HISTORICAL_BOOTSTRAP,
        stream=STREAM_HISTORICAL_BOOTSTRAP,
        block_ms=block_ms,
        delayed_scheduler=delayed_scheduler,
        delayed_payload_store=delayed_payload_store,
        completion_store=completion_store,
        now_ms_factory=now_ms_factory,
        job_audit_logger=job_audit_logger,
    )

    original_handle = worker.handle

    async def _handle_with_logging(entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        ut_id = job.entity_id
        season_id = None
        if job.params:
            raw = job.params.get("target_season_id")
            if raw not in (None, ""):
                try:
                    season_id = int(raw)
                except (TypeError, ValueError):
                    season_id = None

        logger.info(
            "bootstrap_claim: msg_id=%s consumer=%s ut=%s season=%s",
            entry.message_id, consumer, ut_id, season_id,
        )

        start_ms = int(time.time() * 1000)
        try:
            result = await original_handle(entry)
        except Exception as exc:
            duration_ms = int(time.time() * 1000) - start_ms
            logger.warning(
                "bootstrap_job_failed: ut=%s season=%s duration_ms=%d exc=%s",
                ut_id, season_id, duration_ms, exc,
                exc_info=True,
            )
            raise

        duration_ms = int(time.time() * 1000) - start_ms
        logger.info(
            "bootstrap_job_done: ut=%s season=%s duration_ms=%d result=%s",
            ut_id, season_id, duration_ms, result,
        )
        return result

    worker.handle = _handle_with_logging  # type: ignore[assignment]
    # WorkerRuntime captured the original ``self.handle`` reference at
    # construction time — rebind so the runtime loop calls our wrapper.
    worker.runtime.handler = _handle_with_logging
    return worker
