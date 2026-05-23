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


class HistoricalBootstrapWorker(HistoricalTournamentWorker):
    """``HistoricalTournamentWorker`` pinned to the bootstrap stream/group,
    with structured per-job logging wrapped around ``handle``.

    The handler in the parent class already inspects catalog state and
    dispatches in ``bootstrap_mode=True`` for pending rows — there is no
    business logic to add here. The subclass exists to:
      1. Bind ``stream`` and ``group`` to the dedicated bootstrap lane
         constants by default (callers can still override).
      2. Emit ``bootstrap_claim`` / ``bootstrap_job_done`` /
         ``bootstrap_job_failed`` log lines so operators can grep
         journalctl for per-job throughput.

    Subclassing (vs. monkey-patching the parent's ``handle``) avoids an
    implicit contract on ``WorkerRuntime.handler`` capture timing.
    """

    def __init__(
        self,
        *,
        orchestrator,
        queue,
        consumer: str,
        group: str = GROUP_HISTORICAL_BOOTSTRAP,
        stream: str = STREAM_HISTORICAL_BOOTSTRAP,
        block_ms: int = 5_000,
        delayed_scheduler=None,
        delayed_payload_store=None,
        completion_store=None,
        now_ms_factory=None,
        job_audit_logger=None,
    ) -> None:
        super().__init__(
            orchestrator=orchestrator,
            queue=queue,
            consumer=consumer,
            group=group,
            stream=stream,
            block_ms=block_ms,
            delayed_scheduler=delayed_scheduler,
            delayed_payload_store=delayed_payload_store,
            completion_store=completion_store,
            now_ms_factory=now_ms_factory,
            job_audit_logger=job_audit_logger,
        )

    async def handle(self, entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        ut_id = job.entity_id
        season_id = _extract_season_id(job.params)

        logger.info(
            "bootstrap_claim: msg_id=%s consumer=%s ut=%s season=%s",
            entry.message_id, self.consumer, ut_id, season_id,
        )

        start_ms = time.monotonic() * 1000
        try:
            result = await super().handle(entry)
        except Exception as exc:
            duration_ms = int(time.monotonic() * 1000 - start_ms)
            logger.warning(
                "bootstrap_job_failed: msg_id=%s ut=%s season=%s duration_ms=%d exc=%s",
                entry.message_id, ut_id, season_id, duration_ms, exc,
                exc_info=True,
            )
            raise

        duration_ms = int(time.monotonic() * 1000 - start_ms)
        logger.info(
            "bootstrap_job_done: msg_id=%s ut=%s season=%s duration_ms=%d result=%s",
            entry.message_id, ut_id, season_id, duration_ms, result,
        )
        return result


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
) -> HistoricalBootstrapWorker:
    """Factory for ``HistoricalBootstrapWorker``. Named ``make_*`` (not
    ``build_*``) so it does not shadow the
    ``ServiceApp.build_historical_bootstrap_worker`` method that imports
    and calls this factory."""

    return HistoricalBootstrapWorker(
        orchestrator=orchestrator,
        queue=queue,
        consumer=consumer,
        block_ms=block_ms,
        delayed_scheduler=delayed_scheduler,
        delayed_payload_store=delayed_payload_store,
        completion_store=completion_store,
        now_ms_factory=now_ms_factory,
        job_audit_logger=job_audit_logger,
    )


def _extract_season_id(params: dict | None) -> int | None:
    if not params:
        return None
    raw = params.get("target_season_id")
    if raw in (None, ""):
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None
