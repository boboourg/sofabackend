"""Task 2 Phase B (2026-05-20): Final-Sync Planner Daemon.

Lifecycle: HOT → PENDING FINAL SYNC → FROZEN.

This daemon implements the PENDING → FROZEN transition. It periodically
scans ``event_terminal_state`` for events whose ``finalized_at`` is at
least ``SOFASCORE_FINAL_SYNC_DELAY_SECONDS`` (default 7200 = 2 hours)
old and whose ``locked_at`` is still NULL, then publishes one last
``JOB_HYDRATE_EVENT_ROOT`` per event with ``scope="final_sync"`` and
``hydration_mode="final_sync"``. The orchestrator stamps ``locked_at``
on success, freezing the event from further processing.

Failure tolerance: if the final sync run errors out the lock is NOT
stamped, so the planner will re-queue the event on its next tick.
Eventually the run succeeds (or operator intervention via the
``unlock-event`` CLI clears the row).

The planner reuses ``stream:etl:hydrate`` and the existing
``HydrateWorker`` fleet so no new worker pool is needed.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Protocol

from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_HYDRATE_EVENT_ROOT
from ..pipeline.pilot_orchestrator import (
    FINAL_SYNC_DELAY_SECONDS,
    FINAL_SYNC_SCOPE,
)
from ..queue.streams import STREAM_HYDRATE
from ..workers._stream_jobs import encode_stream_job

logger = logging.getLogger(__name__)


class _QueueLike(Protocol):
    def publish(self, stream: str, payload: Any) -> Any: ...


class _RepoLike(Protocol):
    async def pending_lock_event_ids(
        self,
        executor: Any,
        *,
        delay_seconds: int,
        limit: int,
    ) -> list[int]: ...


class _DatabaseLike(Protocol):
    def connection(self) -> Any: ...


@dataclass
class FinalSyncPlannerDaemon:
    """Periodic planner publishing one-shot final-sync hydrate jobs.

    Parameters
    ----------
    database
        Object exposing ``connection()`` async context manager that
        yields an asyncpg-compatible executor.
    queue
        Redis-Streams queue facade with ``publish(stream, payload)``.
    repository
        ``LiveStateRepository`` (or test fake) exposing
        ``pending_lock_event_ids``.
    delay_seconds
        How long after ``finalized_at`` before the event becomes a
        final-sync candidate. Defaults to ``FINAL_SYNC_DELAY_SECONDS``.
    batch_size
        Max events to enqueue per tick. Bounded so the planner cannot
        starve other streams when a finalize wave hits.
    tick_interval_seconds
        Sleep between ticks in ``run_forever``.
    priority
        Job priority — lower than live (8) so it does not crowd the
        live lane, higher than historical (1) so it gets ack'ed quickly.
    """

    database: _DatabaseLike
    queue: _QueueLike
    repository: _RepoLike
    delay_seconds: int = FINAL_SYNC_DELAY_SECONDS
    batch_size: int = 200
    tick_interval_seconds: int = 60
    priority: int = 4
    default_sport_slug: str = "football"

    async def tick(self, *, now_ms: int) -> int:
        """One sweep: pick due events, publish hydrate jobs, return count.

        Lock stamping is performed by the orchestrator on success path,
        NOT here — keeping retries simple: a failed final-sync run
        leaves ``locked_at`` NULL, so the next tick re-enqueues.
        """
        del now_ms  # reserved for future jitter / cooldown logic
        async with self.database.connection() as connection:
            event_ids = await self.repository.pending_lock_event_ids(
                connection,
                delay_seconds=self.delay_seconds,
                limit=self.batch_size,
            )

        published = 0
        for event_id in event_ids:
            envelope = JobEnvelope.create(
                job_type=JOB_HYDRATE_EVENT_ROOT,
                sport_slug=self.default_sport_slug,
                entity_type="event",
                entity_id=int(event_id),
                scope=FINAL_SYNC_SCOPE,
                params={
                    "hydration_mode": FINAL_SYNC_SCOPE,
                },
                priority=self.priority,
                trace_id=None,
            )
            try:
                self.queue.publish(
                    STREAM_HYDRATE,
                    encode_stream_job(envelope),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "final_sync_planner: publish failed event_id=%s: %r",
                    event_id,
                    exc,
                )
                continue
            published += 1
            logger.info(
                "final_sync_planner: published event_id=%s job_id=%s",
                event_id,
                envelope.job_id,
            )

        return published

    async def run_forever(self) -> None:
        """Continuous loop intended for use as a systemd-managed daemon.

        The interval is intentionally on the slow side (default 60s)
        because final-sync is not latency-sensitive — events sit in
        the hot path for 2 hours before becoming candidates, so an
        extra minute of wait is invisible end-to-end.
        """
        logger.info(
            "final_sync_planner: starting (delay=%ds batch=%d tick=%ds)",
            self.delay_seconds,
            self.batch_size,
            self.tick_interval_seconds,
        )
        while True:
            try:
                published = await self.tick(now_ms=0)
            except Exception as exc:  # noqa: BLE001
                logger.exception(
                    "final_sync_planner: tick failed: %r", exc
                )
                published = 0
            if published == 0:
                await asyncio.sleep(self.tick_interval_seconds)
            else:
                # When there's a backlog, tick more aggressively but
                # still yield so other tasks can run.
                await asyncio.sleep(max(1, self.tick_interval_seconds // 6))
