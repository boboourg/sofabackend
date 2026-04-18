"""Continuous sport-level live discovery planner for guaranteed live route coverage."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_DISCOVER_SPORT_SURFACE
from ..queue.streams import STREAM_LIVE_DISCOVERY
from ..workers._stream_jobs import encode_stream_job

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LiveDiscoveryPlanningTarget:
    sport_slug: str
    interval_ms: int
    priority: int = 25


class LiveDiscoveryPlannerDaemon:
    """Publishes sport-level live discovery jobs on a per-sport cadence."""

    def __init__(
        self,
        *,
        queue,
        targets: tuple[LiveDiscoveryPlanningTarget, ...],
        stream: str = STREAM_LIVE_DISCOVERY,
        now_ms_factory=None,
        loop_interval_s: float = 5.0,
        backpressure=None,
    ) -> None:
        self.queue = queue
        self.targets = tuple(targets)
        self.stream = stream
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.loop_interval_s = float(loop_interval_s)
        self.backpressure = backpressure
        self.shutdown_requested = False
        self._last_planned_at_ms: dict[str, int] = {}

    def request_shutdown(self) -> None:
        self.shutdown_requested = True

    async def run_forever(self) -> None:
        while not self.shutdown_requested:
            await self.tick()
            if self.shutdown_requested:
                break
            await asyncio.sleep(self.loop_interval_s)

    async def tick(self, *, now_ms: int | None = None) -> int:
        observed_now = int(now_ms if now_ms is not None else self.now_ms_factory())
        blocking_reason = _blocking_reason(self.backpressure)
        if blocking_reason is not None:
            logger.info("Live discovery planner paused by backpressure: %s", blocking_reason)
            return 0
        published = 0
        for target in self.targets:
            last_planned = self._last_planned_at_ms.get(target.sport_slug)
            if last_planned is not None and (observed_now - last_planned) < int(target.interval_ms):
                continue
            job = JobEnvelope.create(
                job_type=JOB_DISCOVER_SPORT_SURFACE,
                sport_slug=target.sport_slug,
                entity_type="sport",
                entity_id=None,
                scope="live",
                params={},
                priority=target.priority,
                trace_id=None,
            )
            self.queue.publish(self.stream, encode_stream_job(job))
            self._last_planned_at_ms[target.sport_slug] = observed_now
            published += 1
        return published


def _blocking_reason(backpressure: object | None) -> str | None:
    if backpressure is None:
        return None
    blocking_reason = getattr(backpressure, "blocking_reason", None)
    if not callable(blocking_reason):
        return None
    return blocking_reason()
