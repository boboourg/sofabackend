"""Continuous sport-level live discovery planner for guaranteed live route coverage."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from typing import Any

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
        drifted_sports_loader=None,
        repair_cooldown_ms: int = 30_000,
    ) -> None:
        self.queue = queue
        self.targets = tuple(targets)
        self.stream = stream
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.loop_interval_s = float(loop_interval_s)
        self.backpressure = backpressure
        self.drifted_sports_loader = drifted_sports_loader
        self.repair_cooldown_ms = max(1, int(repair_cooldown_ms))
        self.shutdown_requested = False
        self._last_planned_at_ms: dict[str, int] = {}
        self._last_repair_planned_at_ms: dict[str, int] = {}

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
        drifted_sports = await self._load_drifted_sports(observed_now)
        blocking_reason = _blocking_reason(self.backpressure)
        published = 0
        for target in self.targets:
            is_regular_due = self._scheduled_target_due(target, observed_now)
            repair_reason = drifted_sports.get(target.sport_slug)
            is_repair_due = repair_reason is not None and self._repair_due(target.sport_slug, observed_now)
            if not is_regular_due and not is_repair_due:
                continue
            if blocking_reason is not None and not is_repair_due:
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
            if blocking_reason is not None and is_repair_due:
                logger.info(
                    "Live discovery planner bypassing backpressure for drifted sport %s: %s (%s)",
                    target.sport_slug,
                    repair_reason,
                    blocking_reason,
                )
            elif is_repair_due and not is_regular_due:
                logger.info(
                    "Live discovery planner forcing drift repair for sport %s: %s",
                    target.sport_slug,
                    repair_reason,
                )
            self.queue.publish(self.stream, encode_stream_job(job))
            self._last_planned_at_ms[target.sport_slug] = observed_now
            if is_repair_due:
                self._last_repair_planned_at_ms[target.sport_slug] = observed_now
            published += 1
        if blocking_reason is not None and published == 0:
            logger.info("Live discovery planner paused publishing because of backpressure: %s", blocking_reason)
        return published

    def _scheduled_target_due(self, target: LiveDiscoveryPlanningTarget, now_ms: int) -> bool:
        last_planned = self._last_planned_at_ms.get(target.sport_slug)
        if last_planned is None:
            return True
        return (now_ms - last_planned) >= int(target.interval_ms)

    def _repair_due(self, sport_slug: str, now_ms: int) -> bool:
        last_repair_planned = self._last_repair_planned_at_ms.get(sport_slug)
        if last_repair_planned is None:
            return True
        return (now_ms - last_repair_planned) >= self.repair_cooldown_ms

    async def _load_drifted_sports(self, now_ms: int) -> dict[str, str]:
        if self.drifted_sports_loader is None:
            return {}
        loaded = await _maybe_await(self.drifted_sports_loader(now_ms=now_ms))
        if isinstance(loaded, Mapping):
            return {str(sport_slug): str(reason) for sport_slug, reason in loaded.items()}
        if isinstance(loaded, Collection):
            return {str(sport_slug): "drift_detected" for sport_slug in loaded}
        raise TypeError(f"Unsupported drifted sports payload: {type(loaded)!r}")


def _blocking_reason(backpressure: object | None) -> str | None:
    if backpressure is None:
        return None
    blocking_reason = getattr(backpressure, "blocking_reason", None)
    if not callable(blocking_reason):
        return None
    return blocking_reason()


async def _maybe_await(value: Any) -> Any:
    if asyncio.iscoroutine(value) or isinstance(value, asyncio.Future):
        return await value
    return value
