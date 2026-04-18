"""Historical date-range planner for the archival ETL contour."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import date as Date

from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_DISCOVER_SPORT_SURFACE
from ..queue.streams import STREAM_HISTORICAL_DISCOVERY
from ..workers._stream_jobs import encode_stream_job

HISTORICAL_CURSOR_HASH = "hash:etl:historical_cursor"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HistoricalPlanningTarget:
    sport_slug: str
    date_from: str
    date_to: str
    priority: int = 50


class HistoricalCursorStore:
    def __init__(self, backend, *, hash_key: str = HISTORICAL_CURSOR_HASH) -> None:
        self.backend = backend
        self.hash_key = hash_key

    def load_next_date(self, sport_slug: str, date_from: str, date_to: str) -> str | None:
        raw = self.backend.hgetall(self.hash_key).get(_cursor_field(sport_slug, date_from, date_to))
        if raw in (None, ""):
            return None
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="ignore")
        return str(raw)

    def save_next_date(self, sport_slug: str, date_from: str, date_to: str, next_date: str) -> None:
        field = _cursor_field(sport_slug, date_from, date_to)
        try:
            self.backend.hset(self.hash_key, mapping={field: next_date})
        except TypeError:
            self.backend.hset(self.hash_key, {field: next_date})


class HistoricalPlannerDaemon:
    def __init__(
        self,
        *,
        queue,
        cursor_store: HistoricalCursorStore,
        targets: tuple[HistoricalPlanningTarget, ...],
        stream: str = STREAM_HISTORICAL_DISCOVERY,
        dates_per_tick: int = 1,
        loop_interval_s: float = 5.0,
        backpressure=None,
    ) -> None:
        self.queue = queue
        self.cursor_store = cursor_store
        self.targets = tuple(targets)
        self.stream = stream
        self.dates_per_tick = max(1, int(dates_per_tick))
        self.loop_interval_s = float(loop_interval_s)
        self.backpressure = backpressure
        self.shutdown_requested = False

    def request_shutdown(self) -> None:
        self.shutdown_requested = True

    async def run_forever(self) -> None:
        while not self.shutdown_requested:
            await self.tick()
            if self.shutdown_requested:
                break
            await asyncio.sleep(self.loop_interval_s)

    async def tick(self) -> int:
        if self.backpressure is not None:
            reason = self.backpressure.blocking_reason()
            if reason:
                logger.info("Historical planner paused by backpressure: %s", reason)
                return 0
        published = 0
        for target in self.targets:
            current = self.cursor_store.load_next_date(target.sport_slug, target.date_from, target.date_to) or target.date_from
            for _ in range(self.dates_per_tick):
                if Date.fromisoformat(current) > Date.fromisoformat(target.date_to):
                    break
                job = JobEnvelope.create(
                    job_type=JOB_DISCOVER_SPORT_SURFACE,
                    sport_slug=target.sport_slug,
                    entity_type="sport",
                    entity_id=None,
                    scope="historical",
                    params={"date": current},
                    priority=target.priority,
                    trace_id=None,
                )
                self.queue.publish(self.stream, encode_stream_job(job))
                published += 1
                current = _advance_date(current)
                self.cursor_store.save_next_date(target.sport_slug, target.date_from, target.date_to, current)
        return published


def _advance_date(value: str) -> str:
    observed = Date.fromisoformat(value)
    return Date.fromordinal(observed.toordinal() + 1).isoformat()


def _cursor_field(sport_slug: str, date_from: str, date_to: str) -> str:
    return f"{str(sport_slug).strip().lower()}:{date_from}:{date_to}"
