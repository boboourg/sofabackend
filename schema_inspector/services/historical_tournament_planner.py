"""Historical tournament planner for season/tournament archival work."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_SYNC_TOURNAMENT_ARCHIVE
from ..queue.streams import STREAM_HISTORICAL_TOURNAMENT
from ..workers._stream_jobs import encode_stream_job

HISTORICAL_TOURNAMENT_CURSOR_HASH = "hash:etl:historical_tournament_cursor"
TournamentSelector = Callable[..., Awaitable[tuple[int, ...]] | tuple[int, ...]]


@dataclass(frozen=True)
class HistoricalTournamentPlanningTarget:
    sport_slug: str
    priority: int = 40


class HistoricalTournamentCursorStore:
    def __init__(self, backend, *, hash_key: str = HISTORICAL_TOURNAMENT_CURSOR_HASH) -> None:
        self.backend = backend
        self.hash_key = hash_key

    def load_last_unique_tournament_id(self, sport_slug: str) -> int:
        raw = self.backend.hgetall(self.hash_key).get(_cursor_field(sport_slug))
        if raw in (None, ""):
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def save_last_unique_tournament_id(self, sport_slug: str, unique_tournament_id: int) -> None:
        field = _cursor_field(sport_slug)
        value = str(int(unique_tournament_id))
        try:
            self.backend.hset(self.hash_key, mapping={field: value})
        except TypeError:
            self.backend.hset(self.hash_key, {field: value})


class HistoricalTournamentPlannerDaemon:
    def __init__(
        self,
        *,
        queue,
        cursor_store: HistoricalTournamentCursorStore,
        selector: TournamentSelector,
        targets: tuple[HistoricalTournamentPlanningTarget, ...],
        stream: str = STREAM_HISTORICAL_TOURNAMENT,
        tournaments_per_tick: int = 10,
        loop_interval_s: float = 10.0,
    ) -> None:
        self.queue = queue
        self.cursor_store = cursor_store
        self.selector = selector
        self.targets = tuple(targets)
        self.stream = stream
        self.tournaments_per_tick = max(1, int(tournaments_per_tick))
        self.loop_interval_s = float(loop_interval_s)
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
        published = 0
        for target in self.targets:
            after_unique_tournament_id = self.cursor_store.load_last_unique_tournament_id(target.sport_slug)
            selected_ids = await _await_maybe(
                self.selector(
                    sport_slug=target.sport_slug,
                    after_unique_tournament_id=after_unique_tournament_id,
                    limit=self.tournaments_per_tick,
                )
            )
            if not selected_ids:
                continue
            for unique_tournament_id in selected_ids:
                job = JobEnvelope.create(
                    job_type=JOB_SYNC_TOURNAMENT_ARCHIVE,
                    sport_slug=target.sport_slug,
                    entity_type="unique_tournament",
                    entity_id=int(unique_tournament_id),
                    scope="historical",
                    params={},
                    priority=target.priority,
                    trace_id=None,
                )
                self.queue.publish(self.stream, encode_stream_job(job))
                published += 1
            self.cursor_store.save_last_unique_tournament_id(target.sport_slug, int(selected_ids[-1]))
        return published


async def _await_maybe(value: object) -> object:
    if isinstance(value, Awaitable):
        return await value
    return value


def _cursor_field(sport_slug: str) -> str:
    return f"{str(sport_slug).strip().lower()}:last_unique_tournament_id"
