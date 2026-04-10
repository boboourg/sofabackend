"""Async ETL jobs for event-list endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .db import AsyncpgDatabase
from .event_list_parser import EventListBundle, EventListParser
from .event_list_repository import EventListRepository, EventListWriteResult


@dataclass(frozen=True)
class EventListIngestResult:
    job_name: str
    parsed: EventListBundle
    written: EventListWriteResult


class EventListIngestJob:
    """Runs one event-list parser flow and persists it transactionally."""

    def __init__(
        self,
        parser: EventListParser,
        repository: EventListRepository,
        database: AsyncpgDatabase,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.parser = parser
        self.repository = repository
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    async def run_scheduled(self, date: str, *, timeout: float = 20.0) -> EventListIngestResult:
        return await self._run("scheduled", self.parser.fetch_scheduled_events(date, timeout=timeout))

    async def run_live(self, *, timeout: float = 20.0) -> EventListIngestResult:
        return await self._run("live", self.parser.fetch_live_events(timeout=timeout))

    async def run_featured(self, unique_tournament_id: int, *, timeout: float = 20.0) -> EventListIngestResult:
        return await self._run(
            f"featured:{unique_tournament_id}",
            self.parser.fetch_featured_events(unique_tournament_id, timeout=timeout),
        )

    async def run_round(
        self,
        unique_tournament_id: int,
        season_id: int,
        round_number: int,
        *,
        timeout: float = 20.0,
    ) -> EventListIngestResult:
        return await self._run(
            f"round:{unique_tournament_id}:{season_id}:{round_number}",
            self.parser.fetch_round_events(unique_tournament_id, season_id, round_number, timeout=timeout),
        )

    async def _run(self, job_name: str, bundle_awaitable) -> EventListIngestResult:
        bundle = await bundle_awaitable
        async with self.database.transaction() as connection:
            write_result = await self.repository.upsert_bundle(connection, bundle)
        self.logger.info("Event-list ingest completed: job=%s events=%s", job_name, write_result.event_rows)
        return EventListIngestResult(job_name=job_name, parsed=bundle, written=write_result)
