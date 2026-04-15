"""Async ETL job for scheduled-tournament discovery endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .db import AsyncpgDatabase
from .event_list_repository import EventListRepository, EventListWriteResult
from .scheduled_tournaments_parser import ScheduledTournamentsBundle, ScheduledTournamentsParser


@dataclass(frozen=True)
class ScheduledTournamentsIngestResult:
    job_name: str
    parsed: ScheduledTournamentsBundle
    written: EventListWriteResult


class ScheduledTournamentsIngestJob:
    """Persists one scheduled-tournament discovery page transactionally."""

    def __init__(
        self,
        parser: ScheduledTournamentsParser,
        repository: EventListRepository,
        database: AsyncpgDatabase,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.parser = parser
        self.repository = repository
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    async def run_page(
        self,
        observed_date: str,
        page: int,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> ScheduledTournamentsIngestResult:
        bundle = await self.parser.fetch_page(
            observed_date,
            page,
            sport_slug=sport_slug,
            timeout=timeout,
        )
        async with self.database.transaction() as connection:
            write_result = await self.repository.upsert_bundle(connection, bundle.event_list_bundle)
        self.logger.info(
            "Scheduled tournaments ingest completed: sport=%s date=%s page=%s tournaments=%s unique_tournaments=%s",
            sport_slug,
            observed_date,
            page,
            len(bundle.tournament_ids),
            len(bundle.unique_tournament_ids),
        )
        return ScheduledTournamentsIngestResult(
            job_name=f"scheduled_tournaments:{sport_slug}:{observed_date}:{page}",
            parsed=bundle,
            written=write_result,
        )
