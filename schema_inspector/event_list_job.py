"""Async ETL jobs for event-list endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .db import AsyncpgDatabase
from .event_list_parser import EventListBundle, EventListParser
from .event_list_repository import EventListRepository, EventListWriteResult
from .services.surface_correction_detector import SurfaceCorrection, SurfaceCorrectionDetector


@dataclass(frozen=True)
class EventListIngestResult:
    job_name: str
    parsed: EventListBundle
    written: EventListWriteResult
    corrections: tuple[SurfaceCorrection, ...] = ()


class EventListIngestJob:
    """Runs one event-list parser flow and persists it transactionally."""

    def __init__(
        self,
        parser: EventListParser,
        repository: EventListRepository,
        database: AsyncpgDatabase,
        *,
        logger: logging.Logger | None = None,
        correction_detector: SurfaceCorrectionDetector | None = None,
    ) -> None:
        self.parser = parser
        self.repository = repository
        self.database = database
        self.logger = logger or logging.getLogger(__name__)
        self.correction_detector = correction_detector or SurfaceCorrectionDetector()

    async def run_scheduled(
        self,
        date: str,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> EventListIngestResult:
        return await self._run(
            f"scheduled:{sport_slug}:{date}",
            self.parser.fetch_scheduled_events(date, sport_slug=sport_slug, timeout=timeout),
        )

    async def run_live(self, *, sport_slug: str = "football", timeout: float = 20.0) -> EventListIngestResult:
        return await self._run(f"live:{sport_slug}", self.parser.fetch_live_events(sport_slug=sport_slug, timeout=timeout))

    async def run_featured(
        self,
        unique_tournament_id: int,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> EventListIngestResult:
        return await self._run(
            f"featured:{unique_tournament_id}",
            self.parser.fetch_featured_events(unique_tournament_id, sport_slug=sport_slug, timeout=timeout),
        )

    async def run_unique_tournament_scheduled(
        self,
        unique_tournament_id: int,
        date: str,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> EventListIngestResult:
        return await self._run(
            f"tournament_scheduled:{unique_tournament_id}:{date}",
            self.parser.fetch_unique_tournament_scheduled_events(
                unique_tournament_id,
                date,
                sport_slug=sport_slug,
                timeout=timeout,
            ),
        )

    async def run_round(
        self,
        unique_tournament_id: int,
        season_id: int,
        round_number: int,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> EventListIngestResult:
        return await self._run(
            f"round:{unique_tournament_id}:{season_id}:{round_number}",
            self.parser.fetch_round_events(
                unique_tournament_id,
                season_id,
                round_number,
                sport_slug=sport_slug,
                timeout=timeout,
            ),
        )

    async def run_brackets(
        self,
        unique_tournament_id: int,
        season_id: int,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> EventListIngestResult:
        return await self._run(
            f"brackets:{unique_tournament_id}:{season_id}",
            self.parser.fetch_bracket_events(
                unique_tournament_id,
                season_id,
                sport_slug=sport_slug,
                timeout=timeout,
            ),
        )

    async def _run(self, job_name: str, bundle_awaitable) -> EventListIngestResult:
        bundle = await bundle_awaitable
        corrections: tuple[SurfaceCorrection, ...] = ()
        load_surface_states = getattr(self.repository, "load_surface_states", None)
        connection_factory = getattr(self.database, "connection", None)
        if callable(load_surface_states) and callable(connection_factory):
            async with connection_factory() as connection:
                previous_states = await load_surface_states(
                    connection,
                    tuple(int(item.id) for item in bundle.events),
                )
            corrections = self.correction_detector.detect(bundle=bundle, previous_states=previous_states)
        async with self.database.transaction() as connection:
            write_result = await self.repository.upsert_bundle(connection, bundle)
        self.logger.info("Event-list ingest completed: job=%s events=%s", job_name, write_result.event_rows)
        return EventListIngestResult(job_name=job_name, parsed=bundle, written=write_result, corrections=corrections)
