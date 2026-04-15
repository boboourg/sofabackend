"""Async ETL jobs for wide category/tournament discovery endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .category_tournaments_parser import CategoryTournamentsBundle, CategoryTournamentsParser
from .competition_repository import CompetitionRepository, CompetitionWriteResult
from .db import AsyncpgDatabase


@dataclass(frozen=True)
class CategoryTournamentsIngestResult:
    job_name: str
    parsed: CategoryTournamentsBundle
    written: CompetitionWriteResult


class CategoryTournamentsIngestJob:
    """Runs one category/tournament discovery flow and persists it transactionally."""

    def __init__(
        self,
        parser: CategoryTournamentsParser,
        repository: CompetitionRepository,
        database: AsyncpgDatabase,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.parser = parser
        self.repository = repository
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    async def run_categories_all(
        self,
        *,
        sport_slug: str = "tennis",
        timeout: float = 20.0,
    ) -> CategoryTournamentsIngestResult:
        return await self._run(
            f"categories_all:{sport_slug}",
            self.parser.fetch_categories_all(sport_slug=sport_slug, timeout=timeout),
        )

    async def run_category_unique_tournaments(
        self,
        category_id: int,
        *,
        sport_slug: str = "tennis",
        timeout: float = 20.0,
    ) -> CategoryTournamentsIngestResult:
        return await self._run(
            f"category_unique_tournaments:{category_id}",
            self.parser.fetch_category_unique_tournaments(category_id, sport_slug=sport_slug, timeout=timeout),
        )

    async def _run(self, job_name: str, bundle_awaitable) -> CategoryTournamentsIngestResult:
        bundle = await bundle_awaitable
        async with self.database.transaction() as connection:
            write_result = await self.repository.upsert_bundle(connection, bundle.competition_bundle)
        self.logger.info(
            "Category/tournament discovery completed: job=%s categories=%s tournaments=%s",
            job_name,
            len(bundle.category_ids),
            len(bundle.unique_tournament_ids),
        )
        return CategoryTournamentsIngestResult(job_name=job_name, parsed=bundle, written=write_result)
