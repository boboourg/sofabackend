"""Async ETL job for the daily categories seed endpoint."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .categories_seed_parser import CategoriesSeedBundle, CategoriesSeedParser
from .categories_seed_repository import CategoriesSeedRepository, CategoriesSeedWriteResult
from .db import AsyncpgDatabase


@dataclass(frozen=True)
class CategoriesSeedIngestResult:
    observed_date: str
    timezone_offset_seconds: int
    parsed: CategoriesSeedBundle
    written: CategoriesSeedWriteResult


class CategoriesSeedIngestJob:
    """Fetches and persists categories discovery data in one transaction."""

    def __init__(
        self,
        parser: CategoriesSeedParser,
        repository: CategoriesSeedRepository,
        database: AsyncpgDatabase,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.parser = parser
        self.repository = repository
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    async def run(
        self,
        observed_date: str,
        timezone_offset_seconds: int,
        *,
        sport_slug: str = "football",
        timeout: float = 20.0,
    ) -> CategoriesSeedIngestResult:
        bundle = await self.parser.fetch_daily_categories(
            observed_date,
            timezone_offset_seconds,
            sport_slug=sport_slug,
            timeout=timeout,
        )
        async with self.database.transaction() as connection:
            write_result = await self.repository.upsert_bundle(connection, bundle)

        self.logger.info(
            "Categories seed ingest completed: date=%s offset=%s categories=%s",
            observed_date,
            timezone_offset_seconds,
            write_result.category_rows,
        )
        return CategoriesSeedIngestResult(
            observed_date=observed_date,
            timezone_offset_seconds=timezone_offset_seconds,
            parsed=bundle,
            written=write_result,
        )
