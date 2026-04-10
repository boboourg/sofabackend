"""Async ETL job for season-statistics endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Iterable

from .db import AsyncpgDatabase
from .statistics_parser import StatisticsBundle, StatisticsParser, StatisticsQuery
from .statistics_repository import StatisticsRepository, StatisticsWriteResult


@dataclass(frozen=True)
class StatisticsIngestResult:
    unique_tournament_id: int
    season_id: int
    queries: tuple[StatisticsQuery, ...]
    parsed: StatisticsBundle
    written: StatisticsWriteResult


class StatisticsIngestJob:
    """Runs one season-statistics parser flow and persists it transactionally."""

    def __init__(
        self,
        parser: StatisticsParser,
        repository: StatisticsRepository,
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
        unique_tournament_id: int,
        season_id: int,
        *,
        queries: Iterable[StatisticsQuery] = (),
        include_info: bool = True,
        timeout: float = 20.0,
    ) -> StatisticsIngestResult:
        resolved_queries = tuple(queries)
        bundle = await self.parser.fetch_bundle(
            unique_tournament_id,
            season_id,
            queries=resolved_queries,
            include_info=include_info,
            timeout=timeout,
        )
        async with self.database.transaction() as connection:
            write_result = await self.repository.upsert_bundle(connection, bundle)
        self.logger.info(
            "Statistics ingest completed: unique_tournament_id=%s season_id=%s snapshots=%s results=%s",
            unique_tournament_id,
            season_id,
            write_result.snapshot_rows,
            write_result.result_rows,
        )
        return StatisticsIngestResult(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            queries=resolved_queries,
            parsed=bundle,
            written=write_result,
        )
