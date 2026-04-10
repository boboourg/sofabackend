"""Async ETL job for standings endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Literal

from .db import AsyncpgDatabase
from .standings_parser import StandingsBundle, StandingsParser
from .standings_repository import StandingsRepository, StandingsWriteResult


@dataclass(frozen=True)
class StandingsIngestResult:
    source_kind: Literal["unique_tournament", "tournament"]
    source_id: int
    season_id: int
    scopes: tuple[str, ...]
    parsed: StandingsBundle
    written: StandingsWriteResult


class StandingsIngestJob:
    """Runs one standings parser flow and persists it transactionally."""

    def __init__(
        self,
        parser: StandingsParser,
        repository: StandingsRepository,
        database: AsyncpgDatabase,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.parser = parser
        self.repository = repository
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    async def run_for_unique_tournament(
        self,
        unique_tournament_id: int,
        season_id: int,
        *,
        scopes: tuple[str, ...] = ("total",),
        timeout: float = 20.0,
    ) -> StandingsIngestResult:
        bundle = await self.parser.fetch_unique_tournament_standings(
            unique_tournament_id,
            season_id,
            scopes=scopes,
            timeout=timeout,
        )
        async with self.database.transaction() as connection:
            write_result = await self.repository.upsert_bundle(connection, bundle)
        self.logger.info(
            "Standings ingest completed: unique_tournament_id=%s season_id=%s standings=%s rows=%s",
            unique_tournament_id,
            season_id,
            write_result.standing_rows,
            write_result.standing_row_rows,
        )
        return StandingsIngestResult(
            source_kind="unique_tournament",
            source_id=unique_tournament_id,
            season_id=season_id,
            scopes=scopes,
            parsed=bundle,
            written=write_result,
        )

    async def run_for_tournament(
        self,
        tournament_id: int,
        season_id: int,
        *,
        scopes: tuple[str, ...] = ("total",),
        timeout: float = 20.0,
    ) -> StandingsIngestResult:
        bundle = await self.parser.fetch_tournament_standings(
            tournament_id,
            season_id,
            scopes=scopes,
            timeout=timeout,
        )
        async with self.database.transaction() as connection:
            write_result = await self.repository.upsert_bundle(connection, bundle)
        self.logger.info(
            "Standings ingest completed: tournament_id=%s season_id=%s standings=%s rows=%s",
            tournament_id,
            season_id,
            write_result.standing_rows,
            write_result.standing_row_rows,
        )
        return StandingsIngestResult(
            source_kind="tournament",
            source_id=tournament_id,
            season_id=season_id,
            scopes=scopes,
            parsed=bundle,
            written=write_result,
        )
