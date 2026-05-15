"""Async ETL job for competition-family endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from .competition_parser import CompetitionBundle, CompetitionParser
from .competition_repository import CompetitionRepository, CompetitionWriteResult
from .db import AsyncpgDatabase


@dataclass(frozen=True)
class CompetitionIngestResult:
    unique_tournament_id: int
    season_id: int | None
    parsed: CompetitionBundle
    written: CompetitionWriteResult


class CompetitionIngestJob:
    """Fetches and persists competition data in one transaction."""

    def __init__(
        self,
        parser: CompetitionParser,
        repository: CompetitionRepository,
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
        *,
        season_id: int | None = None,
        include_seasons: bool = True,
        include_season_rounds: bool | None = None,
        timeout: float = 20.0,
    ) -> CompetitionIngestResult:
        # Task 3 (2026-05-15): when season_id is provided we now also
        # fetch /season/{s}/rounds as part of the same competition
        # ingest pass. Default mirrors include_season_info: on when a
        # season_id is known, off otherwise. Caller can override.
        bundle = await self.parser.fetch_bundle(
            unique_tournament_id,
            season_id=season_id,
            include_seasons=include_seasons,
            include_season_info=season_id is not None,
            include_season_rounds=(
                include_season_rounds
                if include_season_rounds is not None
                else season_id is not None
            ),
            timeout=timeout,
        )

        async with self.database.transaction() as connection:
            write_result = await self.repository.upsert_bundle(connection, bundle)

        self.logger.info(
            "Competition ingest completed: unique_tournament_id=%s season_id=%s snapshots=%s",
            unique_tournament_id,
            season_id,
            write_result.payload_snapshot_rows,
        )
        return CompetitionIngestResult(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            parsed=bundle,
            written=write_result,
        )
