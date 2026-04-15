"""Async ETL job for player/team enrichment endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Sequence

from .db import AsyncpgDatabase
from .entities_parser import (
    EntitiesBundle,
    EntitiesParser,
    PlayerHeatmapRequest,
    PlayerOverallRequest,
    TeamOverallRequest,
    TeamPerformanceGraphRequest,
)
from .entities_repository import EntitiesRepository, EntitiesWriteResult


@dataclass(frozen=True)
class EntitiesIngestResult:
    parsed: EntitiesBundle
    written: EntitiesWriteResult


class EntitiesIngestJob:
    """Fetches and persists entity/enrichment data in one transaction."""

    def __init__(
        self,
        parser: EntitiesParser,
        repository: EntitiesRepository,
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
        *,
        player_ids: Sequence[int] = (),
        player_statistics_ids: Sequence[int] = (),
        team_ids: Sequence[int] = (),
        player_overall_requests: Sequence[PlayerOverallRequest] = (),
        team_overall_requests: Sequence[TeamOverallRequest] = (),
        player_heatmap_requests: Sequence[PlayerHeatmapRequest] = (),
        team_performance_graph_requests: Sequence[TeamPerformanceGraphRequest] = (),
        include_player_statistics: bool = True,
        include_player_statistics_seasons: bool = True,
        include_player_transfer_history: bool = True,
        include_team_statistics_seasons: bool = True,
        include_team_player_statistics_seasons: bool = True,
        timeout: float = 20.0,
    ) -> EntitiesIngestResult:
        bundle = await self.parser.fetch_bundle(
            player_ids=player_ids,
            player_statistics_ids=player_statistics_ids,
            team_ids=team_ids,
            player_overall_requests=player_overall_requests,
            team_overall_requests=team_overall_requests,
            player_heatmap_requests=player_heatmap_requests,
            team_performance_graph_requests=team_performance_graph_requests,
            include_player_statistics=include_player_statistics,
            include_player_statistics_seasons=include_player_statistics_seasons,
            include_player_transfer_history=include_player_transfer_history,
            include_team_statistics_seasons=include_team_statistics_seasons,
            include_team_player_statistics_seasons=include_team_player_statistics_seasons,
            timeout=timeout,
        )

        async with self.database.transaction() as connection:
            write_result = await self.repository.upsert_bundle(connection, bundle)

        self.logger.info(
            "Entities ingest completed: players=%s teams=%s snapshots=%s",
            len(player_ids),
            len(team_ids),
            write_result.payload_snapshot_rows,
        )
        return EntitiesIngestResult(parsed=bundle, written=write_result)
