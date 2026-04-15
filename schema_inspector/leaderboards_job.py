"""Async ETL job for seasonal leaderboard endpoints."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Sequence

from .db import AsyncpgDatabase
from .leaderboards_parser import LeaderboardsBundle, LeaderboardsParser
from .leaderboards_repository import LeaderboardsRepository, LeaderboardsWriteResult


@dataclass(frozen=True)
class LeaderboardsIngestResult:
    unique_tournament_id: int
    season_id: int
    parsed: LeaderboardsBundle
    written: LeaderboardsWriteResult


class LeaderboardsIngestJob:
    """Fetches and persists leaderboard data in one transaction."""

    def __init__(
        self,
        parser: LeaderboardsParser,
        repository: LeaderboardsRepository,
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
        sport_slug: str = "football",
        include_top_players: bool = True,
        include_top_ratings: bool = True,
        include_top_players_per_game: bool = True,
        include_top_teams: bool = True,
        include_player_of_the_season_race: bool = True,
        include_player_of_the_season: bool = True,
        include_venues: bool = True,
        include_groups: bool = True,
        include_team_of_the_week: bool = True,
        team_of_the_week_period_ids: Sequence[int] = (),
        include_statistics_types: bool = True,
        team_event_scopes: Sequence[str] = ("home", "away", "total"),
        team_top_players_team_ids: Sequence[int] = (),
        include_trending_top_players: bool = False,
        timeout: float = 20.0,
    ) -> LeaderboardsIngestResult:
        bundle = await self.parser.fetch_bundle(
            unique_tournament_id,
            season_id,
            sport_slug=sport_slug,
            include_top_players=include_top_players,
            include_top_ratings=include_top_ratings,
            include_top_players_per_game=include_top_players_per_game,
            include_top_teams=include_top_teams,
            include_player_of_the_season_race=include_player_of_the_season_race,
            include_player_of_the_season=include_player_of_the_season,
            include_venues=include_venues,
            include_groups=include_groups,
            include_team_of_the_week=include_team_of_the_week,
            team_of_the_week_period_ids=team_of_the_week_period_ids,
            include_statistics_types=include_statistics_types,
            team_event_scopes=team_event_scopes,
            team_top_players_team_ids=team_top_players_team_ids,
            include_trending_top_players=include_trending_top_players,
            timeout=timeout,
        )

        async with self.database.transaction() as connection:
            write_result = await self.repository.upsert_bundle(connection, bundle)

        self.logger.info(
            "Leaderboards ingest completed: unique_tournament_id=%s season_id=%s snapshots=%s",
            unique_tournament_id,
            season_id,
            write_result.payload_snapshot_rows,
        )
        return LeaderboardsIngestResult(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            parsed=bundle,
            written=write_result,
        )
