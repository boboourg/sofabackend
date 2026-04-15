"""Backfill job for seasonal leaderboard endpoints using seasons already stored in PostgreSQL."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from .db import AsyncpgDatabase
from .endpoints import unique_tournament_top_players_endpoint
from .limit_utils import normalize_limit
from .leaderboards_job import LeaderboardsIngestJob, LeaderboardsIngestResult
from .sport_profiles import resolve_sport_profile


@dataclass(frozen=True)
class LeaderboardsBackfillItem:
    unique_tournament_id: int
    season_id: int
    team_top_players_team_ids: tuple[int, ...]
    success: bool
    error: str | None = None
    result: LeaderboardsIngestResult | None = None


@dataclass(frozen=True)
class LeaderboardsBackfillResult:
    total_candidates: int
    processed: int
    succeeded: int
    failed: int
    items: tuple[LeaderboardsBackfillItem, ...]


class LeaderboardsBackfillJob:
    """Loads season ids from PostgreSQL and hydrates leaderboard endpoints in batches."""

    def __init__(
        self,
        ingest_job: LeaderboardsIngestJob,
        database: AsyncpgDatabase,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self.ingest_job = ingest_job
        self.database = database
        self.logger = logger or logging.getLogger(__name__)

    async def run(
        self,
        *,
        sport_slug: str = "football",
        season_limit: int | None = None,
        season_offset: int = 0,
        only_missing: bool = True,
        team_limit_per_season: int | None = None,
        include_top_players: bool = True,
        include_top_ratings: bool = True,
        include_top_players_per_game: bool = True,
        include_top_teams: bool = True,
        include_player_of_the_season_race: bool = True,
        include_player_of_the_season: bool = True,
        include_venues: bool = True,
        include_groups: bool = True,
        include_team_of_the_week: bool = True,
        include_statistics_types: bool = True,
        include_trending_top_players: bool = False,
        team_event_scopes: tuple[str, ...] = ("home", "away", "total"),
        concurrency: int = 2,
        timeout: float = 20.0,
    ) -> LeaderboardsBackfillResult:
        sport_profile = resolve_sport_profile(sport_slug)
        season_pairs = await self._load_season_pairs(
            sport_slug=sport_slug,
            limit=season_limit,
            offset=season_offset,
            only_missing=only_missing,
        )
        semaphore = asyncio.Semaphore(max(concurrency, 1))

        async def _run_one(unique_tournament_id: int, season_id: int) -> LeaderboardsBackfillItem:
            async with semaphore:
                team_ids = await self._load_team_ids_for_season(
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    limit=team_limit_per_season,
                )
                try:
                    result = await self.ingest_job.run(
                        unique_tournament_id,
                        season_id,
                        sport_slug=sport_slug,
                        include_top_players=include_top_players,
                        include_top_ratings=include_top_ratings and sport_profile.include_top_ratings,
                        include_top_players_per_game=include_top_players_per_game,
                        include_top_teams=include_top_teams,
                        include_player_of_the_season_race=(
                            include_player_of_the_season_race and sport_profile.include_player_of_the_season_race
                        ),
                        include_player_of_the_season=(
                            include_player_of_the_season and sport_profile.include_player_of_the_season
                        ),
                        include_venues=include_venues and sport_profile.include_venues,
                        include_groups=include_groups and sport_profile.include_groups,
                        include_team_of_the_week=include_team_of_the_week and sport_profile.include_team_of_the_week,
                        include_statistics_types=include_statistics_types and sport_profile.include_statistics_types,
                        include_trending_top_players=(
                            include_trending_top_players and sport_profile.include_trending_top_players
                        ),
                        team_event_scopes=(
                            team_event_scopes if sport_profile.include_team_events else ()
                        ),
                        team_top_players_team_ids=team_ids,
                        timeout=timeout,
                    )
                except Exception as exc:
                    self.logger.warning(
                        "Leaderboards backfill failed for unique_tournament_id=%s season_id=%s: %s",
                        unique_tournament_id,
                        season_id,
                        exc,
                    )
                    return LeaderboardsBackfillItem(
                        unique_tournament_id=unique_tournament_id,
                        season_id=season_id,
                        team_top_players_team_ids=team_ids,
                        success=False,
                        error=str(exc),
                    )
                return LeaderboardsBackfillItem(
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    team_top_players_team_ids=team_ids,
                    success=True,
                    result=result,
                )

        items = tuple(
            await asyncio.gather(
                *(_run_one(unique_tournament_id, season_id) for unique_tournament_id, season_id in season_pairs)
            )
        )
        succeeded = sum(1 for item in items if item.success)
        failed = len(items) - succeeded
        self.logger.info(
            "Leaderboards backfill completed: candidates=%s succeeded=%s failed=%s",
            len(season_pairs),
            succeeded,
            failed,
        )
        return LeaderboardsBackfillResult(
            total_candidates=len(season_pairs),
            processed=len(items),
            succeeded=succeeded,
            failed=failed,
            items=items,
        )

    async def _load_season_pairs(
        self,
        *,
        sport_slug: str,
        limit: int | None,
        offset: int,
        only_missing: bool,
    ) -> tuple[tuple[int, int], ...]:
        resolved_limit = normalize_limit(limit)
        sport_profile = resolve_sport_profile(sport_slug)
        top_players_suffix = sport_profile.top_players_suffix
        async with self.database.connection() as connection:
            sql = """
                SELECT DISTINCT e.unique_tournament_id, e.season_id
                FROM event AS e
                WHERE e.unique_tournament_id IS NOT NULL
                    AND e.season_id IS NOT NULL
                ORDER BY e.season_id DESC, e.unique_tournament_id
                OFFSET $1
            """
            if resolved_limit is None:
                rows = await connection.fetch(sql, offset)
            else:
                rows = await connection.fetch(f"{sql}\n                LIMIT $2", offset, resolved_limit)
            season_pairs = tuple(
                (int(row["unique_tournament_id"]), int(row["season_id"]))
                for row in rows
                if row["unique_tournament_id"] is not None and row["season_id"] is not None
            )
            if not only_missing or not season_pairs or top_players_suffix is None:
                return season_pairs
            top_players_endpoint = unique_tournament_top_players_endpoint(top_players_suffix)
            urls = tuple(
                top_players_endpoint.build_url(
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                )
                for unique_tournament_id, season_id in season_pairs
            )
            existing_rows = await connection.fetch(
                """
                SELECT source_url
                FROM api_payload_snapshot
                WHERE endpoint_pattern = $1
                  AND source_url = ANY($2::text[])
                """,
                top_players_endpoint.pattern,
                list(urls),
            )
            existing = {str(row["source_url"]) for row in existing_rows if row["source_url"] is not None}
        return tuple(
            (unique_tournament_id, season_id)
            for unique_tournament_id, season_id in season_pairs
            if top_players_endpoint.build_url(
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
            )
            not in existing
        )

    async def _load_team_ids_for_season(
        self,
        *,
        unique_tournament_id: int,
        season_id: int,
        limit: int | None,
    ) -> tuple[int, ...]:
        resolved_limit = normalize_limit(limit)
        sql = """
            SELECT seed.team_id
            FROM (
                SELECT DISTINCT e.home_team_id AS team_id
                FROM event AS e
                WHERE e.unique_tournament_id = $1
                    AND e.season_id = $2
                    AND e.home_team_id IS NOT NULL
                UNION
                SELECT DISTINCT e.away_team_id AS team_id
                FROM event AS e
                WHERE e.unique_tournament_id = $1
                    AND e.season_id = $2
                    AND e.away_team_id IS NOT NULL
            ) AS seed
            ORDER BY seed.team_id
        """
        async with self.database.connection() as connection:
            if resolved_limit is None:
                rows = await connection.fetch(sql, unique_tournament_id, season_id)
            else:
                rows = await connection.fetch(f"{sql} LIMIT $3", unique_tournament_id, season_id, resolved_limit)
        return tuple(int(row["team_id"]) for row in rows if row["team_id"] is not None)
