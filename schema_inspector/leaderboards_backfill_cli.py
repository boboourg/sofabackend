"""CLI for backfilling seasonal leaderboard endpoints from seasons already present in PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio
import sys

from .db import AsyncpgDatabase, load_database_config
from .leaderboards_backfill_job import LeaderboardsBackfillJob
from .leaderboards_job import LeaderboardsIngestJob
from .leaderboards_parser import LeaderboardsParser
from .leaderboards_repository import LeaderboardsRepository
from .runtime import load_runtime_config
from .sofascore_client import SofascoreClient


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Backfill seasonal leaderboard endpoints from seasons already stored in PostgreSQL.",
    )
    parser.add_argument("--season-limit", type=int, default=None, help="Maximum number of season pairs to process. Defaults to no limit.")
    parser.add_argument("--season-offset", type=int, default=0, help="Offset into distinct season pairs.")
    parser.add_argument(
        "--team-limit-per-season",
        type=int,
        default=None,
        help="Optional limit for team-scoped top-player requests per season.",
    )
    parser.add_argument("--timeout", type=float, default=20.0, help="Request timeout in seconds.")
    parser.add_argument("--concurrency", type=int, default=2, help="Concurrent season workers.")
    parser.add_argument("--all-seasons", action="store_true", help="Process selected seasons even if already hydrated.")
    parser.add_argument("--skip-top-players", action="store_true", help="Skip /top-players/overall.")
    parser.add_argument("--skip-top-ratings", action="store_true", help="Skip /top-ratings/overall.")
    parser.add_argument("--skip-top-players-per-game", action="store_true", help="Skip /top-players-per-game/all/overall.")
    parser.add_argument("--skip-top-teams", action="store_true", help="Skip /top-teams/overall.")
    parser.add_argument("--skip-player-of-the-season-race", action="store_true", help="Skip /player-of-the-season-race.")
    parser.add_argument("--skip-player-of-the-season", action="store_true", help="Skip /player-of-the-season.")
    parser.add_argument("--skip-venues", action="store_true", help="Skip /venues.")
    parser.add_argument("--skip-groups", action="store_true", help="Skip /groups.")
    parser.add_argument("--skip-team-of-the-week", action="store_true", help="Skip team-of-the-week endpoints.")
    parser.add_argument("--skip-statistics-types", action="store_true", help="Skip player/team statistics types endpoints.")
    parser.add_argument("--include-trending-top-players", action="store_true", help="Also snapshot /sport/football/trending-top-players.")
    parser.add_argument("--team-events-scope", action="append", default=[], help="Repeatable: home, away, total.")
    parser.add_argument("--proxy", action="append", default=[], help="Optional proxy URL. Can be passed multiple times.")
    parser.add_argument("--user-agent", default=None, help="Override User-Agent for the transport layer.")
    parser.add_argument("--max-attempts", type=int, default=None, help="Override retry attempts for the transport.")
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL DSN. Falls back to SOFASCORE_DATABASE_URL / DATABASE_URL / POSTGRES_DSN.",
    )
    parser.add_argument("--db-min-size", type=int, default=None, help="Minimum asyncpg pool size.")
    parser.add_argument("--db-max-size", type=int, default=None, help="Maximum asyncpg pool size.")
    parser.add_argument("--db-timeout", type=float, default=None, help="asyncpg command timeout in seconds.")
    args = parser.parse_args()
    return asyncio.run(_run(args))


async def _run(args: argparse.Namespace) -> int:
    runtime_config = load_runtime_config(
        proxy_urls=args.proxy,
        user_agent=args.user_agent,
        max_attempts=args.max_attempts,
    )
    database_config = load_database_config(
        dsn=args.database_url,
        min_size=args.db_min_size,
        max_size=args.db_max_size,
        command_timeout=args.db_timeout,
    )

    team_event_scopes = tuple(args.team_events_scope or ["home", "away", "total"])

    async with AsyncpgDatabase(database_config) as database:
        client = SofascoreClient(runtime_config)
        parser = LeaderboardsParser(client)
        repository = LeaderboardsRepository()
        ingest_job = LeaderboardsIngestJob(parser, repository, database)
        backfill_job = LeaderboardsBackfillJob(ingest_job, database)
        result = await backfill_job.run(
            season_limit=args.season_limit,
            season_offset=args.season_offset,
            only_missing=not args.all_seasons,
            team_limit_per_season=args.team_limit_per_season,
            include_top_players=not args.skip_top_players,
            include_top_ratings=not args.skip_top_ratings,
            include_top_players_per_game=not args.skip_top_players_per_game,
            include_top_teams=not args.skip_top_teams,
            include_player_of_the_season_race=not args.skip_player_of_the_season_race,
            include_player_of_the_season=not args.skip_player_of_the_season,
            include_venues=not args.skip_venues,
            include_groups=not args.skip_groups,
            include_team_of_the_week=not args.skip_team_of_the_week,
            include_statistics_types=not args.skip_statistics_types,
            include_trending_top_players=args.include_trending_top_players,
            team_event_scopes=team_event_scopes,
            concurrency=args.concurrency,
            timeout=args.timeout,
        )

    print(
        "leaderboards_backfill "
        f"candidates={result.total_candidates} "
        f"processed={result.processed} "
        f"succeeded={result.succeeded} "
        f"failed={result.failed}"
    )
    return 0 if result.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
