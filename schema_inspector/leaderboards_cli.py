"""CLI for loading seasonal leaderboard data into PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio
import sys

from .db import AsyncpgDatabase, load_database_config
from .leaderboards_job import LeaderboardsIngestJob
from .leaderboards_parser import LeaderboardsParser
from .leaderboards_repository import LeaderboardsRepository
from .runtime import load_runtime_config
from .sofascore_client import SofascoreClient


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Fetch seasonal leaderboard Sofascore endpoints and persist them into PostgreSQL.",
    )
    parser.add_argument("unique_tournament_id", type=int, help="Unique tournament ID from Sofascore.")
    parser.add_argument("season_id", type=int, help="Season ID from Sofascore.")
    parser.add_argument("--sport-slug", default="football", help="Sport slug used for sport-scoped endpoints.")
    parser.add_argument("--team-top-players-team-id", type=int, action="append", default=[], help="Also fetch /team/{id}/.../top-players/{scope} for these team IDs.")
    parser.add_argument("--team-of-week-period-id", type=int, action="append", default=[], help="Optional explicit period IDs for team-of-the-week.")
    parser.add_argument("--team-events-scope", action="append", default=[], help="Repeatable: home, away, total.")
    parser.add_argument("--skip-top-players", action="store_true", help="Skip /top-players/{scope}.")
    parser.add_argument("--skip-top-ratings", action="store_true", help="Skip /top-ratings/overall.")
    parser.add_argument("--skip-top-players-per-game", action="store_true", help="Skip /top-players-per-game/{scope}.")
    parser.add_argument("--skip-top-teams", action="store_true", help="Skip /top-teams/{scope}.")
    parser.add_argument("--skip-player-of-the-season-race", action="store_true", help="Skip /player-of-the-season-race.")
    parser.add_argument("--skip-player-of-the-season", action="store_true", help="Skip /player-of-the-season.")
    parser.add_argument("--skip-venues", action="store_true", help="Skip /venues.")
    parser.add_argument("--skip-groups", action="store_true", help="Skip /groups.")
    parser.add_argument("--skip-team-of-the-week", action="store_true", help="Skip team-of-the-week periods and period payloads.")
    parser.add_argument("--skip-statistics-types", action="store_true", help="Skip player/team statistics types endpoints.")
    parser.add_argument("--include-trending-top-players", action="store_true", help="Also snapshot /sport/{sport_slug}/trending-top-players.")
    parser.add_argument("--timeout", type=float, default=20.0, help="Request timeout in seconds.")
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

    team_event_scopes = args.team_events_scope or ["home", "away", "total"]

    async with AsyncpgDatabase(database_config) as database:
        client = SofascoreClient(runtime_config)
        parser = LeaderboardsParser(client)
        repository = LeaderboardsRepository()
        job = LeaderboardsIngestJob(parser, repository, database)
        result = await job.run(
            args.unique_tournament_id,
            args.season_id,
            sport_slug=args.sport_slug,
            include_top_players=not args.skip_top_players,
            include_top_ratings=not args.skip_top_ratings,
            include_top_players_per_game=not args.skip_top_players_per_game,
            include_top_teams=not args.skip_top_teams,
            include_player_of_the_season_race=not args.skip_player_of_the_season_race,
            include_player_of_the_season=not args.skip_player_of_the_season,
            include_venues=not args.skip_venues,
            include_groups=not args.skip_groups,
            include_team_of_the_week=not args.skip_team_of_the_week,
            team_of_the_week_period_ids=args.team_of_week_period_id,
            include_statistics_types=not args.skip_statistics_types,
            team_event_scopes=team_event_scopes,
            team_top_players_team_ids=args.team_top_players_team_id,
            include_trending_top_players=args.include_trending_top_players,
            timeout=args.timeout,
        )

    print(
        "leaderboards_ingest "
        f"top_player_snapshots={result.written.top_player_snapshot_rows} "
        f"top_player_entries={result.written.top_player_entry_rows} "
        f"top_team_snapshots={result.written.top_team_snapshot_rows} "
        f"top_team_entries={result.written.top_team_entry_rows} "
        f"periods={result.written.period_rows} "
        f"team_of_the_week={result.written.team_of_the_week_rows} "
        f"team_of_the_week_players={result.written.team_of_the_week_player_rows} "
        f"groups={result.written.season_group_rows} "
        f"player_of_the_season={result.written.season_player_of_the_season_rows} "
        f"venues={result.written.venue_rows} "
        f"event_rows={result.written.event_rows} "
        f"snapshots={result.written.payload_snapshot_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
