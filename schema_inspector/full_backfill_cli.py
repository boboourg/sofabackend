"""One-shot backfill for all currently implemented sports-data families over existing PostgreSQL seeds."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime

from .db import AsyncpgDatabase, load_database_config
from .entities_backfill_job import EntitiesBackfillJob
from .entities_job import EntitiesIngestJob
from .entities_parser import EntitiesParser
from .entities_repository import EntitiesRepository
from .event_detail_backfill_job import EventDetailBackfillJob
from .event_detail_job import EventDetailIngestJob
from .event_detail_parser import EventDetailParser
from .event_detail_repository import EventDetailRepository
from .leaderboards_backfill_job import LeaderboardsBackfillJob
from .leaderboards_job import LeaderboardsIngestJob
from .leaderboards_parser import LeaderboardsParser
from .leaderboards_repository import LeaderboardsRepository
from .runtime import load_runtime_config
from .sofascore_client import SofascoreClient
from .standings_backfill_job import StandingsBackfillJob
from .standings_job import StandingsIngestJob
from .standings_parser import StandingsParser
from .standings_repository import StandingsRepository
from .statistics_backfill_job import StatisticsBackfillJob
from .statistics_job import StatisticsIngestJob
from .statistics_parser import StatisticsParser, StatisticsQuery
from .statistics_repository import StatisticsRepository


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Backfill all implemented sports-data families using IDs and seasons already stored in PostgreSQL.",
    )
    parser.add_argument("--event-limit", type=int, default=None, help="Maximum number of event_detail candidates. Defaults to no limit.")
    parser.add_argument("--event-offset", type=int, default=0, help="Offset into event_detail candidates.")
    parser.add_argument("--event-concurrency", type=int, default=3, help="Concurrent event_detail workers.")
    parser.add_argument("--statistics-limit", type=int, default=None, help="Maximum number of statistics season pairs. Defaults to no limit.")
    parser.add_argument("--statistics-offset", type=int, default=0, help="Offset into statistics season pairs.")
    parser.add_argument("--statistics-concurrency", type=int, default=2, help="Concurrent statistics workers.")
    parser.add_argument("--statistics-query-limit", type=int, default=20, help="Default statistics page size.")
    parser.add_argument("--statistics-query-offset", type=int, default=0, help="Default statistics page offset.")
    parser.add_argument("--player-limit", type=int, default=None, help="Maximum number of player entity seeds. Defaults to no limit.")
    parser.add_argument("--team-limit", type=int, default=None, help="Maximum number of team entity seeds. Defaults to no limit.")
    parser.add_argument(
        "--player-request-limit",
        type=int,
        default=None,
        help="Maximum number of player season requests for entities backfill. Defaults to no limit.",
    )
    parser.add_argument(
        "--team-request-limit",
        type=int,
        default=None,
        help="Maximum number of team season requests for entities backfill. Defaults to no limit.",
    )
    parser.add_argument("--season-limit", type=int, default=None, help="Maximum number of leaderboard season pairs. Defaults to no limit.")
    parser.add_argument("--season-offset", type=int, default=0, help="Offset into leaderboard season pairs.")
    parser.add_argument("--leaderboards-concurrency", type=int, default=2, help="Concurrent leaderboard workers.")
    parser.add_argument("--standings-limit", type=int, default=None, help="Maximum number of standings season seeds. Defaults to no limit.")
    parser.add_argument("--standings-offset", type=int, default=0, help="Offset into standings season seeds.")
    parser.add_argument("--standings-concurrency", type=int, default=2, help="Concurrent standings workers.")
    parser.add_argument("--standings-scope", action="append", default=[], help="Repeatable standings scope.")
    parser.add_argument("--team-limit-per-season", type=int, default=None, help="Optional team cap per leaderboard season.")
    parser.add_argument(
        "--provider-id",
        type=int,
        action="append",
        default=[],
        help="Odds provider id for event_detail. Can be repeated. Defaults to provider 1.",
    )
    parser.add_argument(
        "--all-seeds",
        action="store_true",
        help="Process selected seeds even if matching snapshots already exist.",
    )
    parser.add_argument("--timeout", type=float, default=20.0, help="Request timeout in seconds.")
    parser.add_argument("--proxy", action="append", default=[], help="Optional proxy URL. Can be passed multiple times.")
    parser.add_argument("--user-agent", default=None, help="Override User-Agent for the transport layer.")
    parser.add_argument("--max-attempts", type=int, default=None, help="Override retry attempts for the transport.")
    parser.add_argument("--log-level", default="INFO", help="Python log level, e.g. INFO or DEBUG.")
    parser.add_argument(
        "--database-url",
        default=None,
        help="PostgreSQL DSN. Falls back to SOFASCORE_DATABASE_URL / DATABASE_URL / POSTGRES_DSN.",
    )
    parser.add_argument("--db-min-size", type=int, default=None, help="Minimum asyncpg pool size.")
    parser.add_argument("--db-max-size", type=int, default=None, help="Maximum asyncpg pool size.")
    parser.add_argument("--db-timeout", type=float, default=None, help="asyncpg command timeout in seconds.")
    args = parser.parse_args()
    _configure_logging(args.log_level)
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
    provider_ids = tuple(dict.fromkeys(args.provider_id or [1]))
    statistics_queries = (
        StatisticsQuery(limit=args.statistics_query_limit, offset=args.statistics_query_offset),
    )
    standings_scopes = tuple(dict.fromkeys(args.standings_scope or ["total"]))

    async with AsyncpgDatabase(database_config) as database:
        client = SofascoreClient(runtime_config)

        event_detail_job = EventDetailIngestJob(EventDetailParser(client), EventDetailRepository(), database)
        event_detail_backfill = EventDetailBackfillJob(event_detail_job, database)

        statistics_job = StatisticsIngestJob(StatisticsParser(client), StatisticsRepository(), database)
        statistics_backfill = StatisticsBackfillJob(statistics_job, database)

        entities_job = EntitiesIngestJob(EntitiesParser(client), EntitiesRepository(), database)
        entities_backfill = EntitiesBackfillJob(entities_job, database)

        standings_job = StandingsIngestJob(StandingsParser(client), StandingsRepository(), database)
        standings_backfill = StandingsBackfillJob(standings_job, database)

        leaderboards_job = LeaderboardsIngestJob(LeaderboardsParser(client), LeaderboardsRepository(), database)
        leaderboards_backfill = LeaderboardsBackfillJob(leaderboards_job, database)

        _progress(
            "full_backfill",
            "starting "
            f"event_limit={args.event_limit or 'all'} "
            f"statistics_limit={args.statistics_limit or 'all'} "
            f"player_limit={args.player_limit or 'all'} "
            f"team_limit={args.team_limit or 'all'} "
            f"player_request_limit={args.player_request_limit or 'all'} "
            f"team_request_limit={args.team_request_limit or 'all'} "
            f"standings_limit={args.standings_limit or 'all'} "
            f"leaderboards_limit={args.season_limit or 'all'}",
        )
        _progress("event_detail", "starting")
        event_result = await event_detail_backfill.run(
            limit=args.event_limit,
            offset=args.event_offset,
            only_missing=not args.all_seeds,
            provider_ids=provider_ids,
            concurrency=args.event_concurrency,
            timeout=args.timeout,
        )
        _progress(
            "event_detail",
            f"completed succeeded={event_result.succeeded}/{event_result.processed} failed={event_result.failed}",
        )
        _progress("statistics", "starting")
        statistics_result = await statistics_backfill.run(
            season_limit=args.statistics_limit,
            season_offset=args.statistics_offset,
            only_missing=not args.all_seeds,
            queries=statistics_queries,
            concurrency=args.statistics_concurrency,
            timeout=args.timeout,
        )
        _progress(
            "statistics",
            f"completed succeeded={statistics_result.succeeded}/{statistics_result.processed} failed={statistics_result.failed}",
        )
        _progress("entities", "starting")
        entities_result = await entities_backfill.run(
            player_limit=args.player_limit,
            team_limit=args.team_limit,
            player_request_limit=args.player_request_limit,
            team_request_limit=args.team_request_limit,
            only_missing=not args.all_seeds,
            timeout=args.timeout,
        )
        _progress(
            "entities",
            "completed "
            f"players={len(entities_result.player_ids)} "
            f"teams={len(entities_result.team_ids)} "
            f"player_overall={len(entities_result.player_overall_requests)} "
            f"team_overall={len(entities_result.team_overall_requests)}",
        )
        _progress("standings", "starting")
        standings_result = await standings_backfill.run(
            season_limit=args.standings_limit,
            season_offset=args.standings_offset,
            only_missing=not args.all_seeds,
            scopes=standings_scopes,
            concurrency=args.standings_concurrency,
            timeout=args.timeout,
        )
        _progress(
            "standings",
            f"completed succeeded={standings_result.succeeded}/{standings_result.processed} failed={standings_result.failed}",
        )
        _progress("leaderboards", "starting")
        leaderboards_result = await leaderboards_backfill.run(
            season_limit=args.season_limit,
            season_offset=args.season_offset,
            only_missing=not args.all_seeds,
            team_limit_per_season=args.team_limit_per_season,
            concurrency=args.leaderboards_concurrency,
            timeout=args.timeout,
        )
        _progress(
            "leaderboards",
            f"completed succeeded={leaderboards_result.succeeded}/{leaderboards_result.processed} failed={leaderboards_result.failed}",
        )

    print(
        "full_backfill "
        f"event_detail_succeeded={event_result.succeeded}/{event_result.processed} "
        f"statistics_succeeded={statistics_result.succeeded}/{statistics_result.processed} "
        f"entities_players={len(entities_result.player_ids)} "
        f"entities_teams={len(entities_result.team_ids)} "
        f"standings_succeeded={standings_result.succeeded}/{standings_result.processed} "
        f"leaderboards_succeeded={leaderboards_result.succeeded}/{leaderboards_result.processed}"
    )
    return (
        0
        if event_result.failed == 0
        and statistics_result.failed == 0
        and standings_result.failed == 0
        and leaderboards_result.failed == 0
        else 1
    )


def _progress(stage: str, message: str) -> None:
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    print(f"[{timestamp}] {stage}: {message}", flush=True)


def _configure_logging(level_name: str) -> None:
    level = getattr(logging, str(level_name).upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="[%(asctime)s] %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
        stream=sys.stdout,
    )


if __name__ == "__main__":
    raise SystemExit(main())
