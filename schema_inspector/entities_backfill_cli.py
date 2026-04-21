"""CLI for backfilling entity endpoints from IDs already present in PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime

from .db import AsyncpgDatabase, load_database_config
from .entities_backfill_job import EntitiesBackfillJob
from .runtime import load_runtime_config
from .sources import build_source_adapter
from .timezone_utils import resolve_timestamp_bounds


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Backfill player/team enrichment endpoints from IDs already stored in PostgreSQL.",
    )
    parser.add_argument("--player-limit", type=int, default=None, help="Maximum number of player ids to hydrate. Defaults to no limit.")
    parser.add_argument("--player-offset", type=int, default=0, help="Offset into the player table.")
    parser.add_argument("--team-limit", type=int, default=None, help="Maximum number of team ids to hydrate. Defaults to no limit.")
    parser.add_argument("--team-offset", type=int, default=0, help="Offset into the team table.")
    parser.add_argument(
        "--player-request-limit",
        type=int,
        default=None,
        help="Maximum number of player season requests to hydrate from season_statistics_result. Defaults to no limit.",
    )
    parser.add_argument("--player-request-offset", type=int, default=0, help="Offset into player season requests.")
    parser.add_argument(
        "--team-request-limit",
        type=int,
        default=None,
        help="Maximum number of team season requests to hydrate from event seeds. Defaults to no limit.",
    )
    parser.add_argument("--team-request-offset", type=int, default=0, help="Offset into team season requests.")
    parser.add_argument(
        "--all-seeds",
        action="store_true",
        help="Process the selected rows even if matching snapshots already exist.",
    )
    parser.add_argument("--skip-player-statistics", action="store_true", help="Skip /player/{id}/statistics.")
    parser.add_argument("--skip-player-overall", action="store_true", help="Skip player season overall endpoints.")
    parser.add_argument("--skip-team-overall", action="store_true", help="Skip team season overall endpoints.")
    parser.add_argument("--skip-player-heatmaps", action="store_true", help="Skip player heatmap endpoints.")
    parser.add_argument(
        "--skip-team-performance-graphs",
        action="store_true",
        help="Skip team performance graph endpoints.",
    )
    parser.add_argument(
        "--skip-player-statistics-seasons",
        action="store_true",
        help="Skip /player/{id}/statistics/seasons.",
    )
    parser.add_argument(
        "--skip-player-transfer-history",
        action="store_true",
        help="Skip /player/{id}/transfer-history.",
    )
    parser.add_argument(
        "--skip-team-statistics-seasons",
        action="store_true",
        help="Skip /team/{id}/team-statistics/seasons.",
    )
    parser.add_argument(
        "--skip-team-player-statistics-seasons",
        action="store_true",
        help="Skip /team/{id}/player-statistics/seasons.",
    )
    parser.add_argument("--date-from", default=None, help="Optional local calendar start date in YYYY-MM-DD format.")
    parser.add_argument("--date-to", default=None, help="Optional local calendar end date in YYYY-MM-DD format.")
    parser.add_argument(
        "--timezone-name",
        default=None,
        help="IANA timezone name for date bounds. Defaults to the local system timezone.",
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
    event_timestamp_from, event_timestamp_to = resolve_timestamp_bounds(
        date_from=args.date_from,
        date_to=args.date_to,
        timezone_name=args.timezone_name,
    )
    _progress(
        "entities_backfill",
        "starting "
        f"player_limit={args.player_limit or 'all'} "
        f"team_limit={args.team_limit or 'all'} "
        f"player_request_limit={args.player_request_limit or 'all'} "
        f"team_request_limit={args.team_request_limit or 'all'} "
        f"date_from={args.date_from or '-'} "
        f"date_to={args.date_to or '-'} "
        f"only_missing={'yes' if not args.all_seeds else 'no'}",
    )

    async with AsyncpgDatabase(database_config) as database:
        adapter = build_source_adapter(
            runtime_config.source_slug,
            runtime_config=runtime_config,
        )
        ingest_job = adapter.build_entities_job(database)
        backfill_job = EntitiesBackfillJob(ingest_job, database)
        result = await backfill_job.run(
            player_limit=args.player_limit,
            player_offset=args.player_offset,
            team_limit=args.team_limit,
            team_offset=args.team_offset,
            player_request_limit=args.player_request_limit,
            player_request_offset=args.player_request_offset,
            team_request_limit=args.team_request_limit,
            team_request_offset=args.team_request_offset,
            only_missing=not args.all_seeds,
            include_player_statistics=not args.skip_player_statistics,
            include_player_overall=not args.skip_player_overall,
            include_team_overall=not args.skip_team_overall,
            include_player_heatmaps=not args.skip_player_heatmaps,
            include_team_performance_graphs=not args.skip_team_performance_graphs,
            include_player_statistics_seasons=not args.skip_player_statistics_seasons,
            include_player_transfer_history=not args.skip_player_transfer_history,
            include_team_statistics_seasons=not args.skip_team_statistics_seasons,
            include_team_player_statistics_seasons=not args.skip_team_player_statistics_seasons,
            event_timestamp_from=event_timestamp_from,
            event_timestamp_to=event_timestamp_to,
            timeout=args.timeout,
        )

    _progress(
        "entities_backfill",
        "completed "
        f"player_ids={len(result.player_ids)} "
        f"player_statistics={len(result.player_statistics_ids)} "
        f"team_ids={len(result.team_ids)} "
        f"player_overall={len(result.player_overall_requests)} "
        f"team_overall={len(result.team_overall_requests)} "
        f"player_heatmaps={len(result.player_heatmap_requests)} "
        f"team_graphs={len(result.team_performance_graph_requests)} "
        f"snapshots={result.ingest.written.payload_snapshot_rows}",
    )
    print(
        "entities_backfill "
        f"player_ids={len(result.player_ids)} "
        f"player_statistics={len(result.player_statistics_ids)} "
        f"team_ids={len(result.team_ids)} "
        f"player_overall={len(result.player_overall_requests)} "
        f"team_overall={len(result.team_overall_requests)} "
        f"player_heatmaps={len(result.player_heatmap_requests)} "
        f"team_graphs={len(result.team_performance_graph_requests)} "
        f"snapshots={result.ingest.written.payload_snapshot_rows}"
    )
    return 0


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
