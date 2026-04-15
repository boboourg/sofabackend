"""CLI for scheduled-tournament discovery endpoints."""

from __future__ import annotations

import argparse
import asyncio
import sys

from .db import AsyncpgDatabase, load_database_config
from .event_list_repository import EventListRepository
from .runtime import load_runtime_config
from .scheduled_tournaments_job import ScheduledTournamentsIngestJob
from .scheduled_tournaments_parser import ScheduledTournamentsParser
from .sofascore_client import SofascoreClient


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Load /sport/{sport_slug}/scheduled-tournaments/{date}/page/{page} into PostgreSQL.",
    )
    parser.add_argument("--sport-slug", default="basketball", help="Sport slug used for discovery.")
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format.")
    parser.add_argument("--page", type=int, default=1, help="Page number for scheduled-tournaments.")
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

    async with AsyncpgDatabase(database_config) as database:
        client = SofascoreClient(runtime_config)
        parser = ScheduledTournamentsParser(client)
        job = ScheduledTournamentsIngestJob(parser, EventListRepository(), database)
        result = await job.run_page(
            args.date,
            args.page,
            sport_slug=args.sport_slug,
            timeout=args.timeout,
        )

    print(
        "scheduled_tournaments_ingest "
        f"job={result.job_name} "
        f"tournaments={len(result.parsed.tournament_ids)} "
        f"unique_tournaments={len(result.parsed.unique_tournament_ids)} "
        f"has_next_page={result.parsed.has_next_page} "
        f"snapshots={result.written.payload_snapshot_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
