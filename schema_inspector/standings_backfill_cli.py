"""CLI for backfilling standings endpoints from seasons already present in PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio
import sys

from .db import AsyncpgDatabase, load_database_config
from .runtime import load_runtime_config
from .sofascore_client import SofascoreClient
from .standings_backfill_job import StandingsBackfillJob
from .standings_job import StandingsIngestJob
from .standings_parser import StandingsParser
from .standings_repository import StandingsRepository


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Backfill standings endpoints from season seeds already stored in PostgreSQL.",
    )
    parser.add_argument("--season-limit", type=int, default=None, help="Maximum number of standings seeds to process. Defaults to no limit.")
    parser.add_argument("--season-offset", type=int, default=0, help="Offset into distinct standings seeds.")
    parser.add_argument("--scope", action="append", default=[], help="Repeatable standing scope. Defaults to total.")
    parser.add_argument("--timeout", type=float, default=20.0, help="Request timeout in seconds.")
    parser.add_argument("--concurrency", type=int, default=2, help="Concurrent season workers.")
    parser.add_argument("--all-seasons", action="store_true", help="Process selected seasons even if already hydrated.")
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
    scopes = tuple(dict.fromkeys(args.scope or ["total"]))

    async with AsyncpgDatabase(database_config) as database:
        client = SofascoreClient(runtime_config)
        parser = StandingsParser(client)
        repository = StandingsRepository()
        ingest_job = StandingsIngestJob(parser, repository, database)
        backfill_job = StandingsBackfillJob(ingest_job, database)
        result = await backfill_job.run(
            season_limit=args.season_limit,
            season_offset=args.season_offset,
            only_missing=not args.all_seasons,
            scopes=scopes,
            concurrency=args.concurrency,
            timeout=args.timeout,
        )

    print(
        "standings_backfill "
        f"candidates={result.total_candidates} "
        f"processed={result.processed} "
        f"succeeded={result.succeeded} "
        f"failed={result.failed}"
    )
    return 0 if result.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
