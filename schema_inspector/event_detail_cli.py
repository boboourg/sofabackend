"""CLI for loading Sofascore event-detail endpoints into PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio
import sys

from .db import AsyncpgDatabase, load_database_config
from .event_detail_job import EventDetailIngestJob
from .event_detail_parser import EventDetailParser
from .event_detail_repository import EventDetailRepository
from .runtime import load_runtime_config
from .sofascore_client import SofascoreClient


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Fetch event-detail Sofascore endpoints and persist them into PostgreSQL.",
    )
    parser.add_argument("--event-id", type=int, required=True, help="Sofascore event id.")
    parser.add_argument(
        "--provider-id",
        type=int,
        action="append",
        default=[],
        help="Odds provider id. Can be passed multiple times. Defaults to provider 1.",
    )
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
    provider_ids = tuple(dict.fromkeys(args.provider_id or [1]))

    async with AsyncpgDatabase(database_config) as database:
        client = SofascoreClient(runtime_config)
        parser = EventDetailParser(client)
        repository = EventDetailRepository()
        job = EventDetailIngestJob(parser, repository, database)
        result = await job.run(args.event_id, provider_ids=provider_ids, timeout=args.timeout)

    print(
        "event_detail_ingest "
        f"event_id={result.event_id} "
        f"providers={','.join(str(item) for item in result.provider_ids)} "
        f"players={result.written.player_rows} "
        f"lineups={result.written.event_lineup_rows} "
        f"markets={result.written.event_market_rows} "
        f"snapshots={result.written.payload_snapshot_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
