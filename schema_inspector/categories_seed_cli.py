"""CLI for loading daily sport category seed data into PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio
import sys

from .db import AsyncpgDatabase, load_database_config
from .runtime import load_runtime_config
from .sources import build_source_adapter
from .timezone_utils import resolve_timezone_offset_seconds


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Fetch /sport/{sport_slug}/{date}/{offset}/categories and persist discovery seeds into PostgreSQL.",
    )
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format.")
    parser.add_argument("--sport-slug", default="football", help="Sport slug used in the Sofascore path.")
    parser.add_argument(
        "--timezone-offset-seconds",
        type=int,
        default=None,
        help="Explicit timezone offset in seconds for the Sofascore path segment.",
    )
    parser.add_argument(
        "--timezone-name",
        default=None,
        help="IANA timezone name used to derive the offset when --timezone-offset-seconds is omitted.",
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
    timezone_offset_seconds = resolve_timezone_offset_seconds(
        observed_date=args.date,
        timezone_name=args.timezone_name,
        timezone_offset_seconds=args.timezone_offset_seconds,
    )

    async with AsyncpgDatabase(database_config) as database:
        adapter = build_source_adapter(
            runtime_config.source_slug,
            runtime_config=runtime_config,
        )
        job = adapter.build_categories_seed_job(database)
        result = await job.run(
            args.date,
            timezone_offset_seconds,
            sport_slug=args.sport_slug,
            timeout=args.timeout,
        )

    print(
        "categories_seed_ingest "
        f"date={result.observed_date} "
        f"timezone_offset_seconds={result.timezone_offset_seconds} "
        f"categories={result.written.category_rows} "
        f"daily_summaries={result.written.daily_summary_rows} "
        f"unique_tournaments={result.written.daily_unique_tournament_rows} "
        f"teams={result.written.daily_team_rows} "
        f"snapshots={result.written.payload_snapshot_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
