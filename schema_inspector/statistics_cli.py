"""CLI for loading Sofascore season-statistics endpoints into PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio
import sys

from .db import AsyncpgDatabase, load_database_config
from .runtime import load_runtime_config
from .statistics_parser import StatisticsQuery
from .sources import build_source_adapter


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Fetch season-statistics Sofascore endpoints and persist them into PostgreSQL.",
    )
    parser.add_argument("unique_tournament_id", type=int, help="Unique tournament ID from Sofascore.")
    parser.add_argument("season_id", type=int, help="Season ID from Sofascore.")
    parser.add_argument("--timeout", type=float, default=20.0, help="Request timeout in seconds.")
    parser.add_argument("--proxy", action="append", default=[], help="Optional proxy URL. Can be passed multiple times.")
    parser.add_argument("--user-agent", default=None, help="Override User-Agent for the transport layer.")
    parser.add_argument("--max-attempts", type=int, default=None, help="Override retry attempts for the transport.")
    parser.add_argument("--skip-info", action="store_true", help="Skip /statistics/info and only fetch query results.")
    parser.add_argument("--limit", type=int, default=None, help="Exact Sofascore statistics query param.")
    parser.add_argument("--offset", type=int, default=None, help="Exact Sofascore statistics query param.")
    parser.add_argument("--order", default=None, help="Exact Sofascore statistics query param, e.g. -rating.")
    parser.add_argument("--accumulation", default=None, help="Exact Sofascore statistics query param.")
    parser.add_argument("--group", default=None, help="Exact Sofascore statistics query param.")
    parser.add_argument("--field", action="append", default=[], help="Repeatable statistics field name.")
    parser.add_argument("--filter", action="append", default=[], help="Repeatable exact Sofascore filter expression.")
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

    query = StatisticsQuery(
        limit=args.limit,
        offset=args.offset,
        order=args.order,
        accumulation=args.accumulation,
        group=args.group,
        fields=tuple(dict.fromkeys(args.field)),
        filters=tuple(dict.fromkeys(args.filter)),
    )
    queries = (query,) if query.to_query_params() else ()

    async with AsyncpgDatabase(database_config) as database:
        adapter = build_source_adapter(
            runtime_config.source_slug,
            runtime_config=runtime_config,
        )
        job = adapter.build_statistics_job(database)
        result = await job.run(
            args.unique_tournament_id,
            args.season_id,
            queries=queries,
            include_info=not args.skip_info,
            timeout=args.timeout,
        )

    print(
        "statistics_ingest "
        f"unique_tournament_id={result.unique_tournament_id} "
        f"season_id={result.season_id} "
        f"queries={len(result.queries)} "
        f"configs={result.written.config_rows} "
        f"snapshots={result.written.snapshot_rows} "
        f"results={result.written.result_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
