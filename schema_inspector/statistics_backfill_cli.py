"""CLI for backfilling season-statistics endpoints from seasons already present in PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio
import sys

from .db import AsyncpgDatabase, load_database_config
from .runtime import load_runtime_config
from .sofascore_client import SofascoreClient
from .statistics_backfill_job import StatisticsBackfillJob
from .statistics_job import StatisticsIngestJob
from .statistics_parser import StatisticsParser, StatisticsQuery
from .statistics_repository import StatisticsRepository


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Backfill season-statistics endpoints from season pairs already stored in PostgreSQL.",
    )
    parser.add_argument("--season-limit", type=int, default=None, help="Maximum number of season pairs to process. Defaults to no limit.")
    parser.add_argument("--season-offset", type=int, default=0, help="Offset into distinct season pairs.")
    parser.add_argument("--timeout", type=float, default=20.0, help="Request timeout in seconds.")
    parser.add_argument("--concurrency", type=int, default=2, help="Concurrent season workers.")
    parser.add_argument("--all-seasons", action="store_true", help="Process selected seasons even if already hydrated.")
    parser.add_argument("--skip-info", action="store_true", help="Skip /statistics/info.")
    parser.add_argument("--skip-results", action="store_true", help="Skip statistics result pages and only fetch info.")
    parser.add_argument("--limit", type=int, default=None, help="Exact Sofascore statistics query param.")
    parser.add_argument("--offset", type=int, default=None, help="Exact Sofascore statistics query param.")
    parser.add_argument("--order", default=None, help="Exact Sofascore statistics query param, e.g. -rating.")
    parser.add_argument("--accumulation", default=None, help="Exact Sofascore statistics query param.")
    parser.add_argument("--group", default=None, help="Exact Sofascore statistics query param.")
    parser.add_argument("--field", action="append", default=[], help="Repeatable statistics field name.")
    parser.add_argument("--filter", action="append", default=[], help="Repeatable exact Sofascore filter expression.")
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
    query = StatisticsQuery(
        limit=args.limit,
        offset=args.offset,
        order=args.order,
        accumulation=args.accumulation,
        group=args.group,
        fields=tuple(dict.fromkeys(args.field)),
        filters=tuple(dict.fromkeys(args.filter)),
    )
    queries: tuple[StatisticsQuery, ...] | None
    if args.skip_results:
        queries = ()
    elif query.to_query_params():
        queries = (query,)
    else:
        queries = None

    async with AsyncpgDatabase(database_config) as database:
        client = SofascoreClient(runtime_config)
        parser = StatisticsParser(client)
        repository = StatisticsRepository()
        ingest_job = StatisticsIngestJob(parser, repository, database)
        backfill_job = StatisticsBackfillJob(ingest_job, database)
        result = await backfill_job.run(
            season_limit=args.season_limit,
            season_offset=args.season_offset,
            only_missing=not args.all_seasons,
            queries=queries,
            include_info=not args.skip_info,
            concurrency=args.concurrency,
            timeout=args.timeout,
        )

    print(
        "statistics_backfill "
        f"candidates={result.total_candidates} "
        f"processed={result.processed} "
        f"succeeded={result.succeeded} "
        f"failed={result.failed}"
    )
    return 0 if result.failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
