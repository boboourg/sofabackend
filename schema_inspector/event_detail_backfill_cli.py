"""CLI for batch-hydrating event-detail endpoints from stored event ids."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from datetime import datetime

from .db import AsyncpgDatabase, load_database_config
from .event_detail_backfill_job import EventDetailBackfillJob
from .event_detail_job import EventDetailIngestJob
from .event_detail_parser import EventDetailParser
from .event_detail_repository import EventDetailRepository
from .runtime import load_runtime_config
from .sofascore_client import SofascoreClient
from .timezone_utils import resolve_timestamp_bounds


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Backfill event-detail Sofascore endpoints for event ids already present in PostgreSQL.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Maximum number of events to process. Defaults to no limit.")
    parser.add_argument("--offset", type=int, default=0, help="Offset into the event table.")
    parser.add_argument(
        "--provider-id",
        type=int,
        action="append",
        default=[],
        help="Odds provider id. Can be passed multiple times. Defaults to provider 1.",
    )
    parser.add_argument("--timeout", type=float, default=20.0, help="Request timeout in seconds.")
    parser.add_argument("--concurrency", type=int, default=3, help="Concurrent event-detail workers.")
    parser.add_argument(
        "--unique-tournament-id",
        type=int,
        default=None,
        help="Optional filter: only process events from one unique tournament.",
    )
    parser.add_argument("--date-from", default=None, help="Optional local calendar start date in YYYY-MM-DD format.")
    parser.add_argument("--date-to", default=None, help="Optional local calendar end date in YYYY-MM-DD format.")
    parser.add_argument(
        "--timezone-name",
        default=None,
        help="IANA timezone name for date bounds. Defaults to the local system timezone.",
    )
    parser.add_argument(
        "--all-events",
        action="store_true",
        help="Process all selected rows, not only events without a stored /api/v1/event/{event_id} snapshot.",
    )
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
    start_timestamp_from, start_timestamp_to = resolve_timestamp_bounds(
        date_from=args.date_from,
        date_to=args.date_to,
        timezone_name=args.timezone_name,
    )
    _progress(
        "event_detail_backfill",
        "starting "
        f"limit={args.limit or 'all'} "
        f"offset={args.offset} "
        f"only_missing={'yes' if not args.all_events else 'no'} "
        f"unique_tournament_id={args.unique_tournament_id or '-'} "
        f"date_from={args.date_from or '-'} "
        f"date_to={args.date_to or '-'} "
        f"concurrency={args.concurrency}",
    )

    async with AsyncpgDatabase(database_config) as database:
        client = SofascoreClient(runtime_config)
        parser = EventDetailParser(client)
        repository = EventDetailRepository()
        detail_job = EventDetailIngestJob(parser, repository, database)
        backfill_job = EventDetailBackfillJob(detail_job, database)
        result = await backfill_job.run(
            limit=args.limit,
            offset=args.offset,
            only_missing=not args.all_events,
            unique_tournament_id=args.unique_tournament_id,
            start_timestamp_from=start_timestamp_from,
            start_timestamp_to=start_timestamp_to,
            provider_ids=provider_ids,
            concurrency=args.concurrency,
            timeout=args.timeout,
        )

    _progress(
        "event_detail_backfill",
        "completed "
        f"candidates={result.total_candidates} "
        f"processed={result.processed} "
        f"succeeded={result.succeeded} "
        f"failed={result.failed}",
    )
    print(
        "event_detail_backfill "
        f"candidates={result.total_candidates} "
        f"processed={result.processed} "
        f"succeeded={result.succeeded} "
        f"failed={result.failed}"
    )
    return 0 if result.failed == 0 else 1


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
