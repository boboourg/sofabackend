"""CLI for loading Sofascore event-list endpoints into PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio

from .db import AsyncpgDatabase, load_database_config
from .event_list_job import EventListIngestJob
from .event_list_parser import EventListParser
from .event_list_repository import EventListRepository
from .runtime import load_runtime_config
from .sofascore_client import SofascoreClient


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch list-style Sofascore event endpoints and persist them into PostgreSQL.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    scheduled = subparsers.add_parser("scheduled", help="Load /sport/football/scheduled-events/{date}")
    scheduled.add_argument("--date", required=True, help="Date in YYYY-MM-DD format.")

    subparsers.add_parser("live", help="Load /sport/football/events/live")

    featured = subparsers.add_parser("featured", help="Load /unique-tournament/{id}/featured-events")
    featured.add_argument("--unique-tournament-id", type=int, required=True)

    round_parser = subparsers.add_parser("round", help="Load /unique-tournament/{id}/season/{id}/events/round/{round}")
    round_parser.add_argument("--unique-tournament-id", type=int, required=True)
    round_parser.add_argument("--season-id", type=int, required=True)
    round_parser.add_argument("--round-number", type=int, required=True)

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
        parser = EventListParser(client)
        repository = EventListRepository()
        job = EventListIngestJob(parser, repository, database)

        if args.command == "scheduled":
            result = await job.run_scheduled(args.date, timeout=args.timeout)
        elif args.command == "live":
            result = await job.run_live(timeout=args.timeout)
        elif args.command == "featured":
            result = await job.run_featured(args.unique_tournament_id, timeout=args.timeout)
        else:
            result = await job.run_round(
                args.unique_tournament_id,
                args.season_id,
                args.round_number,
                timeout=args.timeout,
            )

    print(
        "event_list_ingest "
        f"job={result.job_name} "
        f"events={result.written.event_rows} "
        f"tournaments={result.written.tournament_rows} "
        f"teams={result.written.team_rows} "
        f"snapshots={result.written.payload_snapshot_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
