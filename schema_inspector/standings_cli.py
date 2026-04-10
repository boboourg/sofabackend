"""CLI for loading Sofascore standings endpoints into PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio
import sys

from .db import AsyncpgDatabase, load_database_config
from .runtime import load_runtime_config
from .sofascore_client import SofascoreClient
from .standings_job import StandingsIngestJob
from .standings_parser import StandingsParser
from .standings_repository import StandingsRepository


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Fetch standings Sofascore endpoints and persist them into PostgreSQL.",
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--unique-tournament-id", type=int, default=None, help="Unique tournament ID.")
    source_group.add_argument("--tournament-id", type=int, default=None, help="Tournament ID.")
    parser.add_argument("--season-id", type=int, required=True, help="Season ID.")
    parser.add_argument(
        "--scope",
        action="append",
        default=[],
        help="Standing scope. Can be passed multiple times. Defaults to total.",
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
    scopes = tuple(dict.fromkeys(args.scope or ["total"]))

    async with AsyncpgDatabase(database_config) as database:
        client = SofascoreClient(runtime_config)
        parser = StandingsParser(client)
        repository = StandingsRepository()
        job = StandingsIngestJob(parser, repository, database)

        if args.unique_tournament_id is not None:
            result = await job.run_for_unique_tournament(
                args.unique_tournament_id,
                args.season_id,
                scopes=scopes,
                timeout=args.timeout,
            )
        else:
            result = await job.run_for_tournament(
                args.tournament_id,
                args.season_id,
                scopes=scopes,
                timeout=args.timeout,
            )

    print(
        "standings_ingest "
        f"source_kind={result.source_kind} "
        f"source_id={result.source_id} "
        f"season_id={result.season_id} "
        f"scopes={','.join(result.scopes)} "
        f"standings={result.written.standing_rows} "
        f"rows={result.written.standing_row_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
