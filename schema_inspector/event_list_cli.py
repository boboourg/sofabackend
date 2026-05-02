"""CLI for loading Sofascore event-list endpoints into PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from types import SimpleNamespace

from .db import AsyncpgDatabase, load_database_config
from .runtime import load_runtime_config
from .sources import build_source_adapter

_TEAM_LAST_ENDPOINT_PATTERN = "/api/v1/team/{team_id}/events/last/{page}"
_TEAM_NEXT_ENDPOINT_PATTERN = "/api/v1/team/{team_id}/events/next/{page}"
logger = logging.getLogger(__name__)


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Fetch list-style Sofascore event endpoints and persist them into PostgreSQL.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    parser.add_argument("--sport-slug", default="football", help="Sport slug used for scheduled/live discovery.")

    scheduled = subparsers.add_parser("scheduled", help="Load /sport/{sport_slug}/scheduled-events/{date}")
    scheduled.add_argument("--date", required=True, help="Date in YYYY-MM-DD format.")

    subparsers.add_parser("live", help="Load /sport/{sport_slug}/events/live")

    featured = subparsers.add_parser("featured", help="Load /unique-tournament/{id}/featured-events")
    featured.add_argument("--unique-tournament-id", type=int, required=True)

    tournament_scheduled = subparsers.add_parser(
        "tournament-scheduled",
        help="Load /unique-tournament/{id}/scheduled-events/{date}",
    )
    tournament_scheduled.add_argument("--unique-tournament-id", type=int, required=True)
    tournament_scheduled.add_argument("--date", required=True, help="Date in YYYY-MM-DD format.")

    round_parser = subparsers.add_parser("round", help="Load /unique-tournament/{id}/season/{id}/events/round/{round}")
    round_parser.add_argument("--unique-tournament-id", type=int, required=True)
    round_parser.add_argument("--season-id", type=int, required=True)
    round_parser.add_argument("--round-number", type=int, required=True)

    team_last = subparsers.add_parser("team-last", help="Load /team/{id}/events/last/{page}")
    team_last.add_argument("--team-id", type=int, required=True)
    team_last.add_argument("--page", type=int, default=0)

    team_next = subparsers.add_parser("team-next", help="Load /team/{id}/events/next/{page}")
    team_next.add_argument("--team-id", type=int, required=True)
    team_next.add_argument("--page", type=int, default=0)
    team_next.add_argument("--until-end", action="store_true", help="Continue pages until hasNextPage=false.")
    team_next.add_argument("--max-pages", type=int, default=50, help="Safety cap for --until-end pagination.")

    team_surfaces = subparsers.add_parser(
        "team-surfaces",
        help="Load team last/next surfaces. Full crawl once, then refresh page 0 only after completion.",
    )
    team_surfaces.add_argument("--team-id", type=int, required=True)
    team_surfaces.add_argument("--max-pages", type=int, default=250, help="Safety cap for initial pagination.")

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
        adapter = build_source_adapter(
            runtime_config.source_slug,
            runtime_config=runtime_config,
        )
        job = adapter.build_event_list_job(database)

        if args.command == "scheduled":
            result = await job.run_scheduled(args.date, sport_slug=args.sport_slug, timeout=args.timeout)
        elif args.command == "live":
            result = await job.run_live(sport_slug=args.sport_slug, timeout=args.timeout)
        elif args.command == "featured":
            result = await job.run_featured(
                args.unique_tournament_id,
                sport_slug=args.sport_slug,
                timeout=args.timeout,
            )
        elif args.command == "tournament-scheduled":
            result = await job.run_unique_tournament_scheduled(
                args.unique_tournament_id,
                args.date,
                sport_slug=args.sport_slug,
                timeout=args.timeout,
            )
        elif args.command == "round":
            result = await job.run_round(
                args.unique_tournament_id,
                args.season_id,
                args.round_number,
                sport_slug=args.sport_slug,
                timeout=args.timeout,
            )
        elif args.command == "team-last":
            result = await job.run_team_last(
                args.team_id,
                args.page,
                sport_slug=args.sport_slug,
                timeout=args.timeout,
            )
        elif args.command == "team-next":
            if args.until_end:
                result = await _run_paginated(
                    job.run_team_next,
                    args.team_id,
                    start_page=args.page,
                    max_pages=args.max_pages,
                    sport_slug=args.sport_slug,
                    timeout=args.timeout,
                )
            else:
                result = await job.run_team_next(
                    args.team_id,
                    args.page,
                    sport_slug=args.sport_slug,
                    timeout=args.timeout,
                )
        elif args.command == "team-surfaces":
            result = await _run_team_surfaces(
                database,
                job,
                team_id=args.team_id,
                max_pages=args.max_pages,
                sport_slug=args.sport_slug,
                timeout=args.timeout,
            )
        else:  # pragma: no cover - argparse prevents this branch.
            raise RuntimeError(f"Unsupported event-list command: {args.command}")

    print(
        "event_list_ingest "
        f"job={result.job_name} "
        f"events={result.written.event_rows} "
        f"tournaments={result.written.tournament_rows} "
        f"teams={result.written.team_rows} "
        f"snapshots={result.written.payload_snapshot_rows}"
    )
    return 0


async def _run_team_surfaces(
    database,
    job,
    *,
    team_id: int,
    max_pages: int,
    sport_slug: str,
    timeout: float,
):
    if await _has_complete_team_event_surfaces(database, team_id=team_id):
        results = [
            await job.run_team_last(team_id, 0, sport_slug=sport_slug, timeout=timeout),
            await job.run_team_next(team_id, 0, sport_slug=sport_slug, timeout=timeout),
        ]
    else:
        results = []
        results.extend(
            await _run_paginated_collect(
                job.run_team_last,
                team_id,
                start_page=0,
                max_pages=max_pages,
                sport_slug=sport_slug,
                timeout=timeout,
            )
        )
        results.extend(
            await _run_paginated_collect(
                job.run_team_next,
                team_id,
                start_page=0,
                max_pages=max_pages,
                sport_slug=sport_slug,
                timeout=timeout,
            )
        )
    return _combine_results(f"team_surfaces:{team_id}", results)


async def _run_paginated(
    runner,
    *runner_args,
    start_page: int,
    max_pages: int,
    sport_slug: str,
    timeout: float,
):
    result = None
    for page in range(start_page, start_page + max_pages):
        result = await runner(
            *runner_args,
            page,
            sport_slug=sport_slug,
            timeout=timeout,
        )
        if not _result_has_next_page(result):
            return result
    return result


async def _run_paginated_collect(
    runner,
    *runner_args,
    start_page: int,
    max_pages: int,
    sport_slug: str,
    timeout: float,
):
    results = []
    for page in range(start_page, start_page + max_pages):
        result = await runner(
            *runner_args,
            page,
            sport_slug=sport_slug,
            timeout=timeout,
        )
        results.append(result)
        if not _result_has_next_page(result):
            break
    return results


async def _has_complete_team_event_surfaces(database, *, team_id: int) -> bool:
    connection_factory = getattr(database, "connection", None)
    if not callable(connection_factory):
        return False
    try:
        async with connection_factory() as connection:
            row = await connection.fetchrow(
                """
                SELECT
                    EXISTS (
                        SELECT 1
                        FROM api_payload_snapshot
                        WHERE endpoint_pattern = $1
                          AND context_entity_type = 'team'
                          AND context_entity_id = $3
                          AND payload->>'hasNextPage' = 'false'
                    ) AS last_complete,
                    EXISTS (
                        SELECT 1
                        FROM api_payload_snapshot
                        WHERE endpoint_pattern = $2
                          AND context_entity_type = 'team'
                          AND context_entity_id = $3
                          AND payload->>'hasNextPage' = 'false'
                    ) AS next_complete
                """,
                _TEAM_LAST_ENDPOINT_PATTERN,
                _TEAM_NEXT_ENDPOINT_PATTERN,
                int(team_id),
            )
    except Exception as exc:
        logger.warning("team surface completion check failed team=%s: %s", team_id, exc)
        return False
    return bool(row and row["last_complete"] and row["next_complete"])


def _combine_results(job_name: str, results):
    written = SimpleNamespace(
        event_rows=sum(int(getattr(result.written, "event_rows", 0)) for result in results),
        tournament_rows=sum(int(getattr(result.written, "tournament_rows", 0)) for result in results),
        team_rows=sum(int(getattr(result.written, "team_rows", 0)) for result in results),
        payload_snapshot_rows=sum(int(getattr(result.written, "payload_snapshot_rows", 0)) for result in results),
    )
    parsed = getattr(results[-1], "parsed", None) if results else SimpleNamespace(payload_snapshots=())
    return SimpleNamespace(job_name=job_name, parsed=parsed, written=written)


def _result_has_next_page(result) -> bool:
    snapshots = getattr(getattr(result, "parsed", None), "payload_snapshots", ())
    for snapshot in snapshots:
        payload = getattr(snapshot, "payload", None)
        if isinstance(payload, dict):
            return bool(payload.get("hasNextPage"))
    return False


if __name__ == "__main__":
    raise SystemExit(main())
