"""Bootstrap pipeline that starts from daily categories discovery."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime
from datetime import timedelta

from .categories_seed_job import CategoriesSeedIngestJob
from .categories_seed_parser import CategoriesSeedParser
from .categories_seed_repository import CategoriesSeedRepository
from .competition_job import CompetitionIngestJob
from .competition_parser import CompetitionParser
from .competition_repository import CompetitionRepository
from .db import AsyncpgDatabase, load_database_config
from .event_list_job import EventListIngestJob
from .event_list_parser import EventListParser
from .event_list_repository import EventListRepository
from .runtime import load_runtime_config
from .sofascore_client import SofascoreClient
from .timezone_utils import resolve_timezone_offset_seconds


@dataclass(frozen=True)
class _CompetitionBootstrapItem:
    unique_tournament_id: int
    success: bool
    error: str | None = None


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description=(
            "Bootstrap the local Sofascore database starting from the daily sport "
            "categories discovery endpoint, then hydrate competitions and event lists."
        ),
    )
    parser.add_argument("--sport-slug", default="football", help="Sport slug used for categories/scheduled/live discovery.")
    parser.add_argument("--date", action="append", default=[], help="Repeatable YYYY-MM-DD date.")
    parser.add_argument("--date-from", default=None, help="Inclusive start date in YYYY-MM-DD format.")
    parser.add_argument("--date-to", default=None, help="Inclusive end date in YYYY-MM-DD format.")
    parser.add_argument(
        "--timezone-offset-seconds",
        type=int,
        default=None,
        help="Explicit timezone offset in seconds for every categories request.",
    )
    parser.add_argument(
        "--timezone-name",
        default=None,
        help="IANA timezone name used to derive the daily offset when the explicit offset is omitted.",
    )
    parser.add_argument(
        "--competition-limit",
        type=int,
        default=None,
        help="Optional cap on unique tournaments hydrated from categories discovery.",
    )
    parser.add_argument("--competition-offset", type=int, default=0, help="Offset into discovered unique tournaments.")
    parser.add_argument(
        "--competition-concurrency",
        type=int,
        default=3,
        help="Concurrent competition hydrations after discovery.",
    )
    parser.add_argument("--skip-competition", action="store_true", help="Skip unique tournament hydration.")
    parser.add_argument("--skip-scheduled", action="store_true", help="Skip scheduled-events ingestion for each date.")
    parser.add_argument("--include-live", action="store_true", help="Also load /sport/{sport_slug}/events/live once.")
    parser.add_argument(
        "--progress-every",
        type=int,
        default=25,
        help="Print competition progress every N completed items.",
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
    dates = _resolve_dates(args)
    _progress(
        "bootstrap",
        (
            f"start dates={len(dates)} "
            f"competition_concurrency={max(args.competition_concurrency, 1)} "
            f"include_live={bool(args.include_live)} "
            f"timeout={args.timeout}"
        ),
    )

    async with AsyncpgDatabase(database_config) as database:
        client = SofascoreClient(runtime_config)

        categories_job = CategoriesSeedIngestJob(
            CategoriesSeedParser(client),
            CategoriesSeedRepository(),
            database,
        )
        competition_job = CompetitionIngestJob(
            CompetitionParser(client),
            CompetitionRepository(),
            database,
        )
        event_list_job = EventListIngestJob(
            EventListParser(client),
            EventListRepository(),
            database,
        )

        category_results = []
        discovered_unique_tournament_ids: list[int] = []
        seen_unique_tournament_ids: set[int] = set()
        for observed_date in dates:
            timezone_offset_seconds = resolve_timezone_offset_seconds(
                observed_date=observed_date,
                timezone_name=args.timezone_name,
                timezone_offset_seconds=args.timezone_offset_seconds,
            )
            _progress(
                "categories",
                f"date={observed_date} timezone_offset_seconds={timezone_offset_seconds} start",
            )
            result = await categories_job.run(
                observed_date,
                timezone_offset_seconds,
                sport_slug=args.sport_slug,
                timeout=args.timeout,
            )
            category_results.append(result)
            _progress(
                "categories",
                (
                    f"date={observed_date} done "
                    f"categories={result.written.daily_summary_rows} "
                    f"unique_tournaments={result.written.daily_unique_tournament_rows} "
                    f"teams={result.written.daily_team_rows}"
                ),
            )
            for row in result.parsed.daily_unique_tournaments:
                if row.unique_tournament_id not in seen_unique_tournament_ids:
                    seen_unique_tournament_ids.add(row.unique_tournament_id)
                    discovered_unique_tournament_ids.append(row.unique_tournament_id)

        selected_unique_tournament_ids = discovered_unique_tournament_ids[args.competition_offset :]
        if args.competition_limit is not None:
            selected_unique_tournament_ids = selected_unique_tournament_ids[: max(args.competition_limit, 0)]
        _progress(
            "competition",
            (
                f"selected={len(selected_unique_tournament_ids)} "
                f"discovered_total={len(discovered_unique_tournament_ids)} "
                f"offset={args.competition_offset} "
                f"limit={args.competition_limit if args.competition_limit is not None else 'all'}"
            ),
        )

        competition_items: tuple[_CompetitionBootstrapItem, ...] = ()
        if not args.skip_competition and selected_unique_tournament_ids:
            competition_items = await _run_competitions(
                competition_job,
                selected_unique_tournament_ids,
                concurrency=max(args.competition_concurrency, 1),
                timeout=args.timeout,
                progress_every=max(args.progress_every, 1),
            )

        scheduled_results = []
        if not args.skip_scheduled:
            for observed_date in dates:
                _progress("scheduled", f"date={observed_date} start")
                scheduled_results.append(
                    await event_list_job.run_scheduled(
                        observed_date,
                        sport_slug=args.sport_slug,
                        timeout=args.timeout,
                    )
                )
                _progress(
                    "scheduled",
                    f"date={observed_date} done events={scheduled_results[-1].written.event_rows}",
                )

        live_result = None
        if args.include_live:
            _progress("live", "start")
            live_result = await event_list_job.run_live(sport_slug=args.sport_slug, timeout=args.timeout)
            _progress("live", f"done events={live_result.written.event_rows}")

    categories_total = sum(item.written.daily_summary_rows for item in category_results)
    discovered_total = len(discovered_unique_tournament_ids)
    competition_succeeded = sum(1 for item in competition_items if item.success)
    competition_failed = len(competition_items) - competition_succeeded
    scheduled_event_rows = sum(item.written.event_rows for item in scheduled_results)
    live_event_rows = 0 if live_result is None else live_result.written.event_rows

    print(
        "bootstrap_pipeline "
        f"dates={len(dates)} "
        f"categories={categories_total} "
        f"discovered_unique_tournaments={discovered_total} "
        f"competition_succeeded={competition_succeeded}/{len(competition_items)} "
        f"scheduled_events={scheduled_event_rows} "
        f"live_events={live_event_rows}"
    )
    return 0 if competition_failed == 0 else 1


async def _run_competitions(
    job: CompetitionIngestJob,
    unique_tournament_ids: list[int],
    *,
    concurrency: int,
    timeout: float,
    progress_every: int,
) -> tuple[_CompetitionBootstrapItem, ...]:
    logger = logging.getLogger(__name__)
    semaphore = asyncio.Semaphore(concurrency)

    async def _run_one(unique_tournament_id: int) -> _CompetitionBootstrapItem:
        async with semaphore:
            try:
                await job.run(unique_tournament_id, timeout=timeout)
            except Exception as exc:
                logger.warning(
                    "Bootstrap competition hydration failed for unique_tournament_id=%s: %s",
                    unique_tournament_id,
                    exc,
                )
                return _CompetitionBootstrapItem(
                    unique_tournament_id=unique_tournament_id,
                    success=False,
                    error=str(exc),
                )
            return _CompetitionBootstrapItem(unique_tournament_id=unique_tournament_id, success=True)

    tasks = [asyncio.create_task(_run_one(unique_tournament_id)) for unique_tournament_id in unique_tournament_ids]
    items: list[_CompetitionBootstrapItem] = []
    total = len(tasks)
    success_count = 0
    failure_count = 0

    for completed_count, task in enumerate(asyncio.as_completed(tasks), start=1):
        item = await task
        items.append(item)
        if item.success:
            success_count += 1
        else:
            failure_count += 1

        if (
            completed_count == 1
            or completed_count == total
            or completed_count % progress_every == 0
            or not item.success
        ):
            _progress(
                "competition",
                (
                    f"progress={completed_count}/{total} "
                    f"succeeded={success_count} "
                    f"failed={failure_count}"
                ),
            )

    return tuple(items)


def _resolve_dates(args: argparse.Namespace) -> list[str]:
    ordered_dates = list(dict.fromkeys(args.date or []))
    if args.date_from:
        start = Date.fromisoformat(args.date_from)
        finish = Date.fromisoformat(args.date_to or args.date_from)
        if finish < start:
            raise ValueError("--date-to cannot be earlier than --date-from")
        current = start
        while current <= finish:
            value = current.isoformat()
            if value not in ordered_dates:
                ordered_dates.append(value)
            current += timedelta(days=1)

    if not ordered_dates:
        raise ValueError("Pass at least one --date or a --date-from/--date-to range")
    return ordered_dates


def _progress(stage: str, message: str) -> None:
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{stage}] {message}", flush=True)
