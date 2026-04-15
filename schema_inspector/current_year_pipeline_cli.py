"""Fast current-year sport pipeline built from wide discovery plus bounded backfills."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import date as Date
from datetime import datetime, timedelta

from .categories_seed_job import CategoriesSeedIngestJob
from .categories_seed_parser import CategoriesSeedParser
from .categories_seed_repository import CategoriesSeedRepository
from .category_tournaments_job import CategoryTournamentsIngestJob
from .category_tournaments_parser import CategoryTournamentsParser
from .competition_job import CompetitionIngestJob
from .competition_parser import CompetitionParser
from .competition_repository import CompetitionRepository
from .db import AsyncpgDatabase, load_database_config
from .entities_backfill_job import EntitiesBackfillJob
from .entities_job import EntitiesIngestJob
from .entities_parser import EntitiesParser
from .entities_repository import EntitiesRepository
from .event_detail_backfill_job import EventDetailBackfillJob
from .event_detail_job import EventDetailIngestJob
from .event_detail_parser import EventDetailParser
from .event_detail_repository import EventDetailRepository
from .event_list_job import EventListIngestJob
from .event_list_parser import EventListParser
from .event_list_repository import EventListRepository
from .runtime import load_runtime_config
from .scheduled_tournaments_job import ScheduledTournamentsIngestJob
from .scheduled_tournaments_parser import ScheduledTournamentsParser
from .sofascore_client import SofascoreClient
from .sport_profiles import resolve_sport_profile
from .timezone_utils import resolve_timestamp_bounds, resolve_timezone_offset_seconds


@dataclass(frozen=True)
class _CompetitionItem:
    unique_tournament_id: int
    success: bool
    error: str | None = None


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    today = datetime.now().date()

    parser = argparse.ArgumentParser(
        description=(
            "Fast current-year pipeline: wide tournament discovery, competition hydration, "
            "daily event ingestion, event detail, and bounded entities backfill."
        ),
    )
    parser.add_argument("--sport-slug", default="football", help="Sport slug to ingest.")
    parser.add_argument("--year", type=int, default=today.year, help="Calendar year to ingest when dates are omitted.")
    parser.add_argument("--date-from", default=None, help="Inclusive start date in YYYY-MM-DD format.")
    parser.add_argument("--date-to", default=None, help="Inclusive end date in YYYY-MM-DD format.")
    parser.add_argument(
        "--timezone-offset-seconds",
        type=int,
        default=None,
        help="Explicit timezone offset for daily categories discovery.",
    )
    parser.add_argument(
        "--timezone-name",
        default=None,
        help="IANA timezone name used for date bounds and category seed offsets.",
    )
    parser.add_argument("--tournament-limit", type=int, default=None, help="Optional cap on discovered tournaments.")
    parser.add_argument("--tournament-offset", type=int, default=0, help="Offset into discovered tournaments.")
    parser.add_argument(
        "--discovery-date-concurrency",
        type=int,
        default=4,
        help="Concurrent date workers for discovery stages.",
    )
    parser.add_argument(
        "--scheduled-page-limit",
        type=int,
        default=None,
        help="Optional cap on scheduled-tournaments pages per date.",
    )
    parser.add_argument(
        "--competition-concurrency",
        type=int,
        default=6,
        help="Concurrent competition hydrations after discovery.",
    )
    parser.add_argument(
        "--scheduled-events-concurrency",
        type=int,
        default=4,
        help="Concurrent daily /scheduled-events ingestions.",
    )
    parser.add_argument(
        "--event-detail-concurrency",
        type=int,
        default=6,
        help="Concurrent event-detail workers for current-year events.",
    )
    parser.add_argument(
        "--category-id",
        type=int,
        action="append",
        default=[],
        help="Optional repeatable extra category seed id for category discovery.",
    )
    parser.add_argument(
        "--disable-category-discovery",
        action="store_true",
        help="Skip categories/all + category/{id}/unique-tournaments discovery even if the sport profile enables it.",
    )
    parser.add_argument(
        "--include-all-categories",
        action="store_true",
        help="Force categories/all expansion when running category discovery.",
    )
    parser.add_argument("--skip-competition", action="store_true", help="Skip /unique-tournament/{id} hydration.")
    parser.add_argument(
        "--skip-scheduled-events",
        action="store_true",
        help="Skip /sport/{sport}/scheduled-events/{date} ingestion.",
    )
    parser.add_argument("--skip-event-detail", action="store_true", help="Skip event-detail backfill.")
    parser.add_argument("--skip-entities", action="store_true", help="Skip entities backfill.")
    parser.add_argument(
        "--event-detail-limit",
        type=int,
        default=None,
        help="Optional cap on current-year event-detail candidates.",
    )
    parser.add_argument(
        "--all-event-detail",
        action="store_true",
        help="Process current-year event ids even if detail snapshots already exist.",
    )
    parser.add_argument("--entity-player-limit", type=int, default=None, help="Optional cap on entity player ids.")
    parser.add_argument("--entity-team-limit", type=int, default=None, help="Optional cap on entity team ids.")
    parser.add_argument(
        "--entity-player-request-limit",
        type=int,
        default=None,
        help="Optional cap on player season requests for entities.",
    )
    parser.add_argument(
        "--entity-team-request-limit",
        type=int,
        default=None,
        help="Optional cap on team season requests for entities.",
    )
    parser.add_argument(
        "--all-entities",
        action="store_true",
        help="Process current-year entity seeds even if snapshots already exist.",
    )
    parser.add_argument(
        "--provider-id",
        type=int,
        action="append",
        default=[],
        help="Odds provider ids for event-detail hydration. Defaults to provider 1.",
    )
    parser.add_argument("--timeout", type=float, default=20.0, help="Request timeout in seconds.")
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
    sport_slug = str(args.sport_slug).strip().lower()
    sport_profile = resolve_sport_profile(sport_slug)
    dates = _resolve_dates(args)
    date_from = dates[0]
    date_to = dates[-1]
    start_timestamp_from, start_timestamp_to = resolve_timestamp_bounds(
        date_from=date_from,
        date_to=date_to,
        timezone_name=args.timezone_name,
    )
    provider_ids = tuple(dict.fromkeys(args.provider_id or [1]))

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
        categories_job = CategoriesSeedIngestJob(
            CategoriesSeedParser(client),
            CategoriesSeedRepository(),
            database,
        )
        category_tournaments_job = CategoryTournamentsIngestJob(
            CategoryTournamentsParser(client),
            CompetitionRepository(),
            database,
        )
        scheduled_tournaments_job = ScheduledTournamentsIngestJob(
            ScheduledTournamentsParser(client),
            EventListRepository(),
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
        event_detail_backfill_job = EventDetailBackfillJob(
            EventDetailIngestJob(EventDetailParser(client), EventDetailRepository(), database),
            database,
        )
        entities_backfill_job = EntitiesBackfillJob(
            EntitiesIngestJob(EntitiesParser(client), EntitiesRepository(), database),
            database,
        )

        _progress(
            "current_year",
            (
                f"start sport={sport_slug} dates={len(dates)} "
                f"range={date_from}..{date_to} "
                f"competition_concurrency={max(args.competition_concurrency, 1)} "
                f"event_detail_concurrency={max(args.event_detail_concurrency, 1)}"
            ),
        )

        discovered_unique_tournament_ids: list[int] = []
        seen_unique_tournament_ids: set[int] = set()
        discovery_stage_failures = 0
        discovery_pages = 0

        if sport_profile.use_daily_categories_seed:
            ids, failures = await _discover_from_daily_categories(
                categories_job=categories_job,
                sport_slug=sport_slug,
                dates=dates,
                discovery_date_concurrency=max(args.discovery_date_concurrency, 1),
                timeout=args.timeout,
                timezone_name=args.timezone_name,
                timezone_offset_seconds=args.timezone_offset_seconds,
            )
            discovery_stage_failures += failures
            for unique_tournament_id in ids:
                if unique_tournament_id not in seen_unique_tournament_ids:
                    seen_unique_tournament_ids.add(unique_tournament_id)
                    discovered_unique_tournament_ids.append(unique_tournament_id)

        if sport_profile.use_scheduled_tournaments:
            ids, pages, failures = await _discover_from_scheduled_tournaments(
                scheduled_tournaments_job=scheduled_tournaments_job,
                sport_slug=sport_slug,
                dates=dates,
                discovery_date_concurrency=max(args.discovery_date_concurrency, 1),
                scheduled_page_limit=args.scheduled_page_limit,
                timeout=args.timeout,
            )
            discovery_pages += pages
            discovery_stage_failures += failures
            for unique_tournament_id in ids:
                if unique_tournament_id not in seen_unique_tournament_ids:
                    seen_unique_tournament_ids.add(unique_tournament_id)
                    discovered_unique_tournament_ids.append(unique_tournament_id)

        if not args.disable_category_discovery:
            ids, failures = await _discover_from_category_tournaments(
                category_tournaments_job=category_tournaments_job,
                sport_slug=sport_slug,
                seed_category_ids=tuple(args.category_id),
                profile_category_ids=sport_profile.discovery_category_seed_ids,
                include_all_categories=(
                    args.include_all_categories or sport_profile.include_categories_all_discovery
                ),
                timeout=args.timeout,
            )
            discovery_stage_failures += failures
            for unique_tournament_id in ids:
                if unique_tournament_id not in seen_unique_tournament_ids:
                    seen_unique_tournament_ids.add(unique_tournament_id)
                    discovered_unique_tournament_ids.append(unique_tournament_id)

        selected_unique_tournament_ids = tuple(
            discovered_unique_tournament_ids[args.tournament_offset :]
            if args.tournament_limit is None
            else discovered_unique_tournament_ids[
                args.tournament_offset : args.tournament_offset + max(args.tournament_limit, 0)
            ]
        )

        _progress(
            "discovery",
            (
                f"discovered_unique_tournaments={len(discovered_unique_tournament_ids)} "
                f"selected={len(selected_unique_tournament_ids)} "
                f"scheduled_pages={discovery_pages} "
                f"stage_failures={discovery_stage_failures}"
            ),
        )

        competition_items: tuple[_CompetitionItem, ...] = ()
        if not args.skip_competition and selected_unique_tournament_ids:
            competition_items = await _run_competitions(
                competition_job=competition_job,
                unique_tournament_ids=selected_unique_tournament_ids,
                concurrency=max(args.competition_concurrency, 1),
                timeout=args.timeout,
            )

        scheduled_events_results = ()
        scheduled_event_failures = 0
        if not args.skip_scheduled_events:
            scheduled_events_results, scheduled_event_failures = await _run_scheduled_events(
                event_list_job=event_list_job,
                sport_slug=sport_slug,
                dates=dates,
                concurrency=max(args.scheduled_events_concurrency, 1),
                timeout=args.timeout,
            )

        event_detail_result = None
        if not args.skip_event_detail:
            _progress(
                "event_detail",
                (
                    f"start range={date_from}..{date_to} "
                    f"limit={args.event_detail_limit or 'all'} "
                    f"only_missing={'yes' if not args.all_event_detail else 'no'}"
                ),
            )
            event_detail_result = await event_detail_backfill_job.run(
                limit=args.event_detail_limit,
                only_missing=not args.all_event_detail,
                start_timestamp_from=start_timestamp_from,
                start_timestamp_to=start_timestamp_to,
                provider_ids=provider_ids,
                concurrency=max(args.event_detail_concurrency, 1),
                timeout=args.timeout,
            )
            _progress(
                "event_detail",
                (
                    f"done candidates={event_detail_result.total_candidates} "
                    f"succeeded={event_detail_result.succeeded} "
                    f"failed={event_detail_result.failed}"
                ),
            )

        entities_result = None
        if not args.skip_entities:
            _progress(
                "entities",
                (
                    f"start range={date_from}..{date_to} "
                    f"player_limit={args.entity_player_limit or 'all'} "
                    f"team_limit={args.entity_team_limit or 'all'} "
                    f"only_missing={'yes' if not args.all_entities else 'no'}"
                ),
            )
            entities_result = await entities_backfill_job.run(
                player_limit=args.entity_player_limit,
                team_limit=args.entity_team_limit,
                player_request_limit=args.entity_player_request_limit,
                team_request_limit=args.entity_team_request_limit,
                only_missing=not args.all_entities,
                event_timestamp_from=start_timestamp_from,
                event_timestamp_to=start_timestamp_to,
                timeout=args.timeout,
            )
            _progress(
                "entities",
                (
                    f"done players={len(entities_result.player_ids)} "
                    f"teams={len(entities_result.team_ids)} "
                    f"player_overall={len(entities_result.player_overall_requests)} "
                    f"team_overall={len(entities_result.team_overall_requests)}"
                ),
            )

    competition_succeeded = sum(1 for item in competition_items if item.success)
    competition_failed = len(competition_items) - competition_succeeded
    scheduled_events_total = sum(item.written.event_rows for item in scheduled_events_results)
    event_detail_failed = 0 if event_detail_result is None else event_detail_result.failed
    entities_snapshots = 0 if entities_result is None else entities_result.ingest.written.payload_snapshot_rows
    failed = competition_failed + scheduled_event_failures + event_detail_failed

    print(
        "current_year_pipeline "
        f"sport={sport_slug} "
        f"date_from={date_from} "
        f"date_to={date_to} "
        f"discovered_unique_tournaments={len(discovered_unique_tournament_ids)} "
        f"selected_unique_tournaments={len(selected_unique_tournament_ids)} "
        f"competition_succeeded={competition_succeeded}/{len(competition_items)} "
        f"scheduled_events={scheduled_events_total} "
        f"event_detail_candidates={0 if event_detail_result is None else event_detail_result.total_candidates} "
        f"event_detail_failed={event_detail_failed} "
        f"entities_snapshots={entities_snapshots} "
        f"stage_failures={discovery_stage_failures + scheduled_event_failures}"
    )
    return 0 if failed == 0 else 1


async def _discover_from_daily_categories(
    *,
    categories_job: CategoriesSeedIngestJob,
    sport_slug: str,
    dates: list[str],
    discovery_date_concurrency: int,
    timeout: float,
    timezone_name: str | None,
    timezone_offset_seconds: int | None,
) -> tuple[tuple[int, ...], int]:
    logger = logging.getLogger(__name__)
    semaphore = asyncio.Semaphore(max(discovery_date_concurrency, 1))

    async def _run_one(observed_date: str) -> tuple[tuple[int, ...], int]:
        async with semaphore:
            try:
                offset = resolve_timezone_offset_seconds(
                    observed_date=observed_date,
                    timezone_name=timezone_name,
                    timezone_offset_seconds=timezone_offset_seconds,
                )
                result = await categories_job.run(
                    observed_date,
                    offset,
                    sport_slug=sport_slug,
                    timeout=timeout,
                )
                ids = tuple(sorted({row.unique_tournament_id for row in result.parsed.daily_unique_tournaments}))
                _progress(
                    "categories",
                    f"date={observed_date} categories={result.written.daily_summary_rows} tournaments={len(ids)}",
                )
                return ids, 0
            except Exception as exc:
                logger.warning("Daily categories discovery failed for %s: %s", observed_date, exc)
                return (), 1

    discovered_ids: list[int] = []
    seen: set[int] = set()
    failures = 0
    for ids, item_failures in await asyncio.gather(*(_run_one(observed_date) for observed_date in dates)):
        failures += item_failures
        for unique_tournament_id in ids:
            if unique_tournament_id not in seen:
                seen.add(unique_tournament_id)
                discovered_ids.append(unique_tournament_id)
    return tuple(discovered_ids), failures


async def _discover_from_scheduled_tournaments(
    *,
    scheduled_tournaments_job: ScheduledTournamentsIngestJob,
    sport_slug: str,
    dates: list[str],
    discovery_date_concurrency: int,
    scheduled_page_limit: int | None,
    timeout: float,
) -> tuple[tuple[int, ...], int, int]:
    logger = logging.getLogger(__name__)
    semaphore = asyncio.Semaphore(max(discovery_date_concurrency, 1))

    async def _run_one(observed_date: str) -> tuple[tuple[int, ...], int, int]:
        async with semaphore:
            page = 1
            pages = 0
            ids: list[int] = []
            seen: set[int] = set()
            failures = 0
            while True:
                if scheduled_page_limit is not None and page > max(scheduled_page_limit, 0):
                    break
                try:
                    result = await scheduled_tournaments_job.run_page(
                        observed_date,
                        page,
                        sport_slug=sport_slug,
                        timeout=timeout,
                    )
                except Exception as exc:
                    logger.warning(
                        "Scheduled tournaments discovery failed for sport=%s date=%s page=%s: %s",
                        sport_slug,
                        observed_date,
                        page,
                        exc,
                    )
                    failures += 1
                    break
                pages += 1
                for unique_tournament_id in result.parsed.unique_tournament_ids:
                    if unique_tournament_id not in seen:
                        seen.add(unique_tournament_id)
                        ids.append(unique_tournament_id)
                if not result.parsed.has_next_page:
                    break
                page += 1
            _progress(
                "scheduled_tournaments",
                f"date={observed_date} pages={pages} tournaments={len(ids)} failures={failures}",
            )
            return tuple(ids), pages, failures

    discovered_ids: list[int] = []
    seen: set[int] = set()
    total_pages = 0
    failures = 0
    for ids, pages, item_failures in await asyncio.gather(*(_run_one(observed_date) for observed_date in dates)):
        total_pages += pages
        failures += item_failures
        for unique_tournament_id in ids:
            if unique_tournament_id not in seen:
                seen.add(unique_tournament_id)
                discovered_ids.append(unique_tournament_id)
    return tuple(discovered_ids), total_pages, failures


async def _discover_from_category_tournaments(
    *,
    category_tournaments_job: CategoryTournamentsIngestJob,
    sport_slug: str,
    seed_category_ids: tuple[int, ...],
    profile_category_ids: tuple[int, ...],
    include_all_categories: bool,
    timeout: float,
) -> tuple[tuple[int, ...], int]:
    logger = logging.getLogger(__name__)
    catalog_category_ids: tuple[int, ...] = ()
    failures = 0
    if include_all_categories:
        try:
            result = await category_tournaments_job.run_categories_all(
                sport_slug=sport_slug,
                timeout=timeout,
            )
            catalog_category_ids = result.parsed.category_ids
            _progress(
                "categories_all",
                f"sport={sport_slug} categories={len(catalog_category_ids)}",
            )
        except Exception as exc:
            logger.warning("categories/all discovery failed for sport=%s: %s", sport_slug, exc)
            failures += 1

    selected_category_ids = list(dict.fromkeys((*profile_category_ids, *seed_category_ids)))
    if include_all_categories:
        for category_id in catalog_category_ids:
            if category_id not in selected_category_ids:
                selected_category_ids.append(category_id)
    if not selected_category_ids:
        return (), failures

    discovered_ids: list[int] = []
    seen: set[int] = set()
    for category_id in selected_category_ids:
        try:
            result = await category_tournaments_job.run_category_unique_tournaments(
                category_id,
                sport_slug=sport_slug,
                timeout=timeout,
            )
            candidate_ids = (
                result.parsed.active_unique_tournament_ids
                if result.parsed.active_unique_tournament_ids
                else result.parsed.unique_tournament_ids
            )
            for unique_tournament_id in candidate_ids:
                if unique_tournament_id not in seen:
                    seen.add(unique_tournament_id)
                    discovered_ids.append(unique_tournament_id)
            _progress(
                "category",
                f"category_id={category_id} tournaments={len(candidate_ids)}",
            )
        except Exception as exc:
            logger.warning("Category discovery failed for category_id=%s: %s", category_id, exc)
            failures += 1
    return tuple(discovered_ids), failures


async def _run_competitions(
    *,
    competition_job: CompetitionIngestJob,
    unique_tournament_ids: tuple[int, ...],
    concurrency: int,
    timeout: float,
) -> tuple[_CompetitionItem, ...]:
    logger = logging.getLogger(__name__)
    semaphore = asyncio.Semaphore(max(concurrency, 1))

    async def _run_one(unique_tournament_id: int) -> _CompetitionItem:
        async with semaphore:
            try:
                await competition_job.run(unique_tournament_id, timeout=timeout)
            except Exception as exc:
                logger.warning("Competition hydration failed for unique_tournament_id=%s: %s", unique_tournament_id, exc)
                return _CompetitionItem(unique_tournament_id=unique_tournament_id, success=False, error=str(exc))
            return _CompetitionItem(unique_tournament_id=unique_tournament_id, success=True)

    items: list[_CompetitionItem] = []
    total = len(unique_tournament_ids)
    for completed_count, task in enumerate(
        asyncio.as_completed([asyncio.create_task(_run_one(item)) for item in unique_tournament_ids]),
        start=1,
    ):
        item = await task
        items.append(item)
        if completed_count == 1 or completed_count == total or completed_count % 25 == 0 or not item.success:
            succeeded = sum(1 for candidate in items if candidate.success)
            failed = len(items) - succeeded
            _progress(
                "competition",
                f"progress={completed_count}/{total} succeeded={succeeded} failed={failed}",
            )
    return tuple(items)


async def _run_scheduled_events(
    *,
    event_list_job: EventListIngestJob,
    sport_slug: str,
    dates: list[str],
    concurrency: int,
    timeout: float,
) -> tuple[tuple, int]:
    logger = logging.getLogger(__name__)
    semaphore = asyncio.Semaphore(max(concurrency, 1))

    async def _run_one(observed_date: str):
        async with semaphore:
            try:
                result = await event_list_job.run_scheduled(
                    observed_date,
                    sport_slug=sport_slug,
                    timeout=timeout,
                )
                _progress("scheduled_events", f"date={observed_date} events={result.written.event_rows}")
                return result, 0
            except Exception as exc:
                logger.warning("Scheduled events failed for sport=%s date=%s: %s", sport_slug, observed_date, exc)
                return None, 1

    results = []
    failures = 0
    for result, item_failures in await asyncio.gather(*(_run_one(observed_date) for observed_date in dates)):
        failures += item_failures
        if result is not None:
            results.append(result)
    return tuple(results), failures


def _resolve_dates(args: argparse.Namespace) -> list[str]:
    if args.date_from or args.date_to:
        if not args.date_from or not args.date_to:
            raise ValueError("Pass both --date-from and --date-to together")
        start = Date.fromisoformat(args.date_from)
        finish = Date.fromisoformat(args.date_to)
    else:
        year = int(args.year)
        start = Date(year, 1, 1)
        today = datetime.now().date()
        finish = today if today.year == year else Date(year, 12, 31)
    if finish < start:
        raise ValueError("date_to cannot be earlier than date_from")
    dates: list[str] = []
    current = start
    while current <= finish:
        dates.append(current.isoformat())
        current += timedelta(days=1)
    return dates


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
