"""Pipeline that starts from category/tournament discovery instead of curated defaults."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from .db import AsyncpgDatabase, load_database_config
from .limit_utils import normalize_limit
from .runtime import load_runtime_config
from .sources import build_source_adapter


@dataclass(frozen=True)
class _CategoryResult:
    category_id: int
    success: bool
    selected_unique_tournament_ids: tuple[int, ...]
    stage_failures: int
    error: str | None = None


@dataclass(frozen=True)
class _TournamentResult:
    unique_tournament_id: int
    success: bool
    discovered_team_ids: int
    discovered_event_ids: int
    stage_failures: int
    error: str | None = None


@dataclass(frozen=True)
class _EventDetailItem:
    event_id: int
    success: bool
    error: str | None = None


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description=(
            "Discover categories and unique tournaments from sport/category endpoints, then "
            "hydrate competition metadata plus tournament/day scheduled events."
        ),
    )
    parser.add_argument("--sport-slug", default="tennis", help="Sport slug for discovery.")
    parser.add_argument("--date", required=True, help="Date in YYYY-MM-DD format for tournament scheduled events.")
    parser.add_argument(
        "--category-id",
        type=int,
        action="append",
        default=[],
        help="Repeatable category id seed. Defaults to -101 for tennis when omitted.",
    )
    parser.add_argument(
        "--include-all-categories",
        action="store_true",
        help="After loading /sport/{sport}/categories/all, also expand every discovered category id.",
    )
    parser.add_argument("--skip-categories-all", action="store_true", help="Skip /sport/{sport}/categories/all.")
    parser.add_argument(
        "--prefer-all-tournaments",
        action="store_true",
        help="Use every discovered unique tournament instead of activeUniqueTournamentIds when both are present.",
    )
    parser.add_argument("--category-limit", type=int, default=None, help="Optional cap on selected categories.")
    parser.add_argument("--category-offset", type=int, default=0, help="Offset into selected categories.")
    parser.add_argument("--tournament-limit", type=int, default=None, help="Optional cap on selected tournaments.")
    parser.add_argument("--tournament-offset", type=int, default=0, help="Offset into selected tournaments.")
    parser.add_argument("--tournament-concurrency", type=int, default=3, help="Concurrent tournament workers.")
    parser.add_argument("--event-concurrency", type=int, default=3, help="Concurrent event-detail workers.")
    parser.add_argument("--skip-competition", action="store_true", help="Skip /unique-tournament/{id} hydration.")
    parser.add_argument(
        "--skip-scheduled-events",
        action="store_true",
        help="Skip /unique-tournament/{id}/scheduled-events/{date} hydration.",
    )
    parser.add_argument("--skip-event-detail", action="store_true", help="Skip event-detail hydration.")
    parser.add_argument(
        "--provider-id",
        type=int,
        action="append",
        default=[],
        help="Odds provider ids for event-detail hydration. Defaults to provider 1.",
    )
    parser.add_argument("--progress-every", type=int, default=1, help="Print progress every N completed tournaments.")
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
    sport_slug = args.sport_slug.strip().lower()
    category_seed_ids = tuple(dict.fromkeys(args.category_id or ((-101,) if sport_slug == "tennis" else ())))
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
        adapter = build_source_adapter(
            runtime_config.source_slug,
            runtime_config=runtime_config,
        )
        category_job = adapter.build_category_tournaments_job(database)
        competition_job = adapter.build_competition_job(database)
        event_list_job = adapter.build_event_list_job(database)
        event_detail_job = adapter.build_event_detail_job(database)

        _progress(
            "category_tournaments",
            (
                f"start sport={sport_slug} date={args.date} "
                f"tournament_concurrency={max(args.tournament_concurrency, 1)} "
                f"event_concurrency={max(args.event_concurrency, 1)}"
            ),
        )

        categories_all_result = None
        discovered_catalog_category_ids: tuple[int, ...] = ()
        if not args.skip_categories_all:
            categories_all_result = await category_job.run_categories_all(
                sport_slug=sport_slug,
                timeout=args.timeout,
            )
            discovered_catalog_category_ids = categories_all_result.parsed.category_ids
            _progress(
                "categories_all",
                (
                    f"sport={sport_slug} categories={len(discovered_catalog_category_ids)} "
                    f"snapshots={categories_all_result.written.payload_snapshot_rows}"
                ),
            )

        selected_category_ids = list(category_seed_ids)
        if args.include_all_categories:
            for category_id in discovered_catalog_category_ids:
                if category_id not in selected_category_ids:
                    selected_category_ids.append(category_id)
        selected_category_ids = list(
            _select_ids(
                selected_category_ids,
                offset=args.category_offset,
                limit=args.category_limit,
            )
        )
        if not selected_category_ids:
            raise ValueError("No category ids selected. Pass --category-id or allow categories/all expansion.")

        category_results: list[_CategoryResult] = []
        discovered_unique_tournament_ids: list[int] = []
        seen_unique_tournament_ids: set[int] = set()
        category_stage_failures = 0
        for index, category_id in enumerate(selected_category_ids, start=1):
            _progress("category", f"start {index}/{len(selected_category_ids)} category_id={category_id}")
            try:
                result = await category_job.run_category_unique_tournaments(
                    category_id,
                    sport_slug=sport_slug,
                    timeout=args.timeout,
                )
                candidate_ids = (
                    result.parsed.unique_tournament_ids
                    if args.prefer_all_tournaments or not result.parsed.active_unique_tournament_ids
                    else result.parsed.active_unique_tournament_ids
                )
                for unique_tournament_id in candidate_ids:
                    if unique_tournament_id not in seen_unique_tournament_ids:
                        seen_unique_tournament_ids.add(unique_tournament_id)
                        discovered_unique_tournament_ids.append(unique_tournament_id)
                category_results.append(
                    _CategoryResult(
                        category_id=category_id,
                        success=True,
                        selected_unique_tournament_ids=tuple(candidate_ids),
                        stage_failures=0,
                    )
                )
                _progress(
                    "category",
                    (
                        f"done category_id={category_id} groups={len(result.parsed.group_names)} "
                        f"selected_tournaments={len(candidate_ids)}"
                    ),
                )
            except Exception as exc:
                category_stage_failures += 1
                logging.getLogger(__name__).warning(
                    "Category discovery failed for category_id=%s: %s",
                    category_id,
                    exc,
                )
                category_results.append(
                    _CategoryResult(
                        category_id=category_id,
                        success=False,
                        selected_unique_tournament_ids=(),
                        stage_failures=1,
                        error=str(exc),
                    )
                )

        selected_unique_tournament_ids = _select_ids(
            discovered_unique_tournament_ids,
            offset=args.tournament_offset,
            limit=args.tournament_limit,
        )
        _progress(
            "category_tournaments",
            (
                f"selected_categories={len(selected_category_ids)} "
                f"discovered_tournaments={len(discovered_unique_tournament_ids)} "
                f"selected_tournaments={len(selected_unique_tournament_ids)}"
            ),
        )

        tournament_results = await _run_tournament_workers(
            competition_job=competition_job,
            event_list_job=event_list_job,
            event_detail_job=event_detail_job,
            sport_slug=sport_slug,
            observed_date=args.date,
            unique_tournament_ids=selected_unique_tournament_ids,
            provider_ids=provider_ids,
            tournament_concurrency=max(args.tournament_concurrency, 1),
            event_concurrency=max(args.event_concurrency, 1),
            skip_competition=bool(args.skip_competition),
            skip_scheduled_events=bool(args.skip_scheduled_events),
            skip_event_detail=bool(args.skip_event_detail),
            timeout=args.timeout,
            progress_every=max(args.progress_every, 1),
        )

    succeeded = sum(1 for item in tournament_results if item.success)
    failed = len(tournament_results) - succeeded
    discovered_teams = sum(item.discovered_team_ids for item in tournament_results)
    discovered_events = sum(item.discovered_event_ids for item in tournament_results)
    stage_failures = category_stage_failures + sum(item.stage_failures for item in tournament_results)
    print(
        "category_tournaments_pipeline "
        f"sport={sport_slug} "
        f"categories={len(selected_category_ids)} "
        f"category_failures={category_stage_failures} "
        f"tournaments={len(tournament_results)} "
        f"succeeded={succeeded} "
        f"failed={failed} "
        f"discovered_teams={discovered_teams} "
        f"discovered_events={discovered_events} "
        f"stage_failures={stage_failures}"
    )
    return 0 if failed == 0 else 1


async def _run_tournament_workers(
    *,
    competition_job: CompetitionIngestJob,
    event_list_job: EventListIngestJob,
    event_detail_job: EventDetailIngestJob,
    sport_slug: str,
    observed_date: str,
    unique_tournament_ids: Sequence[int],
    provider_ids: tuple[int, ...],
    tournament_concurrency: int,
    event_concurrency: int,
    skip_competition: bool,
    skip_scheduled_events: bool,
    skip_event_detail: bool,
    timeout: float,
    progress_every: int,
) -> tuple[_TournamentResult, ...]:
    logger = logging.getLogger(__name__)
    semaphore = asyncio.Semaphore(max(tournament_concurrency, 1))

    async def _run_one(unique_tournament_id: int) -> _TournamentResult:
        async with semaphore:
            return await _run_tournament_worker(
                competition_job=competition_job,
                event_list_job=event_list_job,
                event_detail_job=event_detail_job,
                sport_slug=sport_slug,
                observed_date=observed_date,
                unique_tournament_id=unique_tournament_id,
                provider_ids=provider_ids,
                event_concurrency=event_concurrency,
                skip_competition=skip_competition,
                skip_scheduled_events=skip_scheduled_events,
                skip_event_detail=skip_event_detail,
                timeout=timeout,
            )

    tasks = [asyncio.create_task(_run_one(unique_tournament_id)) for unique_tournament_id in unique_tournament_ids]
    items: list[_TournamentResult] = []
    success_count = 0
    failure_count = 0
    total = len(tasks)

    for completed_count, task in enumerate(asyncio.as_completed(tasks), start=1):
        item = await task
        items.append(item)
        if item.success:
            success_count += 1
        else:
            failure_count += 1
            logger.warning(
                "Category/tournament worker failed for unique_tournament_id=%s: %s",
                item.unique_tournament_id,
                item.error,
            )
        if (
            completed_count == 1
            or completed_count == total
            or completed_count % progress_every == 0
            or not item.success
        ):
            _progress(
                "category_tournaments",
                (
                    f"progress={completed_count}/{total} succeeded={success_count} failed={failure_count} "
                    f"stage_failures={sum(result.stage_failures for result in items)}"
                ),
            )

    return tuple(items)


async def _run_tournament_worker(
    *,
    competition_job: CompetitionIngestJob,
    event_list_job: EventListIngestJob,
    event_detail_job: EventDetailIngestJob,
    sport_slug: str,
    observed_date: str,
    unique_tournament_id: int,
    provider_ids: tuple[int, ...],
    event_concurrency: int,
    skip_competition: bool,
    skip_scheduled_events: bool,
    skip_event_detail: bool,
    timeout: float,
) -> _TournamentResult:
    logger = logging.getLogger(__name__)
    stage_failures = 0
    discovered_team_ids = 0
    discovered_event_ids = 0

    _progress("tournament", f"start unique_tournament_id={unique_tournament_id}")
    if not skip_competition:
        try:
            competition_result = await competition_job.run(unique_tournament_id, timeout=timeout)
            _progress(
                "competition",
                (
                    f"unique_tournament_id={unique_tournament_id} "
                    f"seasons={competition_result.written.unique_tournament_season_rows}"
                ),
            )
        except Exception as exc:
            return _TournamentResult(
                unique_tournament_id=unique_tournament_id,
                success=False,
                discovered_team_ids=0,
                discovered_event_ids=0,
                stage_failures=1,
                error=str(exc),
            )

    scheduled_event_ids: tuple[int, ...] = ()
    if not skip_scheduled_events:
        try:
            scheduled_result = await event_list_job.run_unique_tournament_scheduled(
                unique_tournament_id,
                observed_date,
                sport_slug=sport_slug,
                timeout=timeout,
            )
            scheduled_event_ids = tuple(dict.fromkeys(item.id for item in scheduled_result.parsed.events))
            discovered_team_ids = len(tuple(dict.fromkeys(item.id for item in scheduled_result.parsed.teams)))
            discovered_event_ids = len(scheduled_event_ids)
            _progress(
                "scheduled_events",
                (
                    f"unique_tournament_id={unique_tournament_id} date={observed_date} "
                    f"events={discovered_event_ids} teams={discovered_team_ids}"
                ),
            )
        except Exception as exc:
            stage_failures += 1
            logger.warning(
                "Category/tournament scheduled-events failed for unique_tournament_id=%s date=%s: %s",
                unique_tournament_id,
                observed_date,
                exc,
            )

    if scheduled_event_ids and not skip_event_detail:
        event_detail_items = await _run_event_detail_batch(
            event_detail_job,
            event_ids=scheduled_event_ids,
            provider_ids=provider_ids,
            concurrency=event_concurrency,
            timeout=timeout,
        )
        event_detail_failed = sum(1 for item in event_detail_items if not item.success)
        stage_failures += event_detail_failed
        _progress(
            "event_detail",
            (
                f"unique_tournament_id={unique_tournament_id} "
                f"succeeded={len(event_detail_items) - event_detail_failed}/{len(event_detail_items)} "
                f"failed={event_detail_failed}"
            ),
        )

    _progress(
        "tournament",
        (
            f"done unique_tournament_id={unique_tournament_id} "
            f"teams={discovered_team_ids} events={discovered_event_ids} stage_failures={stage_failures}"
        ),
    )
    return _TournamentResult(
        unique_tournament_id=unique_tournament_id,
        success=True,
        discovered_team_ids=discovered_team_ids,
        discovered_event_ids=discovered_event_ids,
        stage_failures=stage_failures,
    )


async def _run_event_detail_batch(
    event_detail_job: EventDetailIngestJob,
    *,
    event_ids: Sequence[int],
    provider_ids: tuple[int, ...],
    concurrency: int,
    timeout: float,
) -> tuple[_EventDetailItem, ...]:
    logger = logging.getLogger(__name__)
    semaphore = asyncio.Semaphore(max(concurrency, 1))

    async def _run_one(event_id: int) -> _EventDetailItem:
        async with semaphore:
            try:
                await event_detail_job.run(event_id, provider_ids=provider_ids, timeout=timeout)
            except Exception as exc:
                logger.warning("Category/tournament event-detail failed for event_id=%s: %s", event_id, exc)
                return _EventDetailItem(event_id=event_id, success=False, error=str(exc))
            return _EventDetailItem(event_id=event_id, success=True)

    deduped_event_ids = tuple(dict.fromkeys(int(event_id) for event_id in event_ids))
    return tuple(await asyncio.gather(*(_run_one(event_id) for event_id in deduped_event_ids)))


def _select_ids(values: Sequence[int], *, offset: int, limit: int | None) -> tuple[int, ...]:
    selected = tuple(dict.fromkeys(int(value) for value in values))
    selected = selected[max(offset, 0) :]
    resolved_limit = normalize_limit(limit)
    if resolved_limit is not None:
        selected = selected[:resolved_limit]
    return selected


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
