"""Worker-based baseline pipeline for Sofascore default tournaments."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Sequence

from .competition_job import CompetitionIngestJob, CompetitionIngestResult
from .db import AsyncpgDatabase, load_database_config
from .endpoints import UNIQUE_TOURNAMENT_SEASONS_ENDPOINT
from .entities_job import EntitiesIngestJob
from .entities_parser import PlayerHeatmapRequest, PlayerOverallRequest, TeamOverallRequest, TeamPerformanceGraphRequest
from .event_detail_job import EventDetailIngestJob
from .event_list_job import EventListIngestJob
from .leaderboards_job import LeaderboardsIngestJob
from .limit_utils import normalize_limit
from .runtime import load_runtime_config
from .sources import build_source_adapter
from .sport_profiles import resolve_sport_profile
from .standings_job import StandingsIngestJob
from .statistics_job import StatisticsIngestJob
from .statistics_parser import StatisticsQuery


@dataclass(frozen=True)
class _WorkerResult:
    unique_tournament_id: int
    success: bool
    season_ids: tuple[int, ...]
    completed_seasons: int
    discovered_team_ids: int
    discovered_player_ids: int
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
            "Load Sofascore's curated default tournaments for one country/sport, then hydrate "
            "competition metadata, season aggregates, discoverable event detail, and entities."
        ),
    )
    parser.add_argument("--country-code", default="UA", help="Country code for the default-tournaments config.")
    parser.add_argument("--sport-slug", default="football", help="Sport slug for the default-tournaments config.")
    parser.add_argument(
        "--unique-tournament-id",
        type=int,
        action="append",
        default=[],
        help="Optional repeatable filter to a subset of the fetched default tournament ids.",
    )
    parser.add_argument("--tournament-limit", type=int, default=None, help="Optional cap on selected tournaments.")
    parser.add_argument("--tournament-offset", type=int, default=0, help="Offset into the fetched tournament ids.")
    parser.add_argument("--tournament-concurrency", type=int, default=3, help="Concurrent tournament workers.")
    parser.add_argument(
        "--seasons-per-tournament",
        type=int,
        default=2,
        help="Latest seasons to process per tournament. Use 0 or a negative value for all discovered seasons.",
    )
    parser.add_argument("--statistics-limit", type=int, default=20, help="Statistics query limit.")
    parser.add_argument("--statistics-offset", type=int, default=0, help="Statistics query offset.")
    parser.add_argument("--statistics-order", default="-rating", help="Statistics query order.")
    parser.add_argument("--statistics-accumulation", default="total", help="Statistics query accumulation.")
    parser.add_argument("--statistics-group", default="summary", help="Statistics query group.")
    parser.add_argument("--statistics-field", action="append", default=[], help="Repeatable statistics field.")
    parser.add_argument("--statistics-filter", action="append", default=[], help="Repeatable statistics filter.")
    parser.add_argument("--standings-scope", action="append", default=[], help="Repeatable standings scope.")
    parser.add_argument(
        "--provider-id",
        type=int,
        action="append",
        default=[],
        help="Odds provider ids for event-detail hydration. Defaults to provider 1.",
    )
    parser.add_argument("--event-concurrency", type=int, default=3, help="Concurrent event-detail workers per tournament.")
    parser.add_argument("--skip-featured-events", action="store_true", help="Skip featured-events ingestion.")
    parser.add_argument("--skip-round-events", action="store_true", help="Skip round-events ingestion.")
    parser.add_argument("--skip-event-detail", action="store_true", help="Skip event-detail hydration.")
    parser.add_argument("--skip-entities", action="store_true", help="Skip entities hydration.")
    parser.add_argument("--skip-statistics", action="store_true", help="Skip season statistics.")
    parser.add_argument("--skip-standings", action="store_true", help="Skip standings.")
    parser.add_argument("--skip-leaderboards", action="store_true", help="Skip leaderboards.")
    parser.add_argument("--progress-every", type=int, default=1, help="Print worker progress every N tournaments.")
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
    sport_profile = resolve_sport_profile(sport_slug)
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
    selected_filter_ids = tuple(dict.fromkeys(args.unique_tournament_id or ()))
    standings_scopes = tuple(dict.fromkeys(args.standings_scope or sport_profile.standings_scopes))
    provider_ids = tuple(dict.fromkeys(args.provider_id or [1]))
    stats_query = StatisticsQuery(
        limit=args.statistics_limit,
        offset=args.statistics_offset,
        order=args.statistics_order,
        accumulation=args.statistics_accumulation,
        group=args.statistics_group,
        fields=tuple(dict.fromkeys(args.statistics_field)),
        filters=tuple(dict.fromkeys(args.statistics_filter)),
    )

    async with AsyncpgDatabase(database_config) as database:
        adapter = build_source_adapter(
            runtime_config.source_slug,
            runtime_config=runtime_config,
        )
        parser = adapter.build_default_tournament_list_parser()
        competition_job = adapter.build_competition_job(database)
        event_list_job = adapter.build_event_list_job(database)
        statistics_job = adapter.build_statistics_job(database)
        standings_job = adapter.build_standings_job(database)
        leaderboards_job = adapter.build_leaderboards_job(database)
        event_detail_job = adapter.build_event_detail_job(database)
        entities_job = adapter.build_entities_job(database)

        _progress(
            "default_tournaments",
            (
                f"start country={args.country_code.upper()} sport={args.sport_slug.lower()} "
                f"tournament_concurrency={max(args.tournament_concurrency, 1)} "
                f"seasons_per_tournament={args.seasons_per_tournament}"
            ),
        )
        default_list = await parser.fetch(
            country_code=args.country_code,
            sport_slug=sport_slug,
            timeout=args.timeout,
        )
        selected_unique_tournament_ids = _select_unique_tournament_ids(
            default_list.unique_tournament_ids,
            offset=args.tournament_offset,
            limit=args.tournament_limit,
            include_ids=selected_filter_ids,
        )
        _progress(
            "default_tournaments",
            (
                f"fetched={len(default_list.unique_tournament_ids)} selected={len(selected_unique_tournament_ids)} "
                f"offset={args.tournament_offset} "
                f"limit={args.tournament_limit if args.tournament_limit is not None else 'all'}"
            ),
        )

        worker_results = await _run_tournament_workers(
            database,
            competition_job=competition_job,
            event_list_job=event_list_job,
            statistics_job=statistics_job,
            standings_job=standings_job,
            leaderboards_job=leaderboards_job,
            event_detail_job=event_detail_job,
            entities_job=entities_job,
            sport_slug=sport_slug,
            sport_profile=sport_profile,
            unique_tournament_ids=selected_unique_tournament_ids,
            standings_scopes=standings_scopes,
            provider_ids=provider_ids,
            stats_query=stats_query,
            seasons_per_tournament=args.seasons_per_tournament,
            tournament_concurrency=max(args.tournament_concurrency, 1),
            event_concurrency=max(args.event_concurrency, 1),
            skip_featured_events=bool(args.skip_featured_events),
            skip_round_events=bool(args.skip_round_events),
            skip_event_detail=bool(args.skip_event_detail),
            skip_entities=bool(args.skip_entities),
            skip_statistics=bool(args.skip_statistics),
            skip_standings=bool(args.skip_standings),
            skip_leaderboards=bool(args.skip_leaderboards),
            timeout=args.timeout,
            progress_every=max(args.progress_every, 1),
        )

    succeeded = sum(1 for item in worker_results if item.success)
    failed = len(worker_results) - succeeded
    completed_seasons = sum(item.completed_seasons for item in worker_results)
    discovered_team_ids = sum(item.discovered_team_ids for item in worker_results)
    discovered_player_ids = sum(item.discovered_player_ids for item in worker_results)
    discovered_event_ids = sum(item.discovered_event_ids for item in worker_results)
    stage_failures = sum(item.stage_failures for item in worker_results)
    print(
        "default_tournaments_pipeline "
        f"tournaments={len(worker_results)} "
        f"succeeded={succeeded} "
        f"failed={failed} "
        f"completed_seasons={completed_seasons} "
        f"discovered_teams={discovered_team_ids} "
        f"discovered_players={discovered_player_ids} "
        f"discovered_events={discovered_event_ids} "
        f"stage_failures={stage_failures}"
    )
    return 0 if failed == 0 else 1


async def _run_tournament_workers(
    database: AsyncpgDatabase,
    *,
    competition_job: CompetitionIngestJob,
    event_list_job: EventListIngestJob,
    statistics_job: StatisticsIngestJob,
    standings_job: StandingsIngestJob,
    leaderboards_job: LeaderboardsIngestJob,
    event_detail_job: EventDetailIngestJob,
    entities_job: EntitiesIngestJob,
    sport_slug: str,
    sport_profile,
    unique_tournament_ids: tuple[int, ...],
    standings_scopes: tuple[str, ...],
    provider_ids: tuple[int, ...],
    stats_query: StatisticsQuery,
    seasons_per_tournament: int,
    tournament_concurrency: int,
    event_concurrency: int,
    skip_featured_events: bool,
    skip_round_events: bool,
    skip_event_detail: bool,
    skip_entities: bool,
    skip_statistics: bool,
    skip_standings: bool,
    skip_leaderboards: bool,
    timeout: float,
    progress_every: int,
) -> tuple[_WorkerResult, ...]:
    logger = logging.getLogger(__name__)
    semaphore = asyncio.Semaphore(max(tournament_concurrency, 1))

    async def _run_one(unique_tournament_id: int) -> _WorkerResult:
        async with semaphore:
            return await _run_tournament_worker(
                database,
                competition_job=competition_job,
                event_list_job=event_list_job,
                statistics_job=statistics_job,
                standings_job=standings_job,
                leaderboards_job=leaderboards_job,
                event_detail_job=event_detail_job,
                entities_job=entities_job,
                sport_slug=sport_slug,
                sport_profile=sport_profile,
                unique_tournament_id=unique_tournament_id,
                standings_scopes=standings_scopes,
                provider_ids=provider_ids,
                stats_query=stats_query,
                seasons_per_tournament=seasons_per_tournament,
                event_concurrency=event_concurrency,
                skip_featured_events=skip_featured_events,
                skip_round_events=skip_round_events,
                skip_event_detail=skip_event_detail,
                skip_entities=skip_entities,
                skip_statistics=skip_statistics,
                skip_standings=skip_standings,
                skip_leaderboards=skip_leaderboards,
                timeout=timeout,
            )

    tasks = [asyncio.create_task(_run_one(unique_tournament_id)) for unique_tournament_id in unique_tournament_ids]
    items: list[_WorkerResult] = []
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
                "Default-tournaments worker failed for unique_tournament_id=%s: %s",
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
                "default_tournaments",
                (
                    f"progress={completed_count}/{total} succeeded={success_count} failed={failure_count} "
                    f"stage_failures={sum(result.stage_failures for result in items)}"
                ),
            )

    return tuple(items)


async def _run_tournament_worker(
    database: AsyncpgDatabase,
    *,
    competition_job: CompetitionIngestJob,
    event_list_job: EventListIngestJob,
    statistics_job: StatisticsIngestJob,
    standings_job: StandingsIngestJob,
    leaderboards_job: LeaderboardsIngestJob,
    event_detail_job: EventDetailIngestJob,
    entities_job: EntitiesIngestJob,
    sport_slug: str,
    sport_profile,
    unique_tournament_id: int,
    standings_scopes: tuple[str, ...],
    provider_ids: tuple[int, ...],
    stats_query: StatisticsQuery,
    seasons_per_tournament: int,
    event_concurrency: int,
    skip_featured_events: bool,
    skip_round_events: bool,
    skip_event_detail: bool,
    skip_entities: bool,
    skip_statistics: bool,
    skip_standings: bool,
    skip_leaderboards: bool,
    timeout: float,
) -> _WorkerResult:
    logger = logging.getLogger(__name__)
    stage_failures = 0
    completed_seasons = 0
    team_ids: list[int] = []
    player_ids: list[int] = []
    event_ids: list[int] = []
    player_overall_requests: list[PlayerOverallRequest] = []
    team_overall_requests: list[TeamOverallRequest] = []
    player_heatmap_requests: list[PlayerHeatmapRequest] = []
    team_graph_requests: list[TeamPerformanceGraphRequest] = []

    _progress("tournament", f"start unique_tournament_id={unique_tournament_id}")
    try:
        competition_result = await competition_job.run(unique_tournament_id, timeout=timeout)
    except Exception as exc:
        return _WorkerResult(
            unique_tournament_id=unique_tournament_id,
            success=False,
            season_ids=(),
            completed_seasons=0,
            discovered_team_ids=0,
            discovered_player_ids=0,
            discovered_event_ids=0,
            stage_failures=1,
            error=str(exc),
        )

    season_ids = _select_season_ids(competition_result, seasons_per_tournament=seasons_per_tournament)
    if not season_ids:
        return _WorkerResult(
            unique_tournament_id=unique_tournament_id,
            success=False,
            season_ids=(),
            completed_seasons=0,
            discovered_team_ids=0,
            discovered_player_ids=0,
            discovered_event_ids=0,
            stage_failures=1,
            error="No seasons discovered for unique tournament",
        )

    _progress(
        "tournament",
        (
            f"selected unique_tournament_id={unique_tournament_id} seasons={len(season_ids)} "
            f"values={','.join(str(value) for value in season_ids)}"
        ),
    )

    if not skip_featured_events:
        try:
            featured_result = await event_list_job.run_featured(
                unique_tournament_id,
                sport_slug=sport_slug,
                timeout=timeout,
            )
            _progress(
                "featured",
                f"unique_tournament_id={unique_tournament_id} events={featured_result.written.event_rows}",
            )
        except Exception as exc:
            stage_failures += 1
            logger.warning(
                "Default-tournaments featured-events failed for unique_tournament_id=%s: %s",
                unique_tournament_id,
                exc,
            )

    for index, season_id in enumerate(season_ids, start=1):
        _progress(
            "season",
            f"start unique_tournament_id={unique_tournament_id} {index}/{len(season_ids)} season_id={season_id}",
        )

        try:
            await competition_job.run(
                unique_tournament_id,
                season_id=season_id,
                include_seasons=False,
                timeout=timeout,
            )
        except Exception as exc:
            stage_failures += 1
            logger.warning(
                "Default-tournaments season-info failed for unique_tournament_id=%s season_id=%s: %s",
                unique_tournament_id,
                season_id,
                exc,
            )

        if not skip_statistics:
            try:
                statistics_result = await statistics_job.run(
                    unique_tournament_id,
                    season_id,
                    queries=(stats_query,),
                    include_info=True,
                    timeout=timeout,
                )
                _progress(
                    "statistics",
                    (
                        f"unique_tournament_id={unique_tournament_id} season_id={season_id} "
                        f"results={statistics_result.written.result_rows}"
                    ),
                )
            except Exception as exc:
                stage_failures += 1
                logger.warning(
                    "Default-tournaments statistics failed for unique_tournament_id=%s season_id=%s: %s",
                    unique_tournament_id,
                    season_id,
                    exc,
                )

        season_team_ids: tuple[int, ...] = ()
        if not skip_standings:
            try:
                standings_result = await standings_job.run_for_unique_tournament(
                    unique_tournament_id,
                    season_id,
                    scopes=standings_scopes,
                    timeout=timeout,
                )
                season_team_ids = tuple(
                    dict.fromkeys(row.team_id for row in standings_result.parsed.standing_rows if row.team_id is not None)
                )
                _progress(
                    "standings",
                    (
                        f"unique_tournament_id={unique_tournament_id} season_id={season_id} "
                        f"teams={len(season_team_ids)} rows={standings_result.written.standing_row_rows}"
                    ),
                )
            except Exception as exc:
                stage_failures += 1
                logger.warning(
                    "Default-tournaments standings failed for unique_tournament_id=%s season_id=%s: %s",
                    unique_tournament_id,
                    season_id,
                    exc,
                )

        if not season_team_ids:
            async with database.connection() as connection:
                season_team_ids = await _load_season_team_ids(
                    connection,
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                )

        if not skip_leaderboards:
            try:
                leaderboards_result = await leaderboards_job.run(
                    unique_tournament_id,
                    season_id,
                    sport_slug=sport_slug,
                    include_top_ratings=sport_profile.include_top_ratings,
                    include_player_of_the_season_race=sport_profile.include_player_of_the_season_race,
                    include_player_of_the_season=sport_profile.include_player_of_the_season,
                    include_venues=sport_profile.include_venues,
                    include_groups=sport_profile.include_groups,
                    include_team_of_the_week=sport_profile.include_team_of_the_week,
                    include_statistics_types=sport_profile.include_statistics_types,
                    team_top_players_team_ids=season_team_ids,
                    team_event_scopes=sport_profile.team_event_scopes,
                    include_trending_top_players=False,
                    timeout=timeout,
                )
                _progress(
                    "leaderboards",
                    (
                        f"unique_tournament_id={unique_tournament_id} season_id={season_id} "
                        f"snapshots={leaderboards_result.written.payload_snapshot_rows}"
                    ),
                )
            except Exception as exc:
                stage_failures += 1
                logger.warning(
                    "Default-tournaments leaderboards failed for unique_tournament_id=%s season_id=%s: %s",
                    unique_tournament_id,
                    season_id,
                    exc,
                )

        if season_team_ids:
            team_ids.extend(season_team_ids)
            team_overall_requests.extend(
                TeamOverallRequest(
                    team_id=team_id,
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                )
                for team_id in season_team_ids
            )
            team_graph_requests.extend(
                TeamPerformanceGraphRequest(
                    team_id=team_id,
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                )
                for team_id in season_team_ids
            )

        if not skip_round_events:
            try:
                async with database.connection() as connection:
                    round_numbers = await _load_round_numbers(
                        connection,
                        unique_tournament_id=unique_tournament_id,
                        season_id=season_id,
                    )
                for round_number in round_numbers:
                    await event_list_job.run_round(
                        unique_tournament_id,
                        season_id,
                        round_number,
                        sport_slug=sport_slug,
                        timeout=timeout,
                    )
                _progress(
                    "round_events",
                    (
                        f"unique_tournament_id={unique_tournament_id} season_id={season_id} "
                        f"rounds={len(round_numbers)}"
                    ),
                )
            except Exception as exc:
                stage_failures += 1
                logger.warning(
                    "Default-tournaments round-events failed for unique_tournament_id=%s season_id=%s: %s",
                    unique_tournament_id,
                    season_id,
                    exc,
                )

        async with database.connection() as connection:
            season_event_ids = await _load_season_event_ids(
                connection,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
            )
        if season_event_ids:
            event_ids.extend(season_event_ids)

        if season_event_ids and not skip_event_detail:
            event_detail_items = await _run_event_detail_batch(
                event_detail_job,
                event_ids=season_event_ids,
                provider_ids=provider_ids,
                concurrency=event_concurrency,
                timeout=timeout,
            )
            event_detail_failed = sum(1 for item in event_detail_items if not item.success)
            stage_failures += event_detail_failed
            _progress(
                "event_detail",
                (
                    f"unique_tournament_id={unique_tournament_id} season_id={season_id} "
                    f"succeeded={len(event_detail_items) - event_detail_failed}/{len(event_detail_items)} "
                    f"failed={event_detail_failed}"
                ),
            )

        async with database.connection() as connection:
            season_player_ids = await _load_season_player_ids(
                connection,
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                team_ids=season_team_ids,
            )
        if season_player_ids:
            player_ids.extend(season_player_ids)
            player_overall_requests.extend(
                PlayerOverallRequest(
                    player_id=player_id,
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                )
                for player_id in season_player_ids
            )
            player_heatmap_requests.extend(
                PlayerHeatmapRequest(
                    player_id=player_id,
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                )
                for player_id in season_player_ids
            )

        completed_seasons += 1
        _progress(
            "season",
            (
                f"done unique_tournament_id={unique_tournament_id} season_id={season_id} "
                f"teams={len(season_team_ids)} players={len(season_player_ids)} events={len(season_event_ids)}"
            ),
        )

    resolved_team_ids = tuple(dict.fromkeys(team_ids))
    resolved_player_ids = tuple(dict.fromkeys(player_ids))
    if not skip_entities and (resolved_team_ids or resolved_player_ids):
        try:
            entities_result = await entities_job.run(
                player_ids=resolved_player_ids,
                player_statistics_ids=resolved_player_ids,
                team_ids=resolved_team_ids,
                player_overall_requests=tuple(player_overall_requests),
                team_overall_requests=tuple(team_overall_requests),
                player_heatmap_requests=tuple(player_heatmap_requests),
                team_performance_graph_requests=tuple(team_graph_requests),
                include_player_statistics=True,
                include_player_statistics_seasons=True,
                include_player_transfer_history=True,
                include_team_statistics_seasons=True,
                include_team_player_statistics_seasons=True,
                timeout=timeout,
            )
            _progress(
                "entities",
                (
                    f"unique_tournament_id={unique_tournament_id} "
                    f"players={len(resolved_player_ids)} teams={len(resolved_team_ids)} "
                    f"player_season_stats={entities_result.written.player_season_statistics_rows}"
                ),
            )
        except Exception as exc:
            stage_failures += 1
            logger.warning(
                "Default-tournaments entities failed for unique_tournament_id=%s: %s",
                unique_tournament_id,
                exc,
            )

    _progress(
        "tournament",
        (
            f"done unique_tournament_id={unique_tournament_id} "
            f"completed_seasons={completed_seasons}/{len(season_ids)} "
            f"teams={len(resolved_team_ids)} players={len(resolved_player_ids)} "
            f"events={len(tuple(dict.fromkeys(event_ids)))} stage_failures={stage_failures}"
        ),
    )
    return _WorkerResult(
        unique_tournament_id=unique_tournament_id,
        success=True,
        season_ids=season_ids,
        completed_seasons=completed_seasons,
        discovered_team_ids=len(resolved_team_ids),
        discovered_player_ids=len(resolved_player_ids),
        discovered_event_ids=len(tuple(dict.fromkeys(event_ids))),
        stage_failures=stage_failures,
    )


def _select_unique_tournament_ids(
    unique_tournament_ids: Sequence[int],
    *,
    offset: int,
    limit: int | None,
    include_ids: Sequence[int] = (),
) -> tuple[int, ...]:
    selected = tuple(dict.fromkeys(int(value) for value in unique_tournament_ids))
    if include_ids:
        allow = set(include_ids)
        selected = tuple(value for value in selected if value in allow)
    selected = selected[max(offset, 0) :]
    resolved_limit = normalize_limit(limit)
    if resolved_limit is not None:
        selected = selected[:resolved_limit]
    return selected


def _select_season_ids(
    competition_result: CompetitionIngestResult,
    *,
    seasons_per_tournament: int,
) -> tuple[int, ...]:
    selected: list[int] = []
    for snapshot in competition_result.parsed.payload_snapshots:
        if snapshot.endpoint_pattern != UNIQUE_TOURNAMENT_SEASONS_ENDPOINT.pattern:
            continue
        seasons = snapshot.payload.get("seasons")
        if not isinstance(seasons, list):
            continue
        for season in seasons:
            if not isinstance(season, dict):
                continue
            season_id = season.get("id")
            if isinstance(season_id, int) and season_id not in selected:
                selected.append(season_id)

    if not selected:
        selected = sorted(
            {
                row.season_id
                for row in competition_result.parsed.unique_tournament_seasons
                if row.unique_tournament_id == competition_result.unique_tournament_id
            },
            reverse=True,
        )

    resolved_limit = normalize_limit(seasons_per_tournament)
    if resolved_limit is None:
        return tuple(selected)
    return tuple(selected[:resolved_limit])


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
                logger.warning("Default-tournaments event-detail failed for event_id=%s: %s", event_id, exc)
                return _EventDetailItem(event_id=event_id, success=False, error=str(exc))
            return _EventDetailItem(event_id=event_id, success=True)

    deduped_event_ids = tuple(dict.fromkeys(int(event_id) for event_id in event_ids))
    return tuple(await asyncio.gather(*(_run_one(event_id) for event_id in deduped_event_ids)))


async def _load_round_numbers(
    connection,
    *,
    unique_tournament_id: int,
    season_id: int,
) -> tuple[int, ...]:
    rows = await connection.fetch(
        """
        SELECT DISTINCT ri.round_number
        FROM event_round_info AS ri
        JOIN event AS e
            ON e.id = ri.event_id
        WHERE e.unique_tournament_id = $1
          AND e.season_id = $2
          AND ri.round_number IS NOT NULL
        ORDER BY ri.round_number
        """,
        unique_tournament_id,
        season_id,
    )
    return tuple(int(row["round_number"]) for row in rows if row["round_number"] is not None)


async def _load_season_event_ids(
    connection,
    *,
    unique_tournament_id: int,
    season_id: int,
) -> tuple[int, ...]:
    rows = await connection.fetch(
        """
        SELECT seed.id
        FROM (
            SELECT DISTINCT e.id, e.start_timestamp
            FROM event AS e
            WHERE e.unique_tournament_id = $1
              AND e.season_id = $2
        ) AS seed
        ORDER BY seed.start_timestamp NULLS LAST, seed.id
        """,
        unique_tournament_id,
        season_id,
    )
    return tuple(int(row["id"]) for row in rows if row["id"] is not None)


async def _load_season_team_ids(
    connection,
    *,
    unique_tournament_id: int,
    season_id: int,
) -> tuple[int, ...]:
    rows = await connection.fetch(
        """
        WITH team_pool AS (
            SELECT sr.team_id
            FROM standing_row AS sr
            JOIN standing AS s
                ON s.id = sr.standing_id
            JOIN tournament AS t
                ON t.id = s.tournament_id
            WHERE t.unique_tournament_id = $1
              AND s.season_id = $2

            UNION

            SELECT r.team_id
            FROM season_statistics_result AS r
            JOIN season_statistics_snapshot AS s
                ON s.id = r.snapshot_id
            WHERE s.unique_tournament_id = $1
              AND s.season_id = $2
              AND r.team_id IS NOT NULL

            UNION

            SELECT e.team_id
            FROM top_team_entry AS e
            JOIN top_team_snapshot AS s
                ON s.id = e.snapshot_id
            WHERE s.unique_tournament_id = $1
              AND s.season_id = $2

            UNION

            SELECT e.team_id
            FROM top_player_entry AS e
            JOIN top_player_snapshot AS s
                ON s.id = e.snapshot_id
            WHERE s.unique_tournament_id = $1
              AND s.season_id = $2
              AND e.team_id IS NOT NULL

            UNION

            SELECT e.home_team_id
            FROM event AS e
            WHERE e.unique_tournament_id = $1
              AND e.season_id = $2
              AND e.home_team_id IS NOT NULL

            UNION

            SELECT e.away_team_id
            FROM event AS e
            WHERE e.unique_tournament_id = $1
              AND e.season_id = $2
              AND e.away_team_id IS NOT NULL
        )
        SELECT DISTINCT team_id
        FROM team_pool
        WHERE team_id IS NOT NULL
        ORDER BY team_id
        """,
        unique_tournament_id,
        season_id,
    )
    return tuple(int(row["team_id"]) for row in rows if row["team_id"] is not None)


async def _load_season_player_ids(
    connection,
    *,
    unique_tournament_id: int,
    season_id: int,
    team_ids: Sequence[int] = (),
) -> tuple[int, ...]:
    rows = await connection.fetch(
        """
        WITH player_pool AS (
            SELECT r.player_id
            FROM season_statistics_result AS r
            JOIN season_statistics_snapshot AS s
                ON s.id = r.snapshot_id
            WHERE s.unique_tournament_id = $1
              AND s.season_id = $2
              AND r.player_id IS NOT NULL

            UNION

            SELECT e.player_id
            FROM top_player_entry AS e
            JOIN top_player_snapshot AS s
                ON s.id = e.snapshot_id
            WHERE s.unique_tournament_id = $1
              AND s.season_id = $2
              AND e.player_id IS NOT NULL

            UNION

            SELECT lp.player_id
            FROM event_lineup_player AS lp
            JOIN event AS e
                ON e.id = lp.event_id
            WHERE e.unique_tournament_id = $1
              AND e.season_id = $2
              AND lp.player_id IS NOT NULL

            UNION

            SELECT mp.player_id
            FROM event_lineup_missing_player AS mp
            JOIN event AS e
                ON e.id = mp.event_id
            WHERE e.unique_tournament_id = $1
              AND e.season_id = $2
              AND mp.player_id IS NOT NULL

            UNION

            SELECT p.id AS player_id
            FROM player AS p
            WHERE p.team_id = ANY($3::bigint[])
        )
        SELECT DISTINCT player_id
        FROM player_pool
        WHERE player_id IS NOT NULL
        ORDER BY player_id
        """,
        unique_tournament_id,
        season_id,
        list(team_ids),
    )
    return tuple(int(row["player_id"]) for row in rows if row["player_id"] is not None)


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
