"""Focused slice pipeline for one league/team/player set with event-detail enrichment."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from dataclasses import dataclass
from datetime import datetime

from .competition_job import CompetitionIngestJob
from .competition_parser import CompetitionParser
from .competition_repository import CompetitionRepository
from .db import AsyncpgDatabase, load_database_config
from .entities_job import EntitiesIngestJob
from .entities_parser import (
    EntitiesParser,
    PlayerHeatmapRequest,
    PlayerOverallRequest,
    TeamOverallRequest,
    TeamPerformanceGraphRequest,
)
from .entities_repository import EntitiesRepository
from .event_detail_job import EventDetailIngestJob
from .event_detail_parser import EventDetailParser
from .event_detail_repository import EventDetailRepository
from .event_list_job import EventListIngestJob
from .event_list_parser import EventListParser
from .event_list_repository import EventListRepository
from .leaderboards_job import LeaderboardsIngestJob
from .leaderboards_parser import LeaderboardsParser
from .leaderboards_repository import LeaderboardsRepository
from .runtime import load_runtime_config
from .sofascore_client import SofascoreClient
from .sport_profiles import resolve_sport_profile
from .standings_job import StandingsIngestJob
from .standings_parser import StandingsParser
from .standings_repository import StandingsRepository
from .statistics_job import StatisticsIngestJob
from .statistics_parser import StatisticsParser, StatisticsQuery
from .statistics_repository import StatisticsRepository


@dataclass(frozen=True)
class SliceEventDetailItem:
    event_id: int
    success: bool
    error: str | None = None


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description=(
            "Load one focused Sofascore slice: league season + selected team(s) + selected player(s), "
            "including team-season event details and discovered team players."
        ),
    )
    parser.add_argument("--sport-slug", default="football", help="Target sport slug, e.g. football, basketball, tennis.")
    parser.add_argument("--unique-tournament-id", type=int, required=True, help="Target unique tournament id.")
    parser.add_argument("--season-id", type=int, required=True, help="Target season id.")
    parser.add_argument("--team-id", type=int, action="append", default=[], help="Repeatable target team id.")
    parser.add_argument("--player-id", type=int, action="append", default=[], help="Repeatable target player id.")
    parser.add_argument("--manager-id", type=int, action="append", default=[], help="Repeatable manager id to verify.")
    parser.add_argument("--event-id", type=int, action="append", default=[], help="Repeatable extra event id.")
    parser.add_argument("--provider-id", type=int, action="append", default=[], help="Repeatable odds provider id.")
    parser.add_argument("--standings-scope", action="append", default=[], help="Repeatable standings scope.")
    parser.add_argument("--statistics-limit", type=int, default=20, help="Statistics query limit.")
    parser.add_argument("--statistics-offset", type=int, default=0, help="Statistics query offset.")
    parser.add_argument("--statistics-order", default="-rating", help="Statistics query order.")
    parser.add_argument("--statistics-accumulation", default="total", help="Statistics query accumulation.")
    parser.add_argument("--statistics-group", default="summary", help="Statistics query group.")
    parser.add_argument("--statistics-field", action="append", default=[], help="Repeatable statistics field.")
    parser.add_argument("--statistics-filter", action="append", default=[], help="Repeatable statistics filter.")
    parser.add_argument("--event-concurrency", type=int, default=3, help="Concurrent event-detail workers.")
    parser.add_argument("--skip-featured-events", action="store_true", help="Skip featured-events ingestion.")
    parser.add_argument("--skip-round-events", action="store_true", help="Skip round-events ingestion for discovered rounds.")
    parser.add_argument("--skip-event-detail", action="store_true", help="Skip event-detail hydration for discovered team events.")
    parser.add_argument("--skip-entities", action="store_true", help="Skip entities hydration after slice discovery.")
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
    if not args.team_id:
        raise SystemExit("Pass at least one --team-id.")
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
    provider_ids = tuple(dict.fromkeys(args.provider_id or [1]))
    team_ids = tuple(dict.fromkeys(args.team_id or []))
    explicit_player_ids = tuple(dict.fromkeys(args.player_id or []))
    manager_ids = tuple(dict.fromkeys(args.manager_id or []))
    explicit_event_ids = tuple(dict.fromkeys(args.event_id or []))
    standings_scopes = tuple(dict.fromkeys(args.standings_scope or sport_profile.standings_scopes))
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
        client = SofascoreClient(runtime_config)

        competition_job = CompetitionIngestJob(CompetitionParser(client), CompetitionRepository(), database)
        event_list_job = EventListIngestJob(EventListParser(client), EventListRepository(), database)
        statistics_job = StatisticsIngestJob(StatisticsParser(client), StatisticsRepository(), database)
        standings_job = StandingsIngestJob(StandingsParser(client), StandingsRepository(), database)
        leaderboards_job = LeaderboardsIngestJob(LeaderboardsParser(client), LeaderboardsRepository(), database)
        event_detail_job = EventDetailIngestJob(EventDetailParser(client), EventDetailRepository(), database)
        entities_job = EntitiesIngestJob(EntitiesParser(client), EntitiesRepository(), database)

        _progress(
            "slice_pipeline",
            (
                f"start sport={sport_slug} unique_tournament_id={args.unique_tournament_id} season_id={args.season_id} "
                f"teams={','.join(str(value) for value in team_ids)} "
                f"players={','.join(str(value) for value in explicit_player_ids) or '-'}"
            ),
        )

        _progress("competition", f"start unique_tournament_id={args.unique_tournament_id} season_id={args.season_id}")
        competition_result = await competition_job.run(
            args.unique_tournament_id,
            season_id=args.season_id,
            timeout=args.timeout,
        )
        _progress("competition", f"done snapshots={competition_result.written.payload_snapshot_rows}")

        if not args.skip_featured_events:
            _progress("event_list", f"featured start unique_tournament_id={args.unique_tournament_id}")
            featured_result = await event_list_job.run_featured(
                args.unique_tournament_id,
                sport_slug=sport_slug,
                timeout=args.timeout,
            )
            _progress(
                "event_list",
                (
                    "featured done "
                    f"events={featured_result.written.event_rows} "
                    f"snapshots={featured_result.written.payload_snapshot_rows}"
                ),
            )

        _progress(
            "statistics",
            (
                f"start unique_tournament_id={args.unique_tournament_id} season_id={args.season_id} "
                f"order={args.statistics_order} accumulation={args.statistics_accumulation} group={args.statistics_group}"
            ),
        )
        statistics_result = await statistics_job.run(
            args.unique_tournament_id,
            args.season_id,
            queries=(stats_query,),
            include_info=True,
            timeout=args.timeout,
        )
        _progress(
            "statistics",
            f"done snapshots={statistics_result.written.snapshot_rows} results={statistics_result.written.result_rows}",
        )

        _progress(
            "standings",
            (
                f"start unique_tournament_id={args.unique_tournament_id} "
                f"season_id={args.season_id} scopes={','.join(standings_scopes)}"
            ),
        )
        standings_result = await standings_job.run_for_unique_tournament(
            args.unique_tournament_id,
            args.season_id,
            scopes=standings_scopes,
            timeout=args.timeout,
        )
        _progress(
            "standings",
            f"done standings={standings_result.written.standing_rows} rows={standings_result.written.standing_row_rows}",
        )

        _progress("leaderboards", f"start unique_tournament_id={args.unique_tournament_id} season_id={args.season_id}")
        leaderboards_result = await leaderboards_job.run(
            args.unique_tournament_id,
            args.season_id,
            sport_slug=sport_slug,
            include_top_ratings=sport_profile.include_top_ratings,
            include_player_of_the_season_race=sport_profile.include_player_of_the_season_race,
            include_player_of_the_season=sport_profile.include_player_of_the_season,
            include_venues=sport_profile.include_venues,
            include_groups=sport_profile.include_groups,
            include_team_of_the_week=sport_profile.include_team_of_the_week,
            include_statistics_types=sport_profile.include_statistics_types,
            team_top_players_team_ids=team_ids,
            team_event_scopes=sport_profile.team_event_scopes,
            include_trending_top_players=False,
            timeout=args.timeout,
        )
        _progress("leaderboards", f"done snapshots={leaderboards_result.written.payload_snapshot_rows}")

        async with database.connection() as connection:
            discovered_rounds = await _load_round_numbers(
                connection,
                unique_tournament_id=args.unique_tournament_id,
                season_id=args.season_id,
            )
        _progress(
            "event_list",
            f"discovered rounds={len(discovered_rounds)} values={','.join(str(value) for value in discovered_rounds) or '-'}",
        )

        round_results = []
        if not args.skip_round_events:
            for index, round_number in enumerate(discovered_rounds, start=1):
                _progress("event_list", f"round start {index}/{len(discovered_rounds)} round={round_number}")
                round_results.append(
                    await event_list_job.run_round(
                        args.unique_tournament_id,
                        args.season_id,
                        round_number,
                        sport_slug=sport_slug,
                        timeout=args.timeout,
                    )
                )
            if discovered_rounds:
                _progress("event_list", f"round done count={len(round_results)}")

        async with database.connection() as connection:
            discovered_event_ids = await _load_team_event_ids(
                connection,
                unique_tournament_id=args.unique_tournament_id,
                season_id=args.season_id,
                team_ids=team_ids,
            )
        event_ids = tuple(dict.fromkeys((*discovered_event_ids, *explicit_event_ids)))
        _progress(
            "event_detail",
            f"discovered team_events={len(event_ids)} values={','.join(str(value) for value in event_ids[:10])}{'...' if len(event_ids) > 10 else ''}",
        )

        event_detail_items: tuple[SliceEventDetailItem, ...] = ()
        if event_ids and not args.skip_event_detail:
            _progress(
                "event_detail",
                f"start events={len(event_ids)} concurrency={max(args.event_concurrency, 1)} provider_ids={','.join(str(value) for value in provider_ids)}",
            )
            event_detail_items = await _run_event_detail_batch(
                event_detail_job,
                event_ids=event_ids,
                provider_ids=provider_ids,
                concurrency=args.event_concurrency,
                timeout=args.timeout,
            )
            event_detail_succeeded = sum(1 for item in event_detail_items if item.success)
            event_detail_failed = len(event_detail_items) - event_detail_succeeded
            _progress(
                "event_detail",
                f"done succeeded={event_detail_succeeded}/{len(event_detail_items)} failed={event_detail_failed}",
            )

        entities_result = None
        discovered_player_ids: tuple[int, ...] = ()
        if not args.skip_entities:
            async with database.connection() as connection:
                discovered_player_ids = await _load_team_player_ids(
                    connection,
                    unique_tournament_id=args.unique_tournament_id,
                    season_id=args.season_id,
                    team_ids=team_ids,
                )
            resolved_player_ids = tuple(dict.fromkeys((*explicit_player_ids, *discovered_player_ids)))
            _progress(
                "entities",
                (
                    f"discovered players={len(resolved_player_ids)} "
                    f"seeded_from_team={len(discovered_player_ids)} "
                    f"explicit={len(explicit_player_ids)}"
                ),
            )
            player_overall_requests = tuple(
                PlayerOverallRequest(
                    player_id=player_id,
                    unique_tournament_id=args.unique_tournament_id,
                    season_id=args.season_id,
                )
                for player_id in resolved_player_ids
            )
            player_heatmap_requests = tuple(
                PlayerHeatmapRequest(
                    player_id=player_id,
                    unique_tournament_id=args.unique_tournament_id,
                    season_id=args.season_id,
                )
                for player_id in resolved_player_ids
            )
            team_overall_requests = tuple(
                TeamOverallRequest(
                    team_id=team_id,
                    unique_tournament_id=args.unique_tournament_id,
                    season_id=args.season_id,
                )
                for team_id in team_ids
            )
            team_graph_requests = tuple(
                TeamPerformanceGraphRequest(
                    team_id=team_id,
                    unique_tournament_id=args.unique_tournament_id,
                    season_id=args.season_id,
                )
                for team_id in team_ids
            )
            _progress(
                "entities",
                f"start players={len(resolved_player_ids)} teams={len(team_ids)} player_overall={len(player_overall_requests)}",
            )
            entities_result = await entities_job.run(
                player_ids=resolved_player_ids,
                player_statistics_ids=resolved_player_ids,
                team_ids=team_ids,
                player_overall_requests=player_overall_requests,
                team_overall_requests=team_overall_requests,
                player_heatmap_requests=player_heatmap_requests,
                team_performance_graph_requests=team_graph_requests,
                include_player_statistics=True,
                include_player_statistics_seasons=True,
                include_player_transfer_history=True,
                include_team_statistics_seasons=True,
                include_team_player_statistics_seasons=True,
                timeout=args.timeout,
            )
            _progress(
                "entities",
                (
                    "done "
                    f"players={entities_result.written.player_rows} "
                    f"player_season_stats={entities_result.written.player_season_statistics_rows} "
                    f"snapshots={entities_result.written.payload_snapshot_rows}"
                ),
            )

        found_manager_ids: tuple[int, ...] = ()
        if manager_ids:
            async with database.connection() as connection:
                found_manager_ids = await _load_existing_manager_ids(connection, manager_ids=manager_ids)
            _progress(
                "managers",
                f"requested={len(manager_ids)} found={len(found_manager_ids)} values={','.join(str(value) for value in found_manager_ids) or '-'}",
            )

    event_detail_succeeded = sum(1 for item in event_detail_items if item.success)
    event_detail_failed = len(event_detail_items) - event_detail_succeeded
    print(
        "slice_pipeline "
        f"competition_snapshots={competition_result.written.payload_snapshot_rows} "
        f"statistics_results={statistics_result.written.result_rows} "
        f"standings={standings_result.written.standing_rows} "
        f"leaderboard_snapshots={leaderboards_result.written.payload_snapshot_rows} "
        f"round_jobs={len(round_results)} "
        f"team_events={len(event_ids)} "
        f"event_detail_succeeded={event_detail_succeeded}/{len(event_detail_items)} "
        f"discovered_players={len(discovered_player_ids)} "
        f"entity_player_stats={0 if entities_result is None else entities_result.written.player_season_statistics_rows} "
        f"found_managers={len(found_manager_ids)}"
    )
    return 0 if event_detail_failed == 0 else 1


async def _run_event_detail_batch(
    event_detail_job: EventDetailIngestJob,
    *,
    event_ids: tuple[int, ...],
    provider_ids: tuple[int, ...],
    concurrency: int,
    timeout: float,
) -> tuple[SliceEventDetailItem, ...]:
    semaphore = asyncio.Semaphore(max(concurrency, 1))

    async def _run_one(event_id: int) -> SliceEventDetailItem:
        async with semaphore:
            try:
                await event_detail_job.run(event_id, provider_ids=provider_ids, timeout=timeout)
            except Exception as exc:
                logging.getLogger(__name__).warning("Slice event-detail failed for event_id=%s: %s", event_id, exc)
                return SliceEventDetailItem(event_id=event_id, success=False, error=str(exc))
            return SliceEventDetailItem(event_id=event_id, success=True)

    return tuple(await asyncio.gather(*(_run_one(event_id) for event_id in event_ids)))


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


async def _load_team_event_ids(
    connection,
    *,
    unique_tournament_id: int,
    season_id: int,
    team_ids: tuple[int, ...],
) -> tuple[int, ...]:
    rows = await connection.fetch(
        """
        SELECT seed.id
        FROM (
            SELECT DISTINCT e.id, e.start_timestamp
            FROM event AS e
            WHERE e.unique_tournament_id = $1
              AND e.season_id = $2
              AND (
                    e.home_team_id = ANY($3::bigint[])
                 OR e.away_team_id = ANY($3::bigint[])
              )
        ) AS seed
        ORDER BY seed.start_timestamp NULLS LAST, seed.id
        """,
        unique_tournament_id,
        season_id,
        list(team_ids),
    )
    return tuple(int(row["id"]) for row in rows if row["id"] is not None)


async def _load_team_player_ids(
    connection,
    *,
    unique_tournament_id: int,
    season_id: int,
    team_ids: tuple[int, ...],
) -> tuple[int, ...]:
    rows = await connection.fetch(
        """
        WITH player_pool AS (
            SELECT p.id AS player_id
            FROM player AS p
            WHERE p.team_id = ANY($3::bigint[])

            UNION

            SELECT r.player_id
            FROM season_statistics_result AS r
            JOIN season_statistics_snapshot AS s
                ON s.id = r.snapshot_id
            WHERE s.unique_tournament_id = $1
              AND s.season_id = $2
              AND r.player_id IS NOT NULL
              AND r.team_id = ANY($3::bigint[])

            UNION

            SELECT lp.player_id
            FROM event_lineup_player AS lp
            JOIN event AS e
                ON e.id = lp.event_id
            WHERE e.unique_tournament_id = $1
              AND e.season_id = $2
              AND lp.player_id IS NOT NULL
              AND lp.team_id = ANY($3::bigint[])

            UNION

            SELECT mp.player_id
            FROM event_lineup_missing_player AS mp
            JOIN event AS e
                ON e.id = mp.event_id
            WHERE e.unique_tournament_id = $1
              AND e.season_id = $2
              AND mp.player_id IS NOT NULL
              AND (
                    (mp.side = 'home' AND e.home_team_id = ANY($3::bigint[]))
                 OR (mp.side = 'away' AND e.away_team_id = ANY($3::bigint[]))
              )
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


async def _load_existing_manager_ids(connection, *, manager_ids: tuple[int, ...]) -> tuple[int, ...]:
    rows = await connection.fetch(
        """
        SELECT id
        FROM manager
        WHERE id = ANY($1::bigint[])
        ORDER BY id
        """,
        list(manager_ids),
    )
    return tuple(int(row["id"]) for row in rows if row["id"] is not None)


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
