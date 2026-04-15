"""Fast targeted pipeline for smoke-loading one league/team/player slice into PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio
import sys
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
from .leaderboards_job import LeaderboardsIngestJob
from .leaderboards_parser import LeaderboardsParser
from .leaderboards_repository import LeaderboardsRepository
from .runtime import load_runtime_config
from .sofascore_client import SofascoreClient
from .standings_job import StandingsIngestJob
from .standings_parser import StandingsParser
from .standings_repository import StandingsRepository
from .statistics_job import StatisticsIngestJob
from .statistics_parser import StatisticsParser, StatisticsQuery
from .statistics_repository import StatisticsRepository


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description=(
            "Load one targeted Sofascore slice fast: a specific unique tournament/season plus "
            "selected teams, players, and optional event detail."
        ),
    )
    parser.add_argument("--unique-tournament-id", type=int, default=None, help="Target unique tournament id.")
    parser.add_argument("--season-id", type=int, default=None, help="Target season id.")
    parser.add_argument("--team-id", type=int, action="append", default=[], help="Repeatable target team id.")
    parser.add_argument("--player-id", type=int, action="append", default=[], help="Repeatable target player id.")
    parser.add_argument("--event-id", type=int, action="append", default=[], help="Repeatable target event id.")
    parser.add_argument("--standings-scope", action="append", default=[], help="Repeatable standings scope.")
    parser.add_argument(
        "--provider-id",
        type=int,
        action="append",
        default=[],
        help="Odds provider id for targeted event-detail runs. Defaults to provider 1.",
    )
    parser.add_argument("--skip-competition", action="store_true", help="Skip unique tournament hydration.")
    parser.add_argument("--skip-statistics", action="store_true", help="Skip season statistics.")
    parser.add_argument("--skip-standings", action="store_true", help="Skip standings.")
    parser.add_argument("--skip-leaderboards", action="store_true", help="Skip leaderboards.")
    parser.add_argument("--skip-entities", action="store_true", help="Skip team/player entities.")
    parser.add_argument("--skip-event-detail", action="store_true", help="Skip targeted event-detail.")
    parser.add_argument("--statistics-limit", type=int, default=20, help="Statistics query limit.")
    parser.add_argument("--statistics-offset", type=int, default=0, help="Statistics query offset.")
    parser.add_argument("--statistics-order", default="-rating", help="Statistics query order.")
    parser.add_argument("--statistics-accumulation", default="total", help="Statistics query accumulation.")
    parser.add_argument("--statistics-group", default="summary", help="Statistics query group.")
    parser.add_argument("--statistics-field", action="append", default=[], help="Repeatable statistics field.")
    parser.add_argument("--statistics-filter", action="append", default=[], help="Repeatable statistics filter.")
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
    _validate_args(args)
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
    team_ids = tuple(dict.fromkeys(args.team_id or []))
    player_ids = tuple(dict.fromkeys(args.player_id or []))
    event_ids = tuple(dict.fromkeys(args.event_id or []))
    standings_scopes = tuple(dict.fromkeys(args.standings_scope or ["total", "home", "away"]))

    stats_query = StatisticsQuery(
        limit=args.statistics_limit,
        offset=args.statistics_offset,
        order=args.statistics_order,
        accumulation=args.statistics_accumulation,
        group=args.statistics_group,
        fields=tuple(dict.fromkeys(args.statistics_field)),
        filters=tuple(dict.fromkeys(args.statistics_filter)),
    )
    statistics_queries = (stats_query,)

    async with AsyncpgDatabase(database_config) as database:
        client = SofascoreClient(runtime_config)

        competition_job = CompetitionIngestJob(CompetitionParser(client), CompetitionRepository(), database)
        statistics_job = StatisticsIngestJob(StatisticsParser(client), StatisticsRepository(), database)
        standings_job = StandingsIngestJob(StandingsParser(client), StandingsRepository(), database)
        leaderboards_job = LeaderboardsIngestJob(LeaderboardsParser(client), LeaderboardsRepository(), database)
        entities_job = EntitiesIngestJob(EntitiesParser(client), EntitiesRepository(), database)
        event_detail_job = EventDetailIngestJob(EventDetailParser(client), EventDetailRepository(), database)

        competition_result = None
        statistics_result = None
        standings_result = None
        leaderboards_result = None
        entities_result = None
        event_results = []

        if args.unique_tournament_id is not None and not args.skip_competition:
            _progress("competition", f"start unique_tournament_id={args.unique_tournament_id} season_id={args.season_id}")
            competition_result = await competition_job.run(
                args.unique_tournament_id,
                season_id=args.season_id,
                timeout=args.timeout,
            )
            _progress("competition", f"done snapshots={competition_result.written.payload_snapshot_rows}")

        if args.unique_tournament_id is not None and args.season_id is not None and not args.skip_statistics:
            _progress(
                "statistics",
                (
                    f"start unique_tournament_id={args.unique_tournament_id} season_id={args.season_id} "
                    f"order={args.statistics_order} accumulation={args.statistics_accumulation} "
                    f"group={args.statistics_group}"
                ),
            )
            statistics_result = await statistics_job.run(
                args.unique_tournament_id,
                args.season_id,
                queries=statistics_queries,
                include_info=True,
                timeout=args.timeout,
            )
            _progress(
                "statistics",
                f"done snapshots={statistics_result.written.snapshot_rows} results={statistics_result.written.result_rows}",
            )

        if args.unique_tournament_id is not None and args.season_id is not None and not args.skip_standings:
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

        if args.unique_tournament_id is not None and args.season_id is not None and not args.skip_leaderboards:
            _progress("leaderboards", f"start unique_tournament_id={args.unique_tournament_id} season_id={args.season_id}")
            leaderboards_result = await leaderboards_job.run(
                args.unique_tournament_id,
                args.season_id,
                team_top_players_team_ids=team_ids,
                team_event_scopes=("home", "away", "total"),
                include_trending_top_players=False,
                timeout=args.timeout,
            )
            _progress("leaderboards", f"done snapshots={leaderboards_result.written.payload_snapshot_rows}")

        if not args.skip_entities and (player_ids or team_ids):
            _progress(
                "entities",
                (
                    f"start players={len(player_ids)} teams={len(team_ids)} "
                    f"season_context={'yes' if args.unique_tournament_id is not None and args.season_id is not None else 'no'}"
                ),
            )
            player_overall_requests = ()
            team_overall_requests = ()
            player_heatmap_requests = ()
            team_performance_graph_requests = ()
            if args.unique_tournament_id is not None and args.season_id is not None:
                player_overall_requests = tuple(
                    PlayerOverallRequest(
                        player_id=player_id,
                        unique_tournament_id=args.unique_tournament_id,
                        season_id=args.season_id,
                    )
                    for player_id in player_ids
                )
                team_overall_requests = tuple(
                    TeamOverallRequest(
                        team_id=team_id,
                        unique_tournament_id=args.unique_tournament_id,
                        season_id=args.season_id,
                    )
                    for team_id in team_ids
                )
                player_heatmap_requests = tuple(
                    PlayerHeatmapRequest(
                        player_id=player_id,
                        unique_tournament_id=args.unique_tournament_id,
                        season_id=args.season_id,
                    )
                    for player_id in player_ids
                )
                team_performance_graph_requests = tuple(
                    TeamPerformanceGraphRequest(
                        team_id=team_id,
                        unique_tournament_id=args.unique_tournament_id,
                        season_id=args.season_id,
                    )
                    for team_id in team_ids
                )

            entities_result = await entities_job.run(
                player_ids=player_ids,
                player_statistics_ids=player_ids,
                team_ids=team_ids,
                player_overall_requests=player_overall_requests,
                team_overall_requests=team_overall_requests,
                player_heatmap_requests=player_heatmap_requests,
                team_performance_graph_requests=team_performance_graph_requests,
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
                    f"done players={entities_result.written.player_rows} "
                    f"player_season_stats={entities_result.written.player_season_statistics_rows} "
                    f"snapshots={entities_result.written.payload_snapshot_rows}"
                ),
            )

        if event_ids and not args.skip_event_detail:
            for event_id in event_ids:
                _progress("event_detail", f"start event_id={event_id}")
                event_results.append(
                    await event_detail_job.run(event_id, provider_ids=provider_ids, timeout=args.timeout)
                )
                _progress("event_detail", f"done event_id={event_id} snapshots={event_results[-1].written.payload_snapshot_rows}")

    print(
        "targeted_pipeline "
        f"competition_snapshots={0 if competition_result is None else competition_result.written.payload_snapshot_rows} "
        f"statistics_results={0 if statistics_result is None else statistics_result.written.result_rows} "
        f"standings={0 if standings_result is None else standings_result.written.standing_rows} "
        f"leaderboard_snapshots={0 if leaderboards_result is None else leaderboards_result.written.payload_snapshot_rows} "
        f"entity_player_stats={0 if entities_result is None else entities_result.written.player_season_statistics_rows} "
        f"event_details={len(event_results)}"
    )
    return 0


def _validate_args(args: argparse.Namespace) -> None:
    if (
        args.unique_tournament_id is None
        and not args.team_id
        and not args.player_id
        and not args.event_id
    ):
        raise SystemExit("Pass at least one target: --unique-tournament-id, --team-id, --player-id, or --event-id.")
    if args.season_id is not None and args.unique_tournament_id is None:
        raise SystemExit("--season-id requires --unique-tournament-id.")


def _progress(stage: str, message: str) -> None:
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    print(f"[{timestamp}] {stage}: {message}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
