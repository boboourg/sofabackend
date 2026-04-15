"""CLI for loading player/team enrichment data into PostgreSQL."""

from __future__ import annotations

import argparse
import asyncio
import sys

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
from .runtime import load_runtime_config
from .sofascore_client import SofascoreClient


def main() -> int:
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    parser = argparse.ArgumentParser(
        description="Fetch player/team enrichment Sofascore endpoints and persist them into PostgreSQL.",
    )
    parser.add_argument("--player-id", type=int, action="append", default=[], help="Fetch /player/{id}.")
    parser.add_argument("--team-id", type=int, action="append", default=[], help="Fetch /team/{id}.")
    parser.add_argument(
        "--player-overall",
        action="append",
        default=[],
        help="Fetch /player/{player}/unique-tournament/{ut}/season/{season}/statistics/overall as player:ut:season.",
    )
    parser.add_argument(
        "--team-overall",
        action="append",
        default=[],
        help="Fetch /team/{team}/unique-tournament/{ut}/season/{season}/statistics/overall as team:ut:season.",
    )
    parser.add_argument(
        "--player-heatmap",
        action="append",
        default=[],
        help="Fetch /player/{player}/unique-tournament/{ut}/season/{season}/heatmap/overall as player:ut:season.",
    )
    parser.add_argument(
        "--team-performance-graph",
        action="append",
        default=[],
        help="Fetch /unique-tournament/{ut}/season/{season}/team/{team}/team-performance-graph-data as team:ut:season.",
    )
    parser.add_argument("--skip-player-statistics", action="store_true", help="Skip /player/{id}/statistics.")
    parser.add_argument(
        "--skip-player-statistics-seasons",
        action="store_true",
        help="Skip /player/{id}/statistics/seasons.",
    )
    parser.add_argument(
        "--skip-player-transfer-history",
        action="store_true",
        help="Skip /player/{id}/transfer-history.",
    )
    parser.add_argument(
        "--skip-team-statistics-seasons",
        action="store_true",
        help="Skip /team/{id}/team-statistics/seasons.",
    )
    parser.add_argument(
        "--skip-team-player-statistics-seasons",
        action="store_true",
        help="Skip /team/{id}/player-statistics/seasons.",
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

    async with AsyncpgDatabase(database_config) as database:
        client = SofascoreClient(runtime_config)
        parser = EntitiesParser(client)
        repository = EntitiesRepository()
        job = EntitiesIngestJob(parser, repository, database)
        result = await job.run(
            player_ids=args.player_id,
            player_statistics_ids=args.player_id,
            team_ids=args.team_id,
            player_overall_requests=tuple(_parse_player_request(value) for value in args.player_overall),
            team_overall_requests=tuple(_parse_team_request(value) for value in args.team_overall),
            player_heatmap_requests=tuple(_parse_heatmap_request(value) for value in args.player_heatmap),
            team_performance_graph_requests=tuple(
                _parse_team_performance_graph_request(value) for value in args.team_performance_graph
            ),
            include_player_statistics=not args.skip_player_statistics,
            include_player_statistics_seasons=not args.skip_player_statistics_seasons,
            include_player_transfer_history=not args.skip_player_transfer_history,
            include_team_statistics_seasons=not args.skip_team_statistics_seasons,
            include_team_player_statistics_seasons=not args.skip_team_player_statistics_seasons,
            timeout=args.timeout,
        )

    print(
        "entities_ingest "
        f"players={result.written.player_rows} "
        f"teams={result.written.team_rows} "
        f"transfers={result.written.transfer_history_rows} "
        f"player_season_stats={result.written.player_season_statistics_rows} "
        f"entity_stat_seasons={result.written.entity_statistics_season_rows} "
        f"entity_stat_types={result.written.entity_statistics_type_rows} "
        f"season_stat_types={result.written.season_statistics_type_rows} "
        f"snapshots={result.written.payload_snapshot_rows}"
    )
    return 0


def _parse_player_request(value: str) -> PlayerOverallRequest:
    player_id, unique_tournament_id, season_id = _parse_triplet(value, "player-overall")
    return PlayerOverallRequest(
        player_id=player_id,
        unique_tournament_id=unique_tournament_id,
        season_id=season_id,
    )


def _parse_team_request(value: str) -> TeamOverallRequest:
    team_id, unique_tournament_id, season_id = _parse_triplet(value, "team-overall")
    return TeamOverallRequest(
        team_id=team_id,
        unique_tournament_id=unique_tournament_id,
        season_id=season_id,
    )


def _parse_heatmap_request(value: str) -> PlayerHeatmapRequest:
    player_id, unique_tournament_id, season_id = _parse_triplet(value, "player-heatmap")
    return PlayerHeatmapRequest(
        player_id=player_id,
        unique_tournament_id=unique_tournament_id,
        season_id=season_id,
    )


def _parse_team_performance_graph_request(value: str) -> TeamPerformanceGraphRequest:
    team_id, unique_tournament_id, season_id = _parse_triplet(value, "team-performance-graph")
    return TeamPerformanceGraphRequest(
        team_id=team_id,
        unique_tournament_id=unique_tournament_id,
        season_id=season_id,
    )


def _parse_triplet(value: str, label: str) -> tuple[int, int, int]:
    parts = value.split(":")
    if len(parts) != 3:
        raise SystemExit(f"{label} expects value in form id:unique_tournament_id:season_id")
    try:
        return int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError as exc:
        raise SystemExit(f"{label} expects integer triplets, got: {value}") from exc


if __name__ == "__main__":
    raise SystemExit(main())
