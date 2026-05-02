"""Build a local OpenAPI/Swagger document for the multi-sport dataset."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit

from .db import connect_with_fallback, load_database_config
from .endpoints import (
    CATEGORY_UNIQUE_TOURNAMENTS_ENDPOINT,
    COMPETITION_ENDPOINTS,
    ENTITIES_ENDPOINTS,
    EVENT_BASEBALL_INNINGS_ENDPOINT,
    EVENT_BASEBALL_PITCHES_ENDPOINT,
    EVENT_DETAIL_ENDPOINTS,
    EVENT_ESPORTS_GAMES_ENDPOINT,
    EVENT_LIST_ENDPOINTS,
    EVENT_POINT_BY_POINT_ENDPOINT,
    EVENT_SHOTMAP_ENDPOINT,
    SPORT_ALL_EVENT_COUNT_ENDPOINT,
    EVENT_TENNIS_POWER_ENDPOINT,
    LEADERBOARDS_ENDPOINTS,
    LOCAL_API_SUPPORTED_SPORTS,
    UNIQUE_TOURNAMENT_SEASON_CUPTREES_ENDPOINT,
    UNIQUE_TOURNAMENT_SEASON_ROUNDS_ENDPOINT,
    STANDINGS_ENDPOINTS,
    STATISTICS_ENDPOINTS,
    UNIQUE_TOURNAMENT_SCHEDULED_EVENTS_ENDPOINT,
    local_api_endpoints,
    sport_categories_all_endpoint,
    sport_categories_endpoint,
    sport_date_categories_endpoint,
    sport_live_events_endpoint,
    sport_scheduled_events_endpoint,
    sport_scheduled_tournaments_endpoint,
    sport_trending_top_players_endpoint,
)
from .entities_parser import _PLAYER_STATISTICS_INTEGER_COLUMNS, _PLAYER_STATISTICS_METRIC_COLUMN_MAP
from .sport_profiles import resolve_sport_profile
from .statistics_parser import _INTEGER_METRIC_COLUMNS, _METRIC_COLUMN_MAP

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "local_swagger"
DEFAULT_OPENAPI_FILE = "multisport.openapi.json"
DEFAULT_VIEWER_FILE = "index.html"
DEFAULT_OPENAPI_CACHE_DIR = PROJECT_ROOT / ".cache" / "local_api"
DEFAULT_LOCAL_API_BASE_URLS = ("http://127.0.0.1:8000", "http://localhost:8000")
_LOCAL_SWAGGER_HOSTS = {"127.0.0.1", "localhost", "0.0.0.0", "::1"}
_OPENAPI_CACHE_INPUTS = (
    PROJECT_ROOT / "schema_inspector" / "local_swagger_builder.py",
    PROJECT_ROOT / "schema_inspector" / "endpoints.py",
    PROJECT_ROOT / "schema_inspector" / "entities_parser.py",
    PROJECT_ROOT / "schema_inspector" / "statistics_parser.py",
    PROJECT_ROOT / "schema_inspector" / "sport_profiles.py",
)
_LOCAL_API_TAGS = (
    ("Operations", "Operational monitoring endpoints for the Hybrid ETL control plane and Autopilot services."),
    ("Event List", "Sport-specific scheduled/live/tournament event feeds."),
    ("Event Detail", "Event detail, lineups, odds, managers, h2h and other event subresources."),
    ("Special Routes", "Sport-specific specialist payloads such as tennis power, baseball innings, pitches, shotmaps and esports games."),
    ("Categories", "Sport-specific category and tournament discovery endpoints."),
    ("Competition", "Unique tournament and season metadata."),
    ("Standings", "Standings and standings rows by unique tournament or tournament."),
    ("Statistics", "Season statistics config and leaderboard-style snapshot results."),
    ("Entities", "Player/team entities, player season statistics and enrichment endpoints."),
    ("Leaderboards", "Top players, top teams, venues, groups, team-of-the-week and related season widgets."),
)

_SUPPORTED_TABLES = (
    "api_request_log",
    "api_payload_snapshot",
    "api_snapshot_head",
    "category_daily_summary",
    "category_daily_unique_tournament",
    "category_daily_team",
    "coverage_ledger",
    "endpoint_capability_rollup",
    "event",
    "event_comment",
    "event_comment_feed",
    "event_duel",
    "event_graph",
    "event_graph_point",
    "event_lineup",
    "event_lineup_player",
    "event_live_state_history",
    "event_manager_assignment",
    "event_market",
    "event_pregame_form",
    "event_team_heatmap",
    "event_team_heatmap_point",
    "event_terminal_state",
    "event_vote_option",
    "event_winning_odds",
    "etl_job_effect",
    "etl_job_run",
    "period",
    "player",
    "player_season_statistics",
    "player_transfer_history",
    "season",
    "season_group",
    "season_round",
    "season_cup_tree",
    "season_cup_tree_round",
    "season_cup_tree_block",
    "season_cup_tree_participant",
    "season_player_of_the_season",
    "season_statistics_config",
    "season_statistics_snapshot",
    "season_statistics_type",
    "standing",
    "team",
    "team_of_the_week",
    "top_player_snapshot",
    "top_team_snapshot",
    "unique_tournament",
    "venue",
)


@dataclass(frozen=True)
class SwaggerDataSummary:
    """Runtime data counts used to annotate the generated OpenAPI document."""

    generated_at: str
    table_counts: dict[str, int]
    snapshot_counts: dict[str, int]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a local OpenAPI/Swagger document for the local multi-sport dataset. "
            "Paths mirror the exact Sofascore-style templates already supported by the project."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Directory where the generated OpenAPI file and viewer HTML will be written.",
    )
    parser.add_argument(
        "--openapi-file",
        default=DEFAULT_OPENAPI_FILE,
        help="Output filename for the generated OpenAPI JSON document.",
    )
    parser.add_argument(
        "--viewer-file",
        default=DEFAULT_VIEWER_FILE,
        help="Output filename for the generated Swagger UI HTML page.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Optional PostgreSQL DSN override. Falls back to SOFASCORE_DATABASE_URL / DATABASE_URL / POSTGRES_DSN.",
    )
    parser.add_argument(
        "--skip-db-summary",
        action="store_true",
        help="Build the document without querying PostgreSQL for live row/snapshot counts.",
    )
    parser.add_argument(
        "--public-base-url",
        action="append",
        default=None,
        help=(
            "Optional public base URL to publish in the OpenAPI servers list. "
            "Can be repeated and also falls back to LOCAL_API_PUBLIC_BASE_URLS."
        ),
    )
    args = parser.parse_args()
    return asyncio.run(_run(args))


async def _run(args: argparse.Namespace) -> int:
    output_dir = Path(args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    summary = await _load_summary(args.database_url) if not args.skip_db_summary else _empty_summary()
    document = build_openapi_document(
        summary,
        base_urls=resolve_openapi_base_urls(public_base_urls=tuple(args.public_base_url or ())),
    )

    openapi_path = output_dir / args.openapi_file
    viewer_path = output_dir / args.viewer_file
    openapi_path.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    viewer_path.write_text(_build_viewer_html(args.openapi_file), encoding="utf-8")

    print(
        "local_swagger "
        f"openapi={openapi_path} "
        f"viewer={viewer_path} "
        f"paths={len(document['paths'])} "
        f"schemas={len(document['components']['schemas'])}",
        flush=True,
    )
    return 0


async def _load_summary(database_url: str | None) -> SwaggerDataSummary:
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    try:
        import asyncpg
    except ImportError:
        return SwaggerDataSummary(generated_at=generated_at, table_counts={}, snapshot_counts={})

    database_config = load_database_config(dsn=database_url)
    connection = await connect_with_fallback(database_config)
    try:
        table_counts: dict[str, int] = {}
        for table_name in _SUPPORTED_TABLES:
            table_counts[table_name] = int(
                await connection.fetchval(f"SELECT COUNT(*) FROM {_quote_identifier(table_name)}")
            )

        snapshot_rows = await connection.fetch(
            """
            SELECT endpoint_pattern, COUNT(*) AS row_count
            FROM api_payload_snapshot
            GROUP BY endpoint_pattern
            """
        )
        snapshot_counts = {
            str(row["endpoint_pattern"]): int(row["row_count"])
            for row in snapshot_rows
            if row["endpoint_pattern"] is not None
        }
    finally:
        await connection.close()

    return SwaggerDataSummary(
        generated_at=generated_at,
        table_counts=table_counts,
        snapshot_counts=snapshot_counts,
    )


def _empty_summary() -> SwaggerDataSummary:
    return SwaggerDataSummary(
        generated_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        table_counts={},
        snapshot_counts={},
    )


def resolve_openapi_cache_path(
    *,
    base_urls: tuple[str, ...] | list[str] | None = None,
    cache_dir: Path | None = None,
) -> Path:
    resolved_cache_dir = Path(cache_dir or DEFAULT_OPENAPI_CACHE_DIR)
    cache_key = _openapi_cache_key(tuple(base_urls or resolve_openapi_base_urls()))
    return resolved_cache_dir / f"{cache_key}.openapi.json"


def load_cached_openapi_bytes(
    *,
    base_urls: tuple[str, ...] | list[str] | None = None,
    cache_dir: Path | None = None,
) -> bytes | None:
    cache_path = resolve_openapi_cache_path(base_urls=base_urls, cache_dir=cache_dir)
    try:
        return cache_path.read_bytes()
    except FileNotFoundError:
        return None


def write_cached_openapi_bytes(
    document: dict[str, Any],
    *,
    base_urls: tuple[str, ...] | list[str] | None = None,
    cache_dir: Path | None = None,
) -> bytes:
    cache_path = resolve_openapi_cache_path(base_urls=base_urls, cache_dir=cache_dir)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(document, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    cache_path.write_bytes(payload)
    return payload


def _openapi_cache_key(base_urls: tuple[str, ...]) -> str:
    hasher = hashlib.sha256()
    for path in _OPENAPI_CACHE_INPUTS:
        stat = path.stat()
        hasher.update(path.relative_to(PROJECT_ROOT).as_posix().encode("utf-8"))
        hasher.update(str(stat.st_mtime_ns).encode("utf-8"))
        hasher.update(str(stat.st_size).encode("utf-8"))
    for url in _normalize_openapi_base_urls(base_urls):
        hasher.update(url.encode("utf-8"))
    return hasher.hexdigest()[:16]


def build_openapi_document(
    summary: SwaggerDataSummary,
    *,
    base_urls: tuple[str, ...] | list[str] | None = None,
) -> dict[str, Any]:
    components = {"schemas": _build_schemas()}
    paths = _build_paths(summary)
    resolved_base_urls = _normalize_openapi_base_urls(tuple(base_urls or resolve_openapi_base_urls()))

    total_snapshot_rows = sum(summary.snapshot_counts.values()) if summary.snapshot_counts else 0
    total_tables = len([name for name, count in summary.table_counts.items() if count > 0]) if summary.table_counts else 0

    info_description = (
        "Local OpenAPI contract for the multi-sport read API backed by the current PostgreSQL dataset. "
        "Paths mirror Sofascore-style templates already used by the ingestion layer. "
        "Normalized endpoints expose typed schemas where we have stable tables, while snapshot-first endpoints remain intentionally flexible.\n\n"
        f"Generated at: {summary.generated_at}\n"
        f"Tables with data in summary: {total_tables}\n"
        f"Raw snapshot rows: {total_snapshot_rows}"
    )

    return {
        "openapi": "3.0.3",
        "info": {
            "title": "Sofascore Local Multi-Sport API",
            "version": "1.0.0",
            "description": info_description,
        },
        "servers": [_openapi_server_entry(url) for url in resolved_base_urls],
        "tags": [{"name": name, "description": description} for name, description in _LOCAL_API_TAGS],
        "paths": paths,
        "components": components,
    }


def _build_paths(summary: SwaggerDataSummary) -> dict[str, Any]:
    paths: dict[str, Any] = {}
    paths.update(_build_core_paths(summary))
    paths.update(_build_statistics_and_entities_paths(summary))
    paths.update(_build_leaderboard_paths(summary))
    paths.update(_build_operations_paths(summary))
    for endpoint in local_api_endpoints():
        paths.setdefault(endpoint.path_template, _build_generic_snapshot_path(endpoint.path_template, summary))
    return paths


def _build_generic_snapshot_path(path_template: str, summary: SwaggerDataSummary) -> dict[str, Any]:
    path_param_names = re.findall(r"{([^{}]+)}", path_template)
    operation_suffix = re.sub(r"[^a-zA-Z0-9]+", " ", path_template).title().replace(" ", "")
    snapshot_rows = int(summary.snapshot_counts.get(path_template, 0))
    return {
        "get": {
            "tags": ["Event Detail" if path_template.startswith("/api/v1/event/") else "Special Routes"],
            "operationId": f"get{operation_suffix}",
            "summary": f"Snapshot-backed `{path_template}`",
            "description": (
                "Generic snapshot-backed local API route generated from the registered endpoint contract.\n\n"
                f"- Snapshot rows in summary: `{snapshot_rows}`"
            ),
            "parameters": [
                _path_param(name, "string", f"Path parameter `{name}`.")
                for name in path_param_names
            ],
            "responses": {
                "200": {
                    "description": "Latest stored payload for this route.",
                    "content": {"application/json": {"schema": _ref("FreeFormObject")}},
                }
            },
        }
    }


def _build_operations_paths(summary: SwaggerDataSummary) -> dict[str, Any]:
    snapshot_rows = int(summary.table_counts.get("api_payload_snapshot", 0))
    job_run_rows = int(summary.table_counts.get("etl_job_run", 0))
    live_history_rows = int(summary.table_counts.get("event_live_state_history", 0))
    coverage_rows = int(summary.table_counts.get("coverage_ledger", 0))
    return {
        "/ops/health": {
            "get": {
                "tags": ["Operations"],
                "operationId": "getOperationalHealth",
                "summary": "Operational health status",
                "description": (
                    "Returns the local Hybrid ETL runtime gate summary, including PostgreSQL connectivity, "
                    "Redis availability and current live lane counts.\n\n"
                    f"- Snapshot rows in summary: `{snapshot_rows}`\n"
                    f"- Live history rows in summary: `{live_history_rows}`"
                ),
                "responses": {
                    "200": {
                        "description": "Operational health status",
                        "content": {"application/json": {"schema": _ref("OperationalHealth")}},
                    }
                },
            }
        },
        "/ops/snapshots/summary": {
            "get": {
                "tags": ["Operations"],
                "operationId": "getSnapshotSummary",
                "summary": "Snapshot ingestion summary",
                "description": (
                    "Aggregated raw-ingestion counters sourced from `api_payload_snapshot` and `api_request_log`.\n\n"
                    f"- Snapshot rows in summary: `{snapshot_rows}`"
                ),
                "responses": {
                    "200": {
                        "description": "Snapshot summary",
                        "content": {"application/json": {"schema": _ref("SnapshotSummary")}},
                    }
                },
            }
        },
        "/ops/queues/summary": {
            "get": {
                "tags": ["Operations"],
                "operationId": "getQueueSummary",
                "summary": "Queue and live-lane summary",
                "description": (
                    "Shows pending Redis Streams work, delayed jobs and current live lane occupancy for the Autopilot services."
                ),
                "responses": {
                    "200": {
                        "description": "Queue and live-lane summary",
                        "content": {"application/json": {"schema": _ref("QueueSummary")}},
                    }
                },
            }
        },
        "/ops/jobs/runs": {
            "get": {
                "tags": ["Operations"],
                "operationId": "getRecentJobRuns",
                "summary": "Recent ETL job runs",
                "description": (
                    "Returns recent rows from `etl_job_run` together with aggregate status counts for the control plane.\n\n"
                    f"- Job run rows in summary: `{job_run_rows}`"
                ),
                "parameters": [
                    _query_param("limit", "integer", "Maximum number of recent job runs to return."),
                ],
                "responses": {
                    "200": {
                        "description": "Recent ETL job runs",
                        "content": {"application/json": {"schema": _ref("JobRunsEnvelope")}},
                    }
                },
            }
        },
        "/ops/coverage/summary": {
            "get": {
                "tags": ["Operations"],
                "operationId": "getCoverageSummary",
                "summary": "Coverage ledger summary",
                "description": (
                    "Returns grouped coverage-ledger counts by source, sport, surface and freshness status.\n\n"
                    f"- Coverage ledger rows in summary: `{coverage_rows}`"
                ),
                "responses": {
                    "200": {
                        "description": "Coverage ledger summary",
                        "content": {"application/json": {"schema": _ref("CoverageSummary")}},
                    }
                },
            }
        },
    }


def _make_operation_builder(summary: SwaggerDataSummary):
    all_entries = local_api_endpoints()
    registry = {entry.path_template: entry.registry_entry() for entry in all_entries}

    def op(
        *,
        path_template: str,
        tag: str,
        operation_id: str,
        summary_text: str,
        description: str,
        response_schema: dict[str, Any],
        parameters: list[dict[str, Any]],
        source_tables: list[str],
    ) -> dict[str, Any]:
        registry_entry = registry[path_template]
        rendered_description = description
        target_count = summary.table_counts.get(registry_entry.target_table or "", 0)
        snapshot_count = summary.snapshot_counts.get(registry_entry.pattern, 0)
        coverage_notes: list[str] = []
        if registry_entry.target_table:
            coverage_notes.append(f"Target table: `{registry_entry.target_table}`")
        if target_count:
            coverage_notes.append(f"Target table rows now: `{target_count}`")
        if snapshot_count:
            coverage_notes.append(f"Raw snapshot rows now: `{snapshot_count}`")
        if coverage_notes:
            rendered_description = f"{description}\n\n" + "\n".join(f"- {item}" for item in coverage_notes)

        return {
            "get": {
                "tags": [tag],
                "operationId": operation_id,
                "summary": summary_text,
                "description": rendered_description,
                "parameters": parameters,
                "responses": {
                    "200": {
                        "description": "OK",
                        "content": {"application/json": {"schema": response_schema}},
                    },
                    "404": {"description": "Not found in local dataset"},
                },
                "x-upstream-pattern": registry_entry.pattern,
                "x-source-tables": source_tables,
                "x-target-table": registry_entry.target_table,
            }
        }

    return op


def _sport_operation_prefix(sport_slug: str) -> str:
    return "".join(part.capitalize() for part in sport_slug.split("-"))


def _build_sport_specific_core_paths(op) -> dict[str, Any]:
    paths: dict[str, Any] = {}
    for sport_slug in LOCAL_API_SUPPORTED_SPORTS:
        prefix = _sport_operation_prefix(sport_slug)
        title = sport_slug.capitalize()

        paths[sport_date_categories_endpoint(sport_slug).path_template] = op(
            path_template=sport_date_categories_endpoint(sport_slug).path_template,
            tag="Categories",
            operation_id=f"get{prefix}CategoriesByDate",
            summary_text=f"{title} categories discovery",
            description=f"Daily {sport_slug} category discovery projection.",
            response_schema=_ref("CategoriesEnvelope"),
            parameters=[
                _path_param("date", "string", "YYYY-MM-DD date used in the discovery endpoint."),
                _path_param("timezone_offset_seconds", "integer", "Timezone offset in seconds, for example 10800."),
            ],
            source_tables=["category_daily_summary", "category_daily_unique_tournament", "category_daily_team", "category"],
        )
        paths[sport_categories_endpoint(sport_slug).path_template] = op(
            path_template=sport_categories_endpoint(sport_slug).path_template,
            tag="Categories",
            operation_id=f"get{prefix}CategoriesCatalog",
            summary_text=f"{title} categories catalog",
            description=f"Sport-level categories listing for {sport_slug}.",
            response_schema=_ref("FreeFormObject"),
            parameters=[],
            source_tables=["category", "api_payload_snapshot"],
        )
        paths[sport_categories_all_endpoint(sport_slug).path_template] = op(
            path_template=sport_categories_all_endpoint(sport_slug).path_template,
            tag="Categories",
            operation_id=f"get{prefix}CategoriesCatalogAll",
            summary_text=f"{title} full categories catalog",
            description=f"Full category catalog for {sport_slug} used for wide discovery.",
            response_schema=_ref("FreeFormObject"),
            parameters=[],
            source_tables=["category", "api_payload_snapshot"],
        )
        paths[sport_scheduled_tournaments_endpoint(sport_slug).path_template] = op(
            path_template=sport_scheduled_tournaments_endpoint(sport_slug).path_template,
            tag="Event List",
            operation_id=f"get{prefix}ScheduledTournamentsPage",
            summary_text=f"{title} scheduled tournaments page",
            description=f"Daily scheduled tournament discovery page for {sport_slug}.",
            response_schema=_ref("FreeFormObject"),
            parameters=[
                _path_param("date", "string", "YYYY-MM-DD date used in the discovery endpoint."),
                _path_param("page", "integer", "Scheduled tournaments page number."),
            ],
            source_tables=["tournament", "unique_tournament", "api_payload_snapshot"],
        )
        paths[sport_scheduled_events_endpoint(sport_slug).path_template] = op(
            path_template=sport_scheduled_events_endpoint(sport_slug).path_template,
            tag="Event List",
            operation_id=f"getScheduled{prefix}Events",
            summary_text=f"Scheduled {sport_slug} events",
            description=f"Scheduled {sport_slug} event feed from normalized event tables.",
            response_schema=_envelope("events", _array(_ref("EventSummary"))),
            parameters=[_path_param("date", "string", "YYYY-MM-DD date used in the scheduled events feed.")],
            source_tables=["event", "event_score", "event_round_info", "event_status_time", "event_time"],
        )
        paths[sport_live_events_endpoint(sport_slug).path_template] = op(
            path_template=sport_live_events_endpoint(sport_slug).path_template,
            tag="Event List",
            operation_id=f"getLive{prefix}Events",
            summary_text=f"Live {sport_slug} events",
            description=f"Live {sport_slug} event feed from normalized event tables.",
            response_schema=_envelope("events", _array(_ref("EventSummary"))),
            parameters=[],
            source_tables=["event", "event_score", "event_round_info", "event_status_time", "event_time"],
        )
    paths[SPORT_ALL_EVENT_COUNT_ENDPOINT.path_template] = op(
        path_template=SPORT_ALL_EVENT_COUNT_ENDPOINT.path_template,
        tag="Event List",
        operation_id="getCurrentDaySportEventCount",
        summary_text="Current-day event counts by sport",
        description=(
            "Synthetic local route computed from PostgreSQL. Returns current-day total events and "
            "currently in-progress events per sport slug."
        ),
        response_schema=_ref("SportEventCountEnvelope"),
        parameters=[],
        source_tables=["sport", "category", "unique_tournament", "tournament", "event", "event_status"],
    )
    return paths


def _build_sport_specific_leaderboard_paths(op) -> dict[str, Any]:
    paths: dict[str, Any] = {}
    for sport_slug in LOCAL_API_SUPPORTED_SPORTS:
        profile = resolve_sport_profile(sport_slug)
        if not profile.include_trending_top_players:
            continue
        prefix = _sport_operation_prefix(sport_slug)
        paths[sport_trending_top_players_endpoint(sport_slug).path_template] = op(
            path_template=sport_trending_top_players_endpoint(sport_slug).path_template,
            tag="Leaderboards",
            operation_id=f"get{prefix}TrendingTopPlayers",
            summary_text=f"Trending top players for {sport_slug}",
            description=f"Latest stored raw payload for trending top players in {sport_slug}.",
            response_schema=_envelope("topPlayers", _array(_ref("TopPlayerEntry"))),
            parameters=[],
            source_tables=["api_payload_snapshot"],
        )
    return paths


def _build_core_paths(summary: SwaggerDataSummary) -> dict[str, Any]:
    op = _make_operation_builder(summary)
    return {
        **_build_sport_specific_core_paths(op),
        "/api/v1/unique-tournament/{unique_tournament_id}": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}",
            tag="Competition",
            operation_id="getUniqueTournament",
            summary_text="Unique tournament",
            description="Base unique tournament metadata from normalized competition tables.",
            response_schema=_envelope("uniqueTournament", _ref("UniqueTournament")),
            parameters=[_path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID.")],
            source_tables=["unique_tournament"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/seasons": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/seasons",
            tag="Competition",
            operation_id="getUniqueTournamentSeasons",
            summary_text="Tournament seasons",
            description="Season list linked to one unique tournament.",
            response_schema=_envelope("seasons", _array(_ref("Season"))),
            parameters=[_path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID.")],
            source_tables=["unique_tournament_season", "season"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/info": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/info",
            tag="Competition",
            operation_id="getUniqueTournamentSeasonInfo",
            summary_text="Tournament season info snapshot",
            description="Latest stored raw season info payload for the requested unique tournament season.",
            response_schema=_ref("FreeFormObject"),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["api_payload_snapshot"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/rounds": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/rounds",
            tag="Competition",
            operation_id="getUniqueTournamentSeasonRounds",
            summary_text="Tournament season rounds",
            description="Season round registry for one unique tournament season, including the current round marker.",
            response_schema=_ref("SeasonRoundsEnvelope"),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["season_round", "api_payload_snapshot"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/cuptrees": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/cuptrees",
            tag="Competition",
            operation_id="getUniqueTournamentSeasonCupTrees",
            summary_text="Tournament season cup trees",
            description="Cup tree / playoff structure for one unique tournament season.",
            response_schema=_ref("SeasonCupTreesEnvelope"),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=[
                "season_cup_tree",
                "season_cup_tree_round",
                "season_cup_tree_block",
                "season_cup_tree_participant",
                "api_payload_snapshot",
            ],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/brackets": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/brackets",
            tag="Competition",
            operation_id="getUniqueTournamentSeasonBrackets",
            summary_text="Tournament season brackets snapshot",
            description="Latest stored raw brackets payload for the requested unique tournament season.",
            response_schema=_ref("FreeFormObject"),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["api_payload_snapshot"],
        ),
        "/api/v1/category/{category_id}/unique-tournaments": op(
            path_template="/api/v1/category/{category_id}/unique-tournaments",
            tag="Categories",
            operation_id="getCategoryUniqueTournaments",
            summary_text="Category unique tournaments",
            description="Category-level unique tournament discovery feed.",
            response_schema=_ref("FreeFormObject"),
            parameters=[_path_param("category_id", "integer", "Sofascore category ID.")],
            source_tables=["unique_tournament", "tournament", "api_payload_snapshot"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/scheduled-events/{date}": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/scheduled-events/{date}",
            tag="Event List",
            operation_id="getUniqueTournamentScheduledEvents",
            summary_text="Tournament scheduled events by day",
            description="Tournament/day event feed. Particularly useful for tennis and other daily-draw sports.",
            response_schema=_envelope("events", _array(_ref("EventSummary"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("date", "string", "YYYY-MM-DD date used in the scheduled events feed."),
            ],
            source_tables=["event", "event_score", "event_round_info", "event_status_time", "event_time"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/featured-events": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/featured-events",
            tag="Event List",
            operation_id="getUniqueTournamentFeaturedEvents",
            summary_text="Featured events for one tournament",
            description="Featured event feed for one unique tournament.",
            response_schema=_envelope("featuredEvents", _array(_ref("EventSummary"))),
            parameters=[_path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID.")],
            source_tables=["event", "event_score", "event_round_info", "event_status_time", "event_time"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round_number}": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round_number}",
            tag="Event List",
            operation_id="getRoundEvents",
            summary_text="Round events",
            description="Event list for one tournament season round.",
            response_schema=_envelope("events", _array(_ref("EventSummary"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
                _path_param("round_number", "integer", "Tournament round number."),
            ],
            source_tables=["event", "event_round_info", "event_score"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/last/{page}": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/last/{page}",
            tag="Event List",
            operation_id="getSeasonLastEventsPage",
            summary_text="Season result events page",
            description="Paginated completed/recent events feed for one tournament season.",
            response_schema=_envelope("events", _array(_ref("EventSummary"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
                _path_param("page", "integer", "Zero-based Sofascore page."),
            ],
            source_tables=["event", "event_round_info", "event_score", "api_payload_snapshot"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/next/{page}": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/next/{page}",
            tag="Event List",
            operation_id="getSeasonNextEventsPage",
            summary_text="Season upcoming events page",
            description="Paginated upcoming events feed for one tournament season.",
            response_schema=_envelope("events", _array(_ref("EventSummary"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
                _path_param("page", "integer", "Zero-based Sofascore page."),
            ],
            source_tables=["event", "event_round_info", "event_score", "api_payload_snapshot"],
        ),
        "/api/v1/event/{event_id}": op(
            path_template="/api/v1/event/{event_id}",
            tag="Event Detail",
            operation_id="getEventDetail",
            summary_text="Event detail",
            description="Normalized event detail record for one football event.",
            response_schema=_envelope("event", _ref("EventDetail")),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["event", "event_score", "event_round_info", "event_status_time", "event_time"],
        ),
        "/api/v1/event/{event_id}/statistics": op(
            path_template="/api/v1/event/{event_id}/statistics",
            tag="Event Detail",
            operation_id="getEventStatistics",
            summary_text="Event statistics",
            description="Stored event statistics rows for one event.",
            response_schema=_envelope("statistics", _array(_ref("FreeFormObject"))),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["event_statistic"],
        ),
        "/api/v1/event/{event_id}/lineups": op(
            path_template="/api/v1/event/{event_id}/lineups",
            tag="Event Detail",
            operation_id="getEventLineups",
            summary_text="Event lineups",
            description="Home and away lineups, players and missing players for one event.",
            response_schema={
                "type": "object",
                "properties": {"home": _ref("EventLineupSide"), "away": _ref("EventLineupSide")},
            },
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["event_lineup", "event_lineup_player", "event_lineup_missing_player"],
        ),
        "/api/v1/event/{event_id}/incidents": op(
            path_template="/api/v1/event/{event_id}/incidents",
            tag="Event Detail",
            operation_id="getEventIncidents",
            summary_text="Event incidents",
            description="Stored incident timeline rows for one event.",
            response_schema=_envelope("incidents", _array(_ref("FreeFormObject"))),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["event_incident"],
        ),
        "/api/v1/event/{event_id}/managers": op(
            path_template="/api/v1/event/{event_id}/managers",
            tag="Event Detail",
            operation_id="getEventManagers",
            summary_text="Event managers",
            description="Manager assignments for the home and away side of one event.",
            response_schema={
                "type": "object",
                "properties": {"homeManager": _ref("Manager"), "awayManager": _ref("Manager")},
            },
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["event_manager_assignment", "manager"],
        ),
        "/api/v1/event/{event_id}/h2h": op(
            path_template="/api/v1/event/{event_id}/h2h",
            tag="Event Detail",
            operation_id="getEventH2h",
            summary_text="Event head-to-head",
            description="Team and manager duel aggregates for one event.",
            response_schema={
                "type": "object",
                "properties": {"teamDuel": _ref("EventDuel"), "managerDuel": _ref("EventDuel")},
            },
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["event_duel"],
        ),
        "/api/v1/event/{event_id}/pregame-form": op(
            path_template="/api/v1/event/{event_id}/pregame-form",
            tag="Event Detail",
            operation_id="getEventPregameForm",
            summary_text="Event pregame form",
            description="Pregame form block for the home and away side.",
            response_schema=_ref("EventPregameFormEnvelope"),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["event_pregame_form", "event_pregame_form_side", "event_pregame_form_item"],
        ),
        "/api/v1/event/{event_id}/votes": op(
            path_template="/api/v1/event/{event_id}/votes",
            tag="Event Detail",
            operation_id="getEventVotes",
            summary_text="Event votes",
            description="Fan vote distributions for one event.",
            response_schema=_envelope("vote", _array(_ref("EventVoteOption"))),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["event_vote_option"],
        ),
        "/api/v1/event/{event_id}/comments": op(
            path_template="/api/v1/event/{event_id}/comments",
            tag="Event Detail",
            operation_id="getEventComments",
            summary_text="Event comments feed",
            description="Minute-by-minute match commentary and kit colors for one event.",
            response_schema=_ref("EventCommentsEnvelope"),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["event_comment_feed", "event_comment", "player"],
        ),
        "/api/v1/event/{event_id}/graph": op(
            path_template="/api/v1/event/{event_id}/graph",
            tag="Event Detail",
            operation_id="getEventGraph",
            summary_text="Event momentum graph",
            description="Stored momentum graph points for one event.",
            response_schema=_ref("EventGraphEnvelope"),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["event_graph", "event_graph_point"],
        ),
        "/api/v1/event/{event_id}/heatmap/{team_id}": op(
            path_template="/api/v1/event/{event_id}/heatmap/{team_id}",
            tag="Event Detail",
            operation_id="getEventTeamHeatmap",
            summary_text="Team heatmap for one event",
            description="Stored event heatmap points for one team inside one event.",
            response_schema=_ref("EventTeamHeatmapEnvelope"),
            parameters=[
                _path_param("event_id", "integer", "Sofascore event ID."),
                _path_param("team_id", "integer", "Sofascore team ID."),
            ],
            source_tables=["event_team_heatmap", "event_team_heatmap_point"],
        ),
        "/api/v1/event/{event_id}/odds/{provider_id}/all": op(
            path_template="/api/v1/event/{event_id}/odds/{provider_id}/all",
            tag="Event Detail",
            operation_id="getEventOddsAll",
            summary_text="All event odds markets",
            description="All stored odds markets for one event/provider combination.",
            response_schema=_envelope("markets", _array(_ref("EventMarket"))),
            parameters=[
                _path_param("event_id", "integer", "Sofascore event ID."),
                _path_param("provider_id", "integer", "Odds provider ID."),
            ],
            source_tables=["event_market", "event_market_choice"],
        ),
        "/api/v1/event/{event_id}/odds/{provider_id}/featured": op(
            path_template="/api/v1/event/{event_id}/odds/{provider_id}/featured",
            tag="Event Detail",
            operation_id="getEventOddsFeatured",
            summary_text="Featured event odds markets",
            description="Featured odds markets for one event/provider combination.",
            response_schema=_envelope("featured", _array(_ref("EventMarket"))),
            parameters=[
                _path_param("event_id", "integer", "Sofascore event ID."),
                _path_param("provider_id", "integer", "Odds provider ID."),
            ],
            source_tables=["event_market", "event_market_choice"],
        ),
        "/api/v1/event/{event_id}/provider/{provider_id}/winning-odds": op(
            path_template="/api/v1/event/{event_id}/provider/{provider_id}/winning-odds",
            tag="Event Detail",
            operation_id="getEventWinningOdds",
            summary_text="Winning odds projection",
            description="Stored winning-odds projection for home and away sides.",
            response_schema={
                "type": "object",
                "properties": {"home": _ref("EventWinningOddsSide"), "away": _ref("EventWinningOddsSide")},
            },
            parameters=[
                _path_param("event_id", "integer", "Sofascore event ID."),
                _path_param("provider_id", "integer", "Odds provider ID."),
            ],
            source_tables=["event_winning_odds"],
        ),
        "/api/v1/event/{event_id}/best-players/summary": op(
            path_template="/api/v1/event/{event_id}/best-players/summary",
            tag="Event Detail",
            operation_id="getEventBestPlayersSummary",
            summary_text="Event best players summary",
            description="Stored best-player summary, including player of the match, for one event.",
            response_schema=_ref("FreeFormObject"),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["event_best_player_entry", "player"],
        ),
        "/api/v1/event/{event_id}/player/{player_id}/statistics": op(
            path_template="/api/v1/event/{event_id}/player/{player_id}/statistics",
            tag="Event Detail",
            operation_id="getEventPlayerStatistics",
            summary_text="Event player statistics",
            description="Per-player event statistics projection for one event/player pair.",
            response_schema=_ref("FreeFormObject"),
            parameters=[
                _path_param("event_id", "integer", "Sofascore event ID."),
                _path_param("player_id", "integer", "Sofascore player ID."),
            ],
            source_tables=["event_player_statistics", "event_player_stat_value", "player", "team"],
        ),
        "/api/v1/event/{event_id}/player/{player_id}/rating-breakdown": op(
            path_template="/api/v1/event/{event_id}/player/{player_id}/rating-breakdown",
            tag="Event Detail",
            operation_id="getEventPlayerRatingBreakdown",
            summary_text="Event player rating breakdown",
            description="Per-player rating-breakdown action feed for one event/player pair.",
            response_schema=_ref("FreeFormObject"),
            parameters=[
                _path_param("event_id", "integer", "Sofascore event ID."),
                _path_param("player_id", "integer", "Sofascore player ID."),
            ],
            source_tables=["event_player_rating_breakdown_action"],
        ),
        "/api/v1/event/{event_id}/graph/sequence": op(
            path_template="/api/v1/event/{event_id}/graph/sequence",
            tag="Special Routes",
            operation_id="getEventGraphSequence",
            summary_text="Graph sequence snapshot",
            description="Latest stored raw graph-sequence payload for one event.",
            response_schema=_ref("FreeFormObject"),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["api_payload_snapshot"],
        ),
        "/api/v1/event/{event_id}/point-by-point": op(
            path_template="/api/v1/event/{event_id}/point-by-point",
            tag="Special Routes",
            operation_id="getEventPointByPoint",
            summary_text="Tennis point-by-point payload",
            description="Latest stored raw point-by-point payload for one tennis event.",
            response_schema=_ref("FreeFormObject"),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["api_payload_snapshot", "tennis_point_by_point"],
        ),
        "/api/v1/event/{event_id}/tennis-power": op(
            path_template="/api/v1/event/{event_id}/tennis-power",
            tag="Special Routes",
            operation_id="getEventTennisPower",
            summary_text="Tennis power snapshot",
            description="Latest stored raw tennis-power payload for one tennis event.",
            response_schema=_ref("FreeFormObject"),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["api_payload_snapshot", "tennis_power"],
        ),
        EVENT_BASEBALL_INNINGS_ENDPOINT.path_template: op(
            path_template=EVENT_BASEBALL_INNINGS_ENDPOINT.path_template,
            tag="Special Routes",
            operation_id="getEventBaseballInnings",
            summary_text="Baseball innings snapshot",
            description="Latest stored raw innings payload for one baseball event.",
            response_schema=_ref("FreeFormObject"),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["api_payload_snapshot", "baseball_inning"],
        ),
        EVENT_BASEBALL_PITCHES_ENDPOINT.path_template: op(
            path_template=EVENT_BASEBALL_PITCHES_ENDPOINT.path_template,
            tag="Special Routes",
            operation_id="getEventBaseballPitches",
            summary_text="Baseball pitches snapshot",
            description="Latest stored raw at-bat pitches payload for one baseball event and at-bat.",
            response_schema=_ref("FreeFormObject"),
            parameters=[
                _path_param("event_id", "integer", "Sofascore event ID."),
                _path_param("at_bat_id", "integer", "Sofascore at-bat ID."),
            ],
            source_tables=["api_payload_snapshot", "baseball_pitch"],
        ),
        EVENT_SHOTMAP_ENDPOINT.path_template: op(
            path_template=EVENT_SHOTMAP_ENDPOINT.path_template,
            tag="Special Routes",
            operation_id="getEventShotmap",
            summary_text="Shotmap snapshot",
            description="Latest stored raw shotmap payload for one event.",
            response_schema=_ref("FreeFormObject"),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["api_payload_snapshot", "shotmap_point"],
        ),
        "/api/v1/event/{event_id}/weather": op(
            path_template="/api/v1/event/{event_id}/weather",
            tag="Special Routes",
            operation_id="getEventWeather",
            summary_text="Event weather snapshot",
            description="Latest stored raw weather payload for one event.",
            response_schema=_ref("FreeFormObject"),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["api_payload_snapshot"],
        ),
        EVENT_ESPORTS_GAMES_ENDPOINT.path_template: op(
            path_template=EVENT_ESPORTS_GAMES_ENDPOINT.path_template,
            tag="Special Routes",
            operation_id="getEventEsportsGames",
            summary_text="Esports games snapshot",
            description="Latest stored raw esports-games payload for one event.",
            response_schema=_ref("FreeFormObject"),
            parameters=[_path_param("event_id", "integer", "Sofascore event ID.")],
            source_tables=["api_payload_snapshot", "esports_game"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/{scope}": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/{scope}",
            tag="Standings",
            operation_id="getUniqueTournamentStandings",
            summary_text="Standings by unique tournament",
            description="Standings projection by unique tournament season and scope.",
            response_schema=_envelope("standings", _array(_ref("Standing"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
                _path_param("scope", "string", "Standings scope.", enum=["total", "home", "away"]),
            ],
            source_tables=["standing", "standing_row"],
        ),
        "/api/v1/tournament/{tournament_id}/season/{season_id}/standings/{scope}": op(
            path_template="/api/v1/tournament/{tournament_id}/season/{season_id}/standings/{scope}",
            tag="Standings",
            operation_id="getTournamentStandings",
            summary_text="Standings by tournament",
            description="Standings projection by concrete tournament season and scope.",
            response_schema=_envelope("standings", _array(_ref("Standing"))),
            parameters=[
                _path_param("tournament_id", "integer", "Sofascore tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
                _path_param("scope", "string", "Standings scope.", enum=["total", "home", "away"]),
            ],
            source_tables=["standing", "standing_row"],
        ),
    }


def _build_statistics_and_entities_paths(summary: SwaggerDataSummary) -> dict[str, Any]:
    op = _make_operation_builder(summary)
    return {
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/info": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/info",
            tag="Statistics",
            operation_id="getSeasonStatisticsInfo",
            summary_text="Season statistics info",
            description="Statistics configuration for a tournament season, including teams, group definitions and nationalities.",
            response_schema=_ref("SeasonStatisticsInfoEnvelope"),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["season_statistics_config", "season_statistics_config_team", "season_statistics_group_item", "season_statistics_nationality"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics",
            tag="Statistics",
            operation_id="getSeasonStatistics",
            summary_text="Season statistics snapshot results",
            description="Leaderboard-style season statistics snapshots. Query parameters intentionally mirror Sofascore grammar.",
            response_schema=_ref("SeasonStatisticsEnvelope"),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
                _query_param("limit", "integer", "Result page size."),
                _query_param("offset", "integer", "Offset used by the original Sofascore query."),
                _query_param("order", "string", "Ordering code, for example -rating."),
                _query_param("accumulation", "string", "Accumulation mode, for example total or per90."),
                _query_param("group", "string", "Statistics group code."),
                _query_param("fields", "array", "Optional list of exact Sofascore field names.", item_type="string", style="form", explode=False),
                _query_param("filters", "array", "Optional list of exact Sofascore filter expressions.", item_type="string", style="form", explode=False),
            ],
            source_tables=["season_statistics_snapshot", "season_statistics_result", "player", "team"],
        ),
        "/api/v1/team/{team_id}": op(
            path_template="/api/v1/team/{team_id}",
            tag="Entities",
            operation_id="getTeam",
            summary_text="Team entity",
            description="Normalized team entity.",
            response_schema=_envelope("team", _ref("Team")),
            parameters=[_path_param("team_id", "integer", "Sofascore team ID.")],
            source_tables=["team"],
        ),
        "/api/v1/player/{player_id}": op(
            path_template="/api/v1/player/{player_id}",
            tag="Entities",
            operation_id="getPlayer",
            summary_text="Player entity",
            description="Normalized player entity.",
            response_schema=_envelope("player", _ref("Player")),
            parameters=[_path_param("player_id", "integer", "Sofascore player ID.")],
            source_tables=["player"],
        ),
        "/api/v1/manager/{manager_id}": op(
            path_template="/api/v1/manager/{manager_id}",
            tag="Entities",
            operation_id="getManager",
            summary_text="Manager entity",
            description="Normalized manager entity.",
            response_schema=_envelope("manager", _ref("Manager")),
            parameters=[_path_param("manager_id", "integer", "Sofascore manager ID.")],
            source_tables=["manager"],
        ),
        "/api/v1/player/{player_id}/statistics": op(
            path_template="/api/v1/player/{player_id}/statistics",
            tag="Entities",
            operation_id="getPlayerStatistics",
            summary_text="Player season statistics",
            description="Player season statistics projection backed by player_season_statistics and related entity tables.",
            response_schema=_ref("PlayerStatisticsEnvelope"),
            parameters=[_path_param("player_id", "integer", "Sofascore player ID.")],
            source_tables=["player_season_statistics", "team", "unique_tournament", "season"],
        ),
        "/api/v1/player/{player_id}/transfer-history": op(
            path_template="/api/v1/player/{player_id}/transfer-history",
            tag="Entities",
            operation_id="getPlayerTransferHistory",
            summary_text="Player transfer history",
            description="Player transfer history projection.",
            response_schema=_envelope("transferHistory", _array(_ref("PlayerTransferHistoryItem"))),
            parameters=[_path_param("player_id", "integer", "Sofascore player ID.")],
            source_tables=["player_transfer_history"],
        ),
        "/api/v1/player/{player_id}/statistics/seasons": op(
            path_template="/api/v1/player/{player_id}/statistics/seasons",
            tag="Entities",
            operation_id="getPlayerStatisticsSeasons",
            summary_text="Player statistics seasons",
            description="Tournament/season availability map for player statistics, plus types map.",
            response_schema=_ref("EntityStatisticsSeasonsEnvelope"),
            parameters=[_path_param("player_id", "integer", "Sofascore player ID.")],
            source_tables=["entity_statistics_season", "entity_statistics_type", "unique_tournament", "season"],
        ),
        "/api/v1/team/{team_id}/team-statistics/seasons": op(
            path_template="/api/v1/team/{team_id}/team-statistics/seasons",
            tag="Entities",
            operation_id="getTeamStatisticsSeasons",
            summary_text="Team statistics seasons",
            description="Tournament/season availability map for team statistics, plus types map.",
            response_schema=_ref("EntityStatisticsSeasonsEnvelope"),
            parameters=[_path_param("team_id", "integer", "Sofascore team ID.")],
            source_tables=["entity_statistics_season", "entity_statistics_type", "unique_tournament", "season"],
        ),
        "/api/v1/team/{team_id}/player-statistics/seasons": op(
            path_template="/api/v1/team/{team_id}/player-statistics/seasons",
            tag="Entities",
            operation_id="getTeamPlayerStatisticsSeasons",
            summary_text="Team player statistics seasons",
            description="Tournament/season availability map for player statistics under one team context.",
            response_schema=_ref("EntityStatisticsSeasonsEnvelope"),
            parameters=[_path_param("team_id", "integer", "Sofascore team ID.")],
            source_tables=["season_statistics_type", "unique_tournament", "season"],
        ),
        "/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall": op(
            path_template="/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall",
            tag="Entities",
            operation_id="getPlayerSeasonOverallStatistics",
            summary_text="Player season overall statistics snapshot",
            description="Latest stored raw payload for one player/tournament/season overall statistics request.",
            response_schema=_ref("PlayerSeasonOverallEnvelope"),
            parameters=[
                _path_param("player_id", "integer", "Sofascore player ID."),
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["api_payload_snapshot"],
        ),
        "/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall": op(
            path_template="/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/overall",
            tag="Entities",
            operation_id="getTeamSeasonOverallStatistics",
            summary_text="Team season overall statistics snapshot",
            description="Latest stored raw payload for one team/tournament/season overall statistics request.",
            response_schema=_ref("TeamSeasonOverallEnvelope"),
            parameters=[
                _path_param("team_id", "integer", "Sofascore team ID."),
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["api_payload_snapshot"],
        ),
        "/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/heatmap/overall": op(
            path_template="/api/v1/player/{player_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/heatmap/overall",
            tag="Entities",
            operation_id="getPlayerSeasonHeatmapOverall",
            summary_text="Player season heatmap snapshot",
            description="Latest stored raw payload for one player/tournament/season overall heatmap request.",
            response_schema=_ref("PlayerHeatmapOverallEnvelope"),
            parameters=[
                _path_param("player_id", "integer", "Sofascore player ID."),
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["api_payload_snapshot"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team/{team_id}/team-performance-graph-data": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team/{team_id}/team-performance-graph-data",
            tag="Entities",
            operation_id="getTeamPerformanceGraphData",
            summary_text="Team performance graph snapshot",
            description="Latest stored raw payload for team performance graph data.",
            response_schema=_ref("TeamPerformanceGraphEnvelope"),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
                _path_param("team_id", "integer", "Sofascore team ID."),
            ],
            source_tables=["api_payload_snapshot"],
        ),
    }


def _build_leaderboard_paths(summary: SwaggerDataSummary) -> dict[str, Any]:
    op = _make_operation_builder(summary)
    return {
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall",
            tag="Leaderboards",
            operation_id="getTopPlayersOverall",
            summary_text="Top players overall",
            description="Top players leaderboard for one tournament season.",
            response_schema=_envelope("topPlayers", _array(_ref("TopPlayerEntry"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["top_player_snapshot", "top_player_entry"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-ratings/overall": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-ratings/overall",
            tag="Leaderboards",
            operation_id="getTopRatingsOverall",
            summary_text="Top ratings overall",
            description="Top ratings leaderboard for one tournament season.",
            response_schema=_envelope("topPlayers", _array(_ref("TopPlayerEntry"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["top_player_snapshot", "top_player_entry"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/regularSeason": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/regularSeason",
            tag="Leaderboards",
            operation_id="getTopPlayersRegularSeason",
            summary_text="Top players regular season",
            description="Regular-season top players leaderboard for one tournament season.",
            response_schema=_envelope("topPlayers", _array(_ref("TopPlayerEntry"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["top_player_snapshot", "top_player_entry"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players-per-game/all/overall": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players-per-game/all/overall",
            tag="Leaderboards",
            operation_id="getTopPlayersPerGameOverall",
            summary_text="Top players per game overall",
            description="Per-game top players leaderboard for one tournament season.",
            response_schema=_envelope("topPlayers", _array(_ref("TopPlayerEntry"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["top_player_snapshot", "top_player_entry"],
        ),
        "/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall": op(
            path_template="/api/v1/team/{team_id}/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall",
            tag="Leaderboards",
            operation_id="getTeamScopedTopPlayersOverall",
            summary_text="Top players overall for one team context",
            description="Top players leaderboard scoped to one team within a tournament season.",
            response_schema=_envelope("topPlayers", _array(_ref("TopPlayerEntry"))),
            parameters=[
                _path_param("team_id", "integer", "Sofascore team ID."),
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["top_player_snapshot", "top_player_entry"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players-per-game/all/regularSeason": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players-per-game/all/regularSeason",
            tag="Leaderboards",
            operation_id="getTopPlayersPerGameRegularSeason",
            summary_text="Top players per game regular season",
            description="Regular-season per-game top players leaderboard for one tournament season.",
            response_schema=_envelope("topPlayers", _array(_ref("TopPlayerEntry"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["top_player_snapshot", "top_player_entry"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season-race": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season-race",
            tag="Leaderboards",
            operation_id="getPlayerOfTheSeasonRace",
            summary_text="Player of the season race",
            description="Player of the season race snapshot plus optional statisticsType metadata.",
            response_schema=_ref("PlayerOfTheSeasonRaceEnvelope"),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["top_player_snapshot", "top_player_entry"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/overall": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/overall",
            tag="Leaderboards",
            operation_id="getTopTeamsOverall",
            summary_text="Top teams overall",
            description="Top teams leaderboard for one tournament season.",
            response_schema=_envelope("topTeams", _array(_ref("TopTeamEntry"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["top_team_snapshot", "top_team_entry"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/venues": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/venues",
            tag="Leaderboards",
            operation_id="getSeasonVenues",
            summary_text="Season venues",
            description="Venue list for one tournament season.",
            response_schema=_envelope("venues", _array(_ref("Venue"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["venue"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/regularSeason": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/regularSeason",
            tag="Leaderboards",
            operation_id="getTopTeamsRegularSeason",
            summary_text="Top teams regular season",
            description="Regular-season top teams leaderboard for one tournament season.",
            response_schema=_envelope("topTeams", _array(_ref("TopTeamEntry"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["top_team_snapshot", "top_team_entry"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/groups": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/groups",
            tag="Leaderboards",
            operation_id="getSeasonGroups",
            summary_text="Season groups",
            description="Group list for one tournament season.",
            response_schema=_envelope("groups", _array(_ref("SeasonGroup"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["season_group"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season",
            tag="Leaderboards",
            operation_id="getPlayerOfTheSeason",
            summary_text="Player of the season",
            description="Player of the season projection for one tournament season.",
            response_schema=_ref("PlayerOfTheSeasonEnvelope"),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["season_player_of_the_season", "player", "team"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-of-the-week/periods": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-of-the-week/periods",
            tag="Leaderboards",
            operation_id="getTeamOfTheWeekPeriods",
            summary_text="Team of the week periods",
            description="Available team-of-the-week periods for one tournament season.",
            response_schema=_envelope("periods", _array(_ref("Period"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["period"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-of-the-week/{period_id}": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-of-the-week/{period_id}",
            tag="Leaderboards",
            operation_id="getTeamOfTheWeek",
            summary_text="Team of the week",
            description="Team of the week formation and player list for one period.",
            response_schema=_ref("TeamOfTheWeekEnvelope"),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
                _path_param("period_id", "integer", "Team-of-the-week period ID."),
            ],
            source_tables=["team_of_the_week", "team_of_the_week_player"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-statistics/types": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-statistics/types",
            tag="Leaderboards",
            operation_id="getPlayerStatisticsTypes",
            summary_text="Player statistics types",
            description="Available player statistics type codes for one tournament season.",
            response_schema=_envelope("types", _array({"type": "string"})),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["season_statistics_type"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-statistics/types": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-statistics/types",
            tag="Leaderboards",
            operation_id="getTeamStatisticsTypes",
            summary_text="Team statistics types",
            description="Available team statistics type codes for one tournament season.",
            response_schema=_envelope("types", _array({"type": "string"})),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
            ],
            source_tables=["season_statistics_type"],
        ),
        "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/{scope}": op(
            path_template="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/{scope}",
            tag="Leaderboards",
            operation_id="getTournamentTeamEvents",
            summary_text="Tournament team events",
            description="Stored team-events buckets for one tournament season and scope.",
            response_schema=_envelope("tournamentTeamEvents", _array(_ref("TournamentTeamEventBucket"))),
            parameters=[
                _path_param("unique_tournament_id", "integer", "Sofascore unique tournament ID."),
                _path_param("season_id", "integer", "Sofascore season ID."),
                _path_param("scope", "string", "Team-events scope.", enum=["total", "home", "away"]),
            ],
            source_tables=["tournament_team_event_snapshot", "tournament_team_event_bucket"],
        ),
        **_build_sport_specific_leaderboard_paths(op),
    }


def _build_schemas() -> dict[str, Any]:
    schemas: dict[str, Any] = {
        "FreeFormObject": {"type": "object", "additionalProperties": True},
        "SportEventCount": _obj({"live": _int32(), "total": _int32()}),
        "SportEventCountEnvelope": {
            "type": "object",
            "additionalProperties": _ref("SportEventCount"),
        },
        "TeamColors": _obj(
            {
                "primary": {"type": "string"},
                "secondary": {"type": "string"},
                "text": {"type": "string"},
            }
        ),
        "TypeMap": {
            "type": "object",
            "additionalProperties": {
                "oneOf": [
                    {"type": "array", "items": {"type": "string"}},
                    {
                        "type": "object",
                        "additionalProperties": {"type": "array", "items": {"type": "string"}},
                    },
                ]
            },
        },
        "Sport": _obj(
            {
                "id": _int64(),
                "slug": {"type": "string"},
                "name": {"type": "string"},
            }
        ),
        "Category": _obj(
            {
                "id": _int64(),
                "slug": {"type": "string"},
                "name": {"type": "string"},
                "flag": {"type": "string"},
                "alpha2": {"type": "string"},
                "priority": _int32(),
                "sportId": _int64(),
                "countryAlpha2": {"type": "string"},
                "fieldTranslations": _ref("FreeFormObject"),
            }
        ),
        "Season": _obj(
            {
                "id": _int64(),
                "name": {"type": "string"},
                "year": {"type": "string"},
                "editor": {"type": "boolean"},
                "seasonCoverageInfo": _ref("FreeFormObject"),
            }
        ),
        "UniqueTournament": _obj(
            {
                "id": _int64(),
                "slug": {"type": "string"},
                "name": {"type": "string"},
                "categoryId": _int64(),
                "countryAlpha2": {"type": "string"},
                "logoAssetId": _int64(),
                "darkLogoAssetId": _int64(),
                "titleHolderTeamId": _int64(),
                "titleHolderTitles": _int32(),
                "mostTitles": _int32(),
                "gender": {"type": "string"},
                "primaryColorHex": {"type": "string"},
                "secondaryColorHex": {"type": "string"},
                "startDateTimestamp": _int64(),
                "endDateTimestamp": _int64(),
                "tier": _int32(),
                "tennisPoints": _int32(),
                "userCount": _int32(),
                "hasRounds": {"type": "boolean"},
                "hasGroups": {"type": "boolean"},
                "hasPerformanceGraphFeature": {"type": "boolean"},
                "hasPlayoffSeries": {"type": "boolean"},
                "hasEventPlayerStatistics": {"type": "boolean"},
                "hasLiveRating": {"type": "boolean"},
                "hasRating": {"type": "boolean"},
                "hasDownDistance": {"type": "boolean"},
                "disabledHomeAwayStandings": {"type": "boolean"},
                "displayInverseHomeAwayTeams": {"type": "boolean"},
                "fieldTranslations": _ref("FreeFormObject"),
                "periodLength": _ref("FreeFormObject"),
            }
        ),
        "Team": _obj(
            {
                "id": _int64(),
                "slug": {"type": "string"},
                "name": {"type": "string"},
                "shortName": {"type": "string"},
                "fullName": {"type": "string"},
                "nameCode": {"type": "string"},
                "sportId": _int64(),
                "categoryId": _int64(),
                "countryAlpha2": {"type": "string"},
                "managerId": _int64(),
                "venueId": _int64(),
                "tournamentId": _int64(),
                "primaryUniqueTournamentId": _int64(),
                "parentTeamId": _int64(),
                "gender": {"type": "string"},
                "type": _int32(),
                "class": _int32(),
                "ranking": _int32(),
                "national": {"type": "boolean"},
                "disabled": {"type": "boolean"},
                "foundationDateTimestamp": _int64(),
                "userCount": _int32(),
                "teamColors": _ref("TeamColors"),
                "fieldTranslations": _ref("FreeFormObject"),
                "timeActive": _ref("FreeFormObject"),
            }
        ),
        "Player": _obj(
            {
                "id": _int64(),
                "slug": {"type": "string"},
                "name": {"type": "string"},
                "shortName": {"type": "string"},
                "firstName": {"type": "string"},
                "lastName": {"type": "string"},
                "teamId": _int64(),
                "countryAlpha2": {"type": "string"},
                "managerId": _int64(),
                "gender": {"type": "string"},
                "position": {"type": "string"},
                "positionsDetailed": {"type": "array", "items": {"type": "string"}},
                "preferredFoot": {"type": "string"},
                "jerseyNumber": {"type": "string"},
                "sofascoreId": {"type": "string"},
                "dateOfBirth": {"type": "string"},
                "dateOfBirthTimestamp": _int64(),
                "height": _int32(),
                "weight": _int32(),
                "marketValueCurrency": {"type": "string"},
                "proposedMarketValueRaw": _ref("FreeFormObject"),
                "rating": {"type": "string"},
                "retired": {"type": "boolean"},
                "deceased": {"type": "boolean"},
                "userCount": _int32(),
                "orderValue": _int32(),
                "fieldTranslations": _ref("FreeFormObject"),
            }
        ),
        "Venue": _obj(
            {
                "id": _int64(),
                "slug": {"type": "string"},
                "name": {"type": "string"},
                "capacity": _int32(),
                "hidden": {"type": "boolean"},
                "countryAlpha2": {"type": "string"},
                "cityName": {"type": "string"},
                "stadiumName": {"type": "string"},
                "stadiumCapacity": _int32(),
                "latitude": {"type": "number"},
                "longitude": {"type": "number"},
                "fieldTranslations": _ref("FreeFormObject"),
            }
        ),
        "Manager": _obj(
            {
                "id": _int64(),
                "slug": {"type": "string"},
                "name": {"type": "string"},
                "shortName": {"type": "string"},
                "sportId": _int64(),
                "countryAlpha2": {"type": "string"},
                "teamId": _int64(),
                "formerPlayerId": _int64(),
                "nationality": {"type": "string"},
                "nationalityIso2": {"type": "string"},
                "dateOfBirthTimestamp": _int64(),
                "deceased": {"type": "boolean"},
                "preferredFormation": {"type": "string"},
                "fieldTranslations": _ref("FreeFormObject"),
            }
        ),
        "CategorySeedItem": _obj(
            {
                "observedDate": {"type": "string", "format": "date"},
                "timezoneOffsetSeconds": _int32(),
                "category": _ref("Category"),
                "totalEvents": _int32(),
                "totalEventPlayerStatistics": _int32(),
                "totalVideos": _int32(),
                "uniqueTournamentIds": {"type": "array", "items": _int64()},
                "teamIds": {"type": "array", "items": _int64()},
            }
        ),
        "CategoriesEnvelope": _envelope_schema("categories", _array(_ref("CategorySeedItem"))),
        "EventScoreSide": _obj(
            {
                "current": _int32(),
                "display": _int32(),
                "aggregated": _int32(),
                "normaltime": _int32(),
                "overtime": _int32(),
                "penalties": _int32(),
                "period1": _int32(),
                "period2": _int32(),
                "period3": _int32(),
                "period4": _int32(),
                "extra1": _int32(),
                "extra2": _int32(),
                "series": _int32(),
            }
        ),
        "EventScores": _obj({"home": _ref("EventScoreSide"), "away": _ref("EventScoreSide")}),
        "EventRoundInfo": _obj(
            {
                "roundNumber": _int32(),
                "slug": {"type": "string"},
                "name": {"type": "string"},
                "cupRoundType": _int32(),
            }
        ),
        "EventStatusTime": _obj(
            {
                "prefix": {"type": "string"},
                "timestamp": _int64(),
                "initial": _int32(),
                "max": _int32(),
                "extra": _int32(),
            }
        ),
        "EventTime": _obj(
            {
                "currentPeriodStartTimestamp": _int64(),
                "initial": _int32(),
                "max": _int32(),
                "extra": _int32(),
                "injuryTime1": _int32(),
                "injuryTime2": _int32(),
                "injuryTime3": _int32(),
                "injuryTime4": _int32(),
                "overtimeLength": _int32(),
                "periodLength": _int32(),
                "totalPeriodCount": _int32(),
            }
        ),
        "EventSummary": _obj(
            {
                "id": _int64(),
                "slug": {"type": "string"},
                "customId": {"type": "string"},
                "detailId": _int64(),
                "tournamentId": _int64(),
                "uniqueTournamentId": _int64(),
                "seasonId": _int64(),
                "homeTeamId": _int64(),
                "awayTeamId": _int64(),
                "venueId": _int64(),
                "refereeId": _int64(),
                "statusCode": _int32(),
                "seasonStatisticsType": {"type": "string"},
                "startTimestamp": _int64(),
                "coverage": _int32(),
                "winnerCode": _int32(),
                "aggregatedWinnerCode": _int32(),
                "homeRedCards": _int32(),
                "awayRedCards": _int32(),
                "previousLegEventId": _int64(),
                "cupMatchesInRound": _int32(),
                "defaultPeriodCount": _int32(),
                "defaultPeriodLength": _int32(),
                "defaultOvertimeLength": _int32(),
                "lastPeriod": {"type": "string"},
                "correctAiInsight": {"type": "boolean"},
                "correctHalftimeAiInsight": {"type": "boolean"},
                "feedLocked": {"type": "boolean"},
                "isEditor": {"type": "boolean"},
                "showTotoPromo": {"type": "boolean"},
                "crowdsourcingEnabled": {"type": "boolean"},
                "crowdsourcingDataDisplayEnabled": {"type": "boolean"},
                "finalResultOnly": {"type": "boolean"},
                "hasEventPlayerStatistics": {"type": "boolean"},
                "hasEventPlayerHeatMap": {"type": "boolean"},
                "hasGlobalHighlights": {"type": "boolean"},
                "hasXg": {"type": "boolean"},
                "roundInfo": _ref("EventRoundInfo"),
                "statusTime": _ref("EventStatusTime"),
                "time": _ref("EventTime"),
                "scores": _ref("EventScores"),
            }
        ),
        "EventDetail": _ref("EventSummary"),
        "EventLineupPlayer": _obj(
            {
                "playerId": _int64(),
                "teamId": _int64(),
                "position": {"type": "string"},
                "substitute": {"type": "boolean"},
                "shirtNumber": _int32(),
                "jerseyNumber": {"type": "string"},
                "avgRating": {"type": "number"},
            }
        ),
        "EventLineupMissingPlayer": _obj(
            {
                "playerId": _int64(),
                "description": {"type": "string"},
                "expectedEndDate": {"type": "string"},
                "externalType": _int32(),
                "reason": _int32(),
                "type": {"type": "string"},
            }
        ),
        "EventLineupSide": _obj(
            {
                "formation": {"type": "string"},
                "playerColor": _ref("FreeFormObject"),
                "goalkeeperColor": _ref("FreeFormObject"),
                "supportStaff": _ref("FreeFormObject"),
                "players": _array(_ref("EventLineupPlayer")),
                "missingPlayers": _array(_ref("EventLineupMissingPlayer")),
            }
        ),
        "EventDuel": _obj(
            {
                "duelType": {"type": "string"},
                "homeWins": _int32(),
                "awayWins": _int32(),
                "draws": _int32(),
            }
        ),
        "EventPregameFormItem": _obj({"ordinal": _int32(), "formValue": {"type": "string"}}),
        "EventPregameFormSide": _obj(
            {
                "avgRating": {"type": "string"},
                "position": _int32(),
                "value": {"type": "string"},
                "items": _array(_ref("EventPregameFormItem")),
            }
        ),
        "EventPregameFormEnvelope": _obj(
            {
                "label": {"type": "string"},
                "homeTeam": _ref("EventPregameFormSide"),
                "awayTeam": _ref("EventPregameFormSide"),
            }
        ),
        "EventVoteOption": _obj(
            {
                "voteType": {"type": "string"},
                "optionName": {"type": "string"},
                "voteCount": _int32(),
            }
        ),
        "EventCommentColor": _obj(
            {
                "primary": {"type": "string"},
                "number": {"type": "string"},
                "outline": {"type": "string"},
                "fancyNumber": {"type": "string"},
            }
        ),
        "EventCommentSideStyle": _obj(
            {
                "playerColor": _ref("EventCommentColor"),
                "goalkeeperColor": _ref("EventCommentColor"),
            }
        ),
        "EventComment": _obj(
            {
                "id": _int64(),
                "sequence": _int64(),
                "periodName": {"type": "string"},
                "isHome": {"type": "boolean"},
                "player": _ref("Player"),
                "text": {"type": "string"},
                "time": {"type": "number"},
                "type": {"type": "string"},
            }
        ),
        "EventCommentsEnvelope": _obj(
            {
                "comments": _array(_ref("EventComment")),
                "home": _ref("EventCommentSideStyle"),
                "away": _ref("EventCommentSideStyle"),
            }
        ),
        "EventGraphPoint": _obj(
            {
                "minute": {"type": "number"},
                "value": _int32(),
            }
        ),
        "EventGraphEnvelope": _obj(
            {
                "graphPoints": _array(_ref("EventGraphPoint")),
                "periodTime": _int32(),
                "periodCount": _int32(),
                "overtimeLength": _int32(),
            }
        ),
        "HeatmapPoint": _obj(
            {
                "x": {"type": "number"},
                "y": {"type": "number"},
            }
        ),
        "EventTeamHeatmapEnvelope": _obj(
            {
                "playerPoints": _array(_ref("HeatmapPoint")),
                "goalkeeperPoints": _array(_ref("HeatmapPoint")),
            }
        ),
        "EventMarketChoice": _obj(
            {
                "sourceId": _int64(),
                "name": {"type": "string"},
                "changeValue": _int32(),
                "fractionalValue": {"type": "string"},
                "initialFractionalValue": {"type": "string"},
            }
        ),
        "EventMarket": _obj(
            {
                "id": _int64(),
                "eventId": _int64(),
                "providerId": _int64(),
                "fid": _int64(),
                "marketId": _int64(),
                "sourceId": _int64(),
                "marketGroup": {"type": "string"},
                "marketName": {"type": "string"},
                "marketPeriod": {"type": "string"},
                "structureType": _int32(),
                "choiceGroup": {"type": "string"},
                "isLive": {"type": "boolean"},
                "suspended": {"type": "boolean"},
                "choices": _array(_ref("EventMarketChoice")),
            }
        ),
        "EventWinningOddsSide": _obj(
            {
                "side": {"type": "string"},
                "oddsId": _int64(),
                "actual": _int32(),
                "expected": _int32(),
                "fractionalValue": {"type": "string"},
            }
        ),
        "StandingRow": _obj(
            {
                "id": _int64(),
                "teamId": _int64(),
                "position": _int32(),
                "matches": _int32(),
                "wins": _int32(),
                "draws": _int32(),
                "losses": _int32(),
                "points": _int32(),
                "scoresFor": _int32(),
                "scoresAgainst": _int32(),
                "scoreDiffFormatted": {"type": "string"},
                "promotionId": _int64(),
                "descriptions": _ref("FreeFormObject"),
            }
        ),
        "Standing": _obj(
            {
                "id": _int64(),
                "seasonId": _int64(),
                "tournamentId": _int64(),
                "name": {"type": "string"},
                "type": {"type": "string"},
                "updatedAtTimestamp": _int64(),
                "tieBreakingRuleId": _int64(),
                "descriptions": _ref("FreeFormObject"),
                "rows": _array(_ref("StandingRow")),
            }
        ),
    }
    schemas.update(
        {
            "SeasonStatisticsInfoEnvelope": _obj(
                {
                    "hideHomeAndAway": {"type": "boolean"},
                    "teams": _array(_ref("Team")),
                    "statisticsGroups": _ref("FreeFormObject"),
                    "nationalities": {
                        "type": "object",
                        "additionalProperties": {"type": "string"},
                    },
                }
            ),
            "SeasonStatisticsResult": _build_statistics_result_schema(),
            "SeasonStatisticsEnvelope": _obj(
                {
                    "page": _int32(),
                    "pages": _int32(),
                    "limit": _int32(),
                    "offset": _int32(),
                    "order": {"type": "string"},
                    "accumulation": {"type": "string"},
                    "group": {"type": "string"},
                    "fields": {"type": "array", "items": {"type": "string"}},
                    "filters": {"type": "array", "items": _ref("FreeFormObject")},
                    "results": _array(_ref("SeasonStatisticsResult")),
                }
            ),
        }
    )
    schemas.update(
        {
            "PlayerTransferHistoryItem": _obj(
                {
                    "id": _int64(),
                    "playerId": _int64(),
                    "transferFromTeamId": _int64(),
                    "transferToTeamId": _int64(),
                    "fromTeamName": {"type": "string"},
                    "toTeamName": {"type": "string"},
                    "transferDateTimestamp": _int64(),
                    "transferFee": _int32(),
                    "transferFeeDescription": {"type": "string"},
                    "transferFeeRaw": _ref("FreeFormObject"),
                    "type": _int32(),
                }
            ),
            "PlayerSeasonStatisticsMetrics": _build_player_statistics_schema(),
            "PlayerStatisticsSeasonItem": _obj(
                {
                    "statistics": _ref("PlayerSeasonStatisticsMetrics"),
                    "team": _ref("Team"),
                    "uniqueTournament": _ref("UniqueTournament"),
                    "season": _ref("Season"),
                    "year": {"type": "string"},
                    "startYear": _int32(),
                    "endYear": _int32(),
                }
            ),
            "UniqueTournamentSeasonsItem": _obj(
                {
                    "uniqueTournament": _ref("UniqueTournament"),
                    "seasons": _array(_ref("Season")),
                    "allTimeSeasonId": _int64(),
                }
            ),
            "EntityStatisticsSeasonsEnvelope": _obj(
                {
                    "uniqueTournamentSeasons": _array(_ref("UniqueTournamentSeasonsItem")),
                    "typesMap": _ref("TypeMap"),
                }
            ),
            "PlayerStatisticsEnvelope": _obj(
                {
                    "seasons": _array(_ref("PlayerStatisticsSeasonItem")),
                    "typesMap": _ref("TypeMap"),
                }
            ),
            "PlayerSeasonOverallEnvelope": _obj(
                {"statistics": _ref("FreeFormObject"), "team": _ref("Team")}
            ),
            "TeamSeasonOverallEnvelope": _obj({"statistics": _ref("FreeFormObject")}),
            "PlayerHeatmapOverallEnvelope": _obj(
                {
                    "heatmap": {"type": "array", "items": _ref("FreeFormObject")},
                    "events": {"type": "array", "items": _ref("FreeFormObject")},
                }
            ),
            "TeamPerformanceGraphEnvelope": _obj({"graphData": _ref("FreeFormObject")}),
            "TopPlayerEntry": _obj(
                {
                    "metricName": {"type": "string"},
                    "ordinal": _int32(),
                    "playerId": _int64(),
                    "teamId": _int64(),
                    "eventId": _int64(),
                    "playedEnough": {"type": "boolean"},
                    "statistic": {"type": "number"},
                    "statisticsId": _int64(),
                    "statisticsPayload": _ref("FreeFormObject"),
                }
            ),
            "TopTeamEntry": _obj(
                {
                    "metricName": {"type": "string"},
                    "ordinal": _int32(),
                    "teamId": _int64(),
                    "statisticsId": _int64(),
                    "statisticsPayload": _ref("FreeFormObject"),
                }
            ),
            "SeasonGroup": _obj(
                {
                    "uniqueTournamentId": _int64(),
                    "seasonId": _int64(),
                    "tournamentId": _int64(),
                    "groupName": {"type": "string"},
                }
            ),
            "SeasonRound": _obj(
                {
                    "round": _int32(),
                    "name": {"type": "string"},
                    "slug": {"type": "string"},
                    "isCurrent": {"type": "boolean"},
                }
            ),
            "SeasonRoundsEnvelope": _obj(
                {
                    "currentRound": _ref("SeasonRound"),
                    "rounds": _array(_ref("SeasonRound")),
                }
            ),
            "SeasonCupTreeParticipant": _obj(
                {
                    "id": _int64(),
                    "order": _int32(),
                    "winner": {"type": "boolean"},
                    "team": _ref("Team"),
                }
            ),
            "SeasonCupTreeBlock": _obj(
                {
                    "id": _int64(),
                    "blockId": _int64(),
                    "order": _int32(),
                    "finished": {"type": "boolean"},
                    "matchesInRound": _int32(),
                    "result": {"type": "string"},
                    "homeTeamScore": {"type": "string"},
                    "awayTeamScore": {"type": "string"},
                    "participants": _array(_ref("SeasonCupTreeParticipant")),
                    "hasNextRoundLink": {"type": "boolean"},
                    "events": {"type": "array", "items": _int64()},
                    "seriesStartDateTimestamp": _int64(),
                    "automaticProgression": {"type": "boolean"},
                }
            ),
            "SeasonCupTreeRound": _obj(
                {
                    "order": _int32(),
                    "type": _int32(),
                    "description": {"type": "string"},
                    "blocks": _array(_ref("SeasonCupTreeBlock")),
                }
            ),
            "SeasonCupTree": _obj(
                {
                    "id": _int64(),
                    "name": {"type": "string"},
                    "tournament": _ref("Tournament"),
                    "currentRound": _int32(),
                    "rounds": _array(_ref("SeasonCupTreeRound")),
                }
            ),
            "SeasonCupTreesEnvelope": _obj(
                {
                    "cupTrees": _array(_ref("SeasonCupTree")),
                }
            ),
            "PlayerOfTheSeasonEnvelope": _obj(
                {
                    "player": _ref("Player"),
                    "team": _ref("Team"),
                    "statistics": _ref("FreeFormObject"),
                    "playerOfTheTournament": {"type": "boolean"},
                }
            ),
            "PlayerOfTheSeasonRaceEnvelope": _obj(
                {
                    "topPlayers": _array(_ref("TopPlayerEntry")),
                    "statisticsType": _ref("FreeFormObject"),
                }
            ),
            "Period": _obj(
                {
                    "id": _int64(),
                    "uniqueTournamentId": _int64(),
                    "seasonId": _int64(),
                    "periodName": {"type": "string"},
                    "type": {"type": "string"},
                    "startDateTimestamp": _int64(),
                    "createdAtTimestamp": _int64(),
                    "roundName": {"type": "string"},
                    "roundNumber": _int32(),
                    "roundSlug": {"type": "string"},
                }
            ),
            "TeamOfTheWeekPlayer": _obj(
                {
                    "periodId": _int64(),
                    "entryId": _int64(),
                    "playerId": _int64(),
                    "teamId": _int64(),
                    "orderValue": _int32(),
                    "rating": {"type": "string"},
                }
            ),
            "TeamOfTheWeekEnvelope": _obj(
                {
                    "formation": {"type": "string"},
                    "players": _array(_ref("TeamOfTheWeekPlayer")),
                }
            ),
            "TournamentTeamEventBucket": _obj(
                {
                    "level1Key": {"type": "string"},
                    "level2Key": {"type": "string"},
                    "eventId": _int64(),
                    "ordinal": _int32(),
                }
            ),
            "OperationalHealth": _obj(
                {
                    "service": {"type": "string"},
                    "snapshot_count": _int64(),
                    "capability_rollup_count": _int64(),
                    "live_hot_count": _int64(),
                    "live_warm_count": _int64(),
                    "live_cold_count": _int64(),
                    "database_ok": {"type": "boolean"},
                    "redis_ok": {"type": "boolean"},
                    "redis_backend_kind": {"type": "string"},
                    "drift_summary": _ref("DriftSummary"),
                    "coverage_summary": _ref("CoverageRollupSummary"),
                    "coverage_alert_summary": _ref("CoverageAlertSummary"),
                    "reconcile_policy_summary": _ref("ReconcilePolicySummary"),
                }
            ),
            "DriftFlag": _obj(
                {
                    "surface": {"type": "string"},
                    "sport_slug": {"type": "string"},
                    "reason": {"type": "string"},
                }
            ),
            "DriftSummary": _obj(
                {
                    "flag_count": _int64(),
                    "flags": _array(_ref("DriftFlag")),
                }
            ),
            "CoverageRollupSummary": _obj(
                {
                    "tracked_scope_count": _int64(),
                    "fresh_scope_count": _int64(),
                    "stale_scope_count": _int64(),
                    "other_scope_count": _int64(),
                    "source_count": _int64(),
                    "sport_count": _int64(),
                    "surface_count": _int64(),
                    "avg_completeness_ratio": {"type": "number"},
                }
            ),
            "CoverageAlert": _obj(
                {
                    "severity": {"type": "string"},
                    "reason": {"type": "string"},
                    "stale_scope_count": _int64(),
                }
            ),
            "CoverageAlertSummary": _obj(
                {
                    "flag_count": _int64(),
                    "flags": _array(_ref("CoverageAlert")),
                }
            ),
            "ReconcilePolicySourceEntry": _obj(
                {
                    "source_slug": {"type": "string"},
                    "priority": _int64(),
                }
            ),
            "ReconcilePolicySummary": _obj(
                {
                    "policy_enabled": {"type": "boolean"},
                    "primary_source_slug": {"type": "string", "nullable": True},
                    "source_count": _int64(),
                    "sources": _array(_ref("ReconcilePolicySourceEntry")),
                }
            ),
            "SnapshotSummary": _obj(
                {
                    "raw_requests": _int64(),
                    "raw_snapshots": _int64(),
                    "trace_count": _int64(),
                    "endpoint_pattern_count": _int64(),
                    "event_context_count": _int64(),
                    "http_200_count": _int64(),
                    "http_404_count": _int64(),
                    "http_5xx_count": _int64(),
                    "latest_fetched_at": {"type": "string", "format": "date-time"},
                }
            ),
            "QueueStreamSummary": _obj(
                {
                    "stream": {"type": "string"},
                    "group": {"type": "string"},
                    "pending_total": _int64(),
                    "smallest_id": {"type": "string"},
                    "largest_id": {"type": "string"},
                    "consumers": {"type": "object", "additionalProperties": _int64()},
                }
            ),
            "QueueSummary": _obj(
                {
                    "redis_backend_kind": {"type": "string"},
                    "live_lanes": {
                        "type": "object",
                        "properties": {
                            "hot": _int64(),
                            "warm": _int64(),
                            "cold": _int64(),
                        },
                    },
                    "streams": _array(_ref("QueueStreamSummary")),
                    "delayed_total": _int64(),
                    "delayed_due": _int64(),
                }
            ),
            "JobRunEntry": _obj(
                {
                    "job_run_id": {"type": "string"},
                    "job_id": {"type": "string"},
                    "job_type": {"type": "string"},
                    "sport_slug": {"type": "string"},
                    "entity_type": {"type": "string"},
                    "entity_id": _int64(),
                    "scope": {"type": "string"},
                    "worker_id": {"type": "string"},
                    "attempt": _int32(),
                    "status": {"type": "string"},
                    "started_at": {"type": "string", "format": "date-time"},
                    "finished_at": {"type": "string", "format": "date-time"},
                    "duration_ms": _int64(),
                    "error_class": {"type": "string"},
                    "error_message": {"type": "string"},
                    "retry_scheduled_for": {"type": "string", "format": "date-time"},
                }
            ),
            "JobRunsEnvelope": _obj(
                {
                    "limit": _int32(),
                    "status_counts": {"type": "object", "additionalProperties": _int64()},
                    "jobRuns": _array(_ref("JobRunEntry")),
                }
            ),
            "CoverageSummaryEntry": _obj(
                {
                    "source_slug": {"type": "string"},
                    "sport_slug": {"type": "string"},
                    "surface_name": {"type": "string"},
                    "freshness_status": {"type": "string"},
                    "tracked_scopes": _int64(),
                }
            ),
            "CoverageSummary": _obj(
                {
                    "coverage": _array(_ref("CoverageSummaryEntry")),
                }
            ),
        }
    )
    return schemas


def _build_player_statistics_schema() -> dict[str, Any]:
    properties: dict[str, Any] = {
        "id": _int64(),
        "type": {"type": "string"},
    }
    for json_field, column_name in sorted(_PLAYER_STATISTICS_METRIC_COLUMN_MAP.items()):
        properties[json_field] = _int32() if column_name in _PLAYER_STATISTICS_INTEGER_COLUMNS else {"type": "number"}
    return _obj(properties)


def _build_statistics_result_schema() -> dict[str, Any]:
    properties: dict[str, Any] = {
        "rowNumber": _int32(),
        "player": _ref("Player"),
        "team": _ref("Team"),
    }
    for json_field, column_name in sorted(_METRIC_COLUMN_MAP.items()):
        properties[json_field] = _int32() if column_name in _INTEGER_METRIC_COLUMNS else {"type": "number"}
    return _obj(properties)


def _build_viewer_html(openapi_file_name: str) -> str:
    return f"""<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Sofascore Local Swagger</title>
    <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
    <style>
      body {{
        margin: 0;
        background: #f5f6f8;
      }}
      .topbar-wrapper img {{
        display: none;
      }}
      .swagger-ui .topbar {{
        background: #111827;
      }}
    </style>
  </head>
  <body>
    <div id="swagger-ui"></div>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
    <script>
      const tagOrder = {json.dumps([name for name, _ in _LOCAL_API_TAGS])};
      window.onload = () => {{
        window.ui = SwaggerUIBundle({{
          url: "./{openapi_file_name}",
          dom_id: "#swagger-ui",
          deepLinking: true,
          presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
          layout: "StandaloneLayout",
          docExpansion: "list",
          defaultModelsExpandDepth: 2,
          displayRequestDuration: true,
          filter: true,
          operationsSorter: (a, b) => {{
            const methodOrder = ["get", "post", "put", "patch", "delete", "options", "trace"];
            const methodDelta = methodOrder.indexOf(a.get("method")) - methodOrder.indexOf(b.get("method"));
            if (methodDelta !== 0) {{
              return methodDelta;
            }}
            return a.get("path").localeCompare(b.get("path"));
          }},
          tagsSorter: (a, b) => {{
            const left = tagOrder.indexOf(String(a));
            const right = tagOrder.indexOf(String(b));
            const leftRank = left === -1 ? tagOrder.length : left;
            const rightRank = right === -1 ? tagOrder.length : right;
            if (leftRank !== rightRank) {{
              return leftRank - rightRank;
            }}
            return String(a).localeCompare(String(b));
          }},
        }});
      }};
    </script>
  </body>
</html>
"""


def resolve_openapi_base_urls(
    *,
    primary_base_url: str | None = None,
    public_base_urls: tuple[str, ...] | list[str] | None = None,
) -> tuple[str, ...]:
    env = _load_project_env()
    configured_public_urls = _split_public_base_urls(env.get("LOCAL_API_PUBLIC_BASE_URLS"))

    candidates: list[str] = []
    candidates.extend(str(url) for url in (public_base_urls or ()))
    candidates.extend(configured_public_urls)
    candidates.extend(_local_base_urls(primary_base_url))
    candidates.extend(DEFAULT_LOCAL_API_BASE_URLS)
    return _normalize_openapi_base_urls(tuple(candidates))


def resolve_request_openapi_base_urls(
    *,
    request_headers: Mapping[str, str] | None = None,
    configured_base_urls: tuple[str, ...] | list[str] | None = None,
) -> tuple[str, ...]:
    candidates: list[str] = []
    candidates.extend(_forwarded_request_base_urls(request_headers))
    candidates.extend(str(url) for url in (configured_base_urls or resolve_openapi_base_urls()))
    return _normalize_openapi_base_urls(tuple(candidates))


def _normalize_openapi_base_urls(base_urls: tuple[str, ...]) -> tuple[str, ...]:
    deduped: list[str] = []
    seen: set[str] = set()
    for raw_url in base_urls:
        cleaned_url = _clean_base_url(raw_url)
        if not cleaned_url or cleaned_url in seen:
            continue
        seen.add(cleaned_url)
        deduped.append(cleaned_url)
    public_urls = [url for url in deduped if not _is_local_base_url(url)]
    local_urls = [url for url in deduped if _is_local_base_url(url)]
    return tuple(public_urls + local_urls)


def _forwarded_request_base_urls(request_headers: Mapping[str, str] | None) -> tuple[str, ...]:
    candidates: list[str] = []
    forwarded_url = _forwarded_header_base_url(request_headers)
    if forwarded_url:
        candidates.append(forwarded_url)
    x_forwarded_url = _x_forwarded_header_base_url(request_headers)
    if x_forwarded_url and x_forwarded_url not in candidates:
        candidates.append(x_forwarded_url)
    return tuple(candidates)


def _forwarded_header_base_url(request_headers: Mapping[str, str] | None) -> str | None:
    forwarded = _get_header_value(request_headers, "Forwarded")
    if not forwarded:
        return None
    first_entry = forwarded.split(",", 1)[0].strip()
    if not first_entry:
        return None
    params: dict[str, str] = {}
    for part in first_entry.split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        params[key.strip().lower()] = value.strip().strip('"')
    proto = (params.get("proto") or "").strip().lower()
    host = (params.get("host") or "").strip()
    if proto not in {"http", "https"} or not host:
        return None
    return _clean_base_url(f"{proto}://{host}")


def _x_forwarded_header_base_url(request_headers: Mapping[str, str] | None) -> str | None:
    proto = _forwarded_header_token(request_headers, "X-Forwarded-Proto")
    if proto not in {"http", "https"}:
        return None
    host = _forwarded_header_token(request_headers, "X-Forwarded-Host") or _forwarded_header_token(
        request_headers,
        "Host",
    )
    if not host:
        return None
    port = _forwarded_header_token(request_headers, "X-Forwarded-Port")
    host_with_port = _append_forwarded_port(host=host, port=port, proto=proto)
    return _clean_base_url(f"{proto}://{host_with_port}")


def _forwarded_header_token(request_headers: Mapping[str, str] | None, name: str) -> str | None:
    raw_value = _get_header_value(request_headers, name)
    if not raw_value:
        return None
    return raw_value.split(",", 1)[0].strip().strip('"')


def _get_header_value(request_headers: Mapping[str, str] | None, name: str) -> str | None:
    if request_headers is None:
        return None
    getter = getattr(request_headers, "get", None)
    if callable(getter):
        value = getter(name)
        if value is not None:
            return str(value)
    for key, value in request_headers.items():
        if str(key).lower() == name.lower():
            return str(value)
    return None


def _append_forwarded_port(*, host: str, port: str | None, proto: str) -> str:
    cleaned_host = host.strip()
    if not cleaned_host or not port or ":" in cleaned_host:
        return cleaned_host
    cleaned_port = port.strip()
    if not cleaned_port:
        return cleaned_host
    default_port = "443" if proto == "https" else "80"
    if cleaned_port == default_port:
        return cleaned_host
    return f"{cleaned_host}:{cleaned_port}"


def _local_base_urls(primary_base_url: str | None) -> tuple[str, ...]:
    cleaned_url = _clean_base_url(primary_base_url)
    if not cleaned_url:
        return ()
    parsed = urlsplit(cleaned_url)
    host = (parsed.hostname or "").lower()
    if host == "0.0.0.0":
        port = f":{parsed.port}" if parsed.port else ""
        return (
            f"{parsed.scheme}://127.0.0.1{port}",
            f"{parsed.scheme}://localhost{port}",
        )
    return (cleaned_url,)


def _split_public_base_urls(raw_value: str | None) -> tuple[str, ...]:
    if not raw_value:
        return ()
    parts = re.split(r"[\s,;]+", raw_value.strip())
    return tuple(part for part in (_clean_base_url(item) for item in parts) if part)


def _load_project_env() -> dict[str, str]:
    merged = dict(os.environ)
    env_path = PROJECT_ROOT / ".env"
    if not env_path.exists():
        return merged
    for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        merged.setdefault(key.strip(), value.strip().strip("'\""))
    return merged


def _clean_base_url(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().rstrip("/")
    if not normalized:
        return None
    return normalized


def _is_local_base_url(base_url: str) -> bool:
    hostname = (urlsplit(base_url).hostname or "").lower()
    return hostname in _LOCAL_SWAGGER_HOSTS


def _openapi_server_entry(url: str) -> dict[str, str]:
    description = "Public API base URL" if not _is_local_base_url(url) else "Local API base URL"
    return {"url": url, "description": description}


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _obj(properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "object", "properties": properties}


def _array(item_schema: dict[str, Any]) -> dict[str, Any]:
    return {"type": "array", "items": item_schema}


def _ref(name: str) -> dict[str, Any]:
    return {"$ref": f"#/components/schemas/{name}"}


def _int32() -> dict[str, Any]:
    return {"type": "integer", "format": "int32"}


def _int64() -> dict[str, Any]:
    return {"type": "integer", "format": "int64"}


def _envelope(name: str, value_schema: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {name: value_schema},
    }


def _envelope_schema(name: str, value_schema: dict[str, Any]) -> dict[str, Any]:
    return _envelope(name, value_schema)


def _path_param(name: str, schema_type: str, description: str, enum: list[str] | None = None) -> dict[str, Any]:
    schema: dict[str, Any]
    if schema_type == "integer":
        schema = _int64()
    else:
        schema = {"type": schema_type}
    if enum:
        schema["enum"] = enum
    return {
        "name": name,
        "in": "path",
        "required": True,
        "description": description,
        "schema": schema,
    }


def _query_param(
    name: str,
    schema_type: str,
    description: str,
    *,
    item_type: str | None = None,
    style: str | None = None,
    explode: bool | None = None,
) -> dict[str, Any]:
    if schema_type == "array":
        assert item_type is not None
        items = _int64() if item_type == "integer" else {"type": item_type}
        schema: dict[str, Any] = {"type": "array", "items": items}
    elif schema_type == "integer":
        schema = _int64()
    else:
        schema = {"type": schema_type}
    value = {
        "name": name,
        "in": "query",
        "required": False,
        "description": description,
        "schema": schema,
    }
    if style is not None:
        value["style"] = style
    if explode is not None:
        value["explode"] = explode
    return value


if __name__ == "__main__":
    raise SystemExit(main())
