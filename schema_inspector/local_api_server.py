"""Serve the ingested multi-sport dataset through local Sofascore-style HTTP routes."""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .db import DatabaseConfig, load_database_config
from .endpoints import SofascoreEndpoint, local_api_endpoints
from .ops.health import collect_health_report
from .queue.delayed import DELAYED_JOBS_KEY
from .queue.live_state import LiveEventStateStore
from .queue.streams import (
    RedisStreamQueue,
    STREAM_DISCOVERY,
    STREAM_HISTORICAL_DISCOVERY,
    STREAM_HISTORICAL_ENRICHMENT,
    STREAM_HISTORICAL_HYDRATE,
    STREAM_HISTORICAL_MAINTENANCE,
    STREAM_HISTORICAL_TOURNAMENT,
    STREAM_HYDRATE,
    STREAM_LIVE_HOT,
    STREAM_LIVE_WARM,
    STREAM_MAINTENANCE,
)

from .local_swagger_builder import (
    _build_viewer_html,
    _empty_summary,
    _load_summary,
    build_openapi_document,
    resolve_openapi_base_urls,
)

_ALL_ENDPOINTS = local_api_endpoints()
_QUEUE_GROUPS = (
    (STREAM_DISCOVERY, "cg:discovery"),
    (STREAM_HYDRATE, "cg:hydrate"),
    (STREAM_HISTORICAL_DISCOVERY, "cg:historical_discovery"),
    (STREAM_HISTORICAL_TOURNAMENT, "cg:historical_tournament"),
    (STREAM_HISTORICAL_ENRICHMENT, "cg:historical_enrichment"),
    (STREAM_HISTORICAL_HYDRATE, "cg:historical_hydrate"),
    (STREAM_LIVE_HOT, "cg:live_hot"),
    (STREAM_LIVE_WARM, "cg:live_warm"),
    (STREAM_MAINTENANCE, "cg:maintenance"),
    (STREAM_HISTORICAL_MAINTENANCE, "cg:historical_maintenance"),
)


@dataclass(frozen=True)
class RouteSpec:
    """One local HTTP route backed by the API snapshot cache."""

    endpoint: SofascoreEndpoint
    path_regex: re.Pattern[str]
    context_entity_type: str | None
    context_param_name: str | None

    def match(self, path: str) -> dict[str, str] | None:
        match = self.path_regex.fullmatch(path)
        if match is None:
            return None
        return match.groupdict()


@dataclass(frozen=True)
class ApiResponse:
    """Structured API response returned by the application."""

    status_code: int
    payload: Any


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Serve a local multi-sport read API over the ingested PostgreSQL snapshot cache. "
            "Swagger UI is available at the root path and API routes mirror Sofascore-style paths."
        ),
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind.")
    parser.add_argument("--port", type=int, default=8000, help="Port to serve the local API on.")
    parser.add_argument(
        "--database-url",
        default=None,
        help="Optional PostgreSQL DSN override. Falls back to SOFASCORE_DATABASE_URL / DATABASE_URL / POSTGRES_DSN.",
    )
    parser.add_argument(
        "--redis-url",
        default=None,
        help="Optional Redis URL for operational monitoring endpoints. Falls back to REDIS_URL / SOFASCORE_REDIS_URL.",
    )
    parser.add_argument(
        "--public-base-url",
        action="append",
        default=None,
        help=(
            "Optional public base URL to publish in Swagger/OpenAPI servers. "
            "Can be repeated and also falls back to LOCAL_API_PUBLIC_BASE_URLS."
        ),
    )
    args = parser.parse_args()

    database_config = load_database_config(dsn=args.database_url)
    base_url = f"http://{args.host}:{args.port}"
    application = LocalApiApplication(
        database_config=database_config,
        base_url=base_url,
        openapi_base_urls=resolve_openapi_base_urls(
            primary_base_url=base_url,
            public_base_urls=tuple(args.public_base_url or ()),
        ),
        redis_backend=_load_optional_redis_backend(args.redis_url),
    )
    server = LocalApiHttpServer((args.host, args.port), LocalApiRequestHandler, application)
    print(f"Serving local multi-sport API on {base_url}", flush=True)
    print(f"Swagger UI: {base_url}/", flush=True)
    print(f"OpenAPI JSON: {base_url}/openapi.json", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


class LocalApiApplication:
    """Local API application backed by api_payload_snapshot rows."""

    def __init__(
        self,
        *,
        database_config: DatabaseConfig,
        base_url: str,
        openapi_base_urls: tuple[str, ...] | None = None,
        redis_backend: Any | None = None,
    ) -> None:
        self.database_config = database_config
        self.base_url = base_url.rstrip("/")
        self.openapi_base_urls = tuple(openapi_base_urls or (self.base_url,))
        self.redis_backend = redis_backend
        self.stream_queue = RedisStreamQueue(redis_backend) if redis_backend is not None else None
        self.live_state_store = LiveEventStateStore(redis_backend) if redis_backend is not None else None
        self.routes = build_route_specs()
        self.swagger_html = _build_viewer_html("openapi.json")
        self.openapi_document = asyncio.run(self._build_openapi_document())
        self.openapi_json = json.dumps(
            self.openapi_document,
            ensure_ascii=False,
            indent=2,
            default=_json_default,
        ).encode("utf-8")

    async def _build_openapi_document(self) -> dict[str, Any]:
        try:
            summary = await _load_summary(self.database_config.dsn)
        except Exception:
            summary = _empty_summary()
        return build_openapi_document(summary, base_urls=self.openapi_base_urls)

    async def handle_api_get(self, path: str, raw_query: str) -> ApiResponse:
        route_match = match_route(path, self.routes)
        if route_match is None:
            return ApiResponse(
                status_code=HTTPStatus.NOT_FOUND,
                payload={"error": "Route is not registered in the local multi-sport API.", "path": path},
            )

        route, path_params = route_match
        payload = await self._fetch_snapshot_payload(route, path, raw_query, path_params)
        if payload is None:
            payload = await self._fetch_normalized_payload(route, path, raw_query, path_params)
        if payload is None:
            return ApiResponse(
                status_code=HTTPStatus.NOT_FOUND,
                payload={
                    "error": "Requested path exists in the local API contract, but no ingested payload matched this path/query yet.",
                    "path": path,
                    "query": parse_qs(raw_query, keep_blank_values=True),
                    "endpointPattern": route.endpoint.pattern,
                },
            )

        return ApiResponse(status_code=HTTPStatus.OK, payload=payload)

    async def handle_ops_get(self, path: str, raw_query: str) -> ApiResponse:
        if path == "/ops/health":
            return ApiResponse(status_code=HTTPStatus.OK, payload=await self._fetch_ops_health_payload())
        if path == "/ops/snapshots/summary":
            return ApiResponse(status_code=HTTPStatus.OK, payload=await self._fetch_ops_snapshots_summary_payload())
        if path == "/ops/queues/summary":
            return ApiResponse(status_code=HTTPStatus.OK, payload=await self._fetch_ops_queue_summary_payload())
        if path == "/ops/jobs/runs":
            limit = _query_int(raw_query, "limit", default=20, minimum=1, maximum=200)
            return ApiResponse(status_code=HTTPStatus.OK, payload=await self._fetch_ops_job_runs_payload(limit))
        return ApiResponse(
            status_code=HTTPStatus.NOT_FOUND,
            payload={"error": "Route is not registered in the operational API.", "path": path},
        )

    async def _fetch_snapshot_payload(
        self,
        route: RouteSpec,
        path: str,
        raw_query: str,
        path_params: dict[str, str],
    ) -> Any | None:
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError("asyncpg is required to serve the local multi-sport API.") from exc

        context_value = _parse_context_value(route, path_params)
        query = "SELECT source_url, payload FROM api_payload_snapshot WHERE endpoint_pattern = $1"
        arguments: list[Any] = [route.endpoint.pattern]
        if route.context_entity_type is not None and context_value is not None:
            query += " AND context_entity_type = $2 AND context_entity_id = $3"
            arguments.extend([route.context_entity_type, context_value])
        query += " ORDER BY id DESC LIMIT 500"

        connection = await asyncpg.connect(
            self.database_config.dsn,
            command_timeout=self.database_config.command_timeout,
        )
        try:
            rows = await connection.fetch(query, *arguments)
        finally:
            await connection.close()

        latest_path_match: Any | None = None
        for row in rows:
            split = urlsplit(str(row["source_url"]))
            if split.path != path:
                continue
            decoded_payload = _decode_snapshot_payload(row["payload"])
            if _query_maps_equal(split.query, raw_query):
                return decoded_payload
            if not raw_query and latest_path_match is None:
                latest_path_match = decoded_payload

        if route.endpoint.query_template and not raw_query:
            return latest_path_match
        return None

    async def _fetch_normalized_payload(
        self,
        route: RouteSpec,
        path: str,
        raw_query: str,
        path_params: dict[str, str],
    ) -> Any | None:
        if raw_query:
            return None

        sport_slug = _extract_sport_slug_from_categories_path(path)
        if sport_slug is not None:
            return await self._fetch_sport_categories_payload(sport_slug)

        category_seed = _extract_category_seed_request(path, path_params)
        if category_seed is not None:
            sport_slug, observed_date, timezone_offset_seconds = category_seed
            return await self._fetch_sport_category_seed_payload(
                sport_slug=sport_slug,
                observed_date=observed_date,
                timezone_offset_seconds=timezone_offset_seconds,
            )

        return None

    async def _fetch_sport_categories_payload(self, sport_slug: str) -> dict[str, Any] | None:
        connection = await self._connect()
        try:
            rows = await connection.fetch(
                """
                SELECT
                    c.id,
                    c.slug,
                    c.name,
                    c.flag,
                    c.alpha2,
                    c.priority,
                    c.country_alpha2,
                    c.field_translations,
                    s.id AS sport_id,
                    s.slug AS sport_slug,
                    s.name AS sport_name
                FROM category AS c
                JOIN sport AS s ON s.id = c.sport_id
                WHERE s.slug = $1
                ORDER BY c.priority NULLS LAST, c.name, c.id
                """,
                sport_slug,
            )
        finally:
            await connection.close()

        if not rows:
            return None
        return {"categories": [_serialize_category_row(row) for row in rows]}

    async def _fetch_sport_category_seed_payload(
        self,
        *,
        sport_slug: str,
        observed_date: str,
        timezone_offset_seconds: int,
    ) -> dict[str, Any] | None:
        connection = await self._connect()
        try:
            rows = await connection.fetch(
                """
                SELECT
                    cds.observed_date,
                    cds.timezone_offset_seconds,
                    cds.total_events,
                    cds.total_event_player_statistics,
                    cds.total_videos,
                    c.id,
                    c.slug,
                    c.name,
                    c.flag,
                    c.alpha2,
                    c.priority,
                    c.country_alpha2,
                    c.field_translations,
                    s.id AS sport_id,
                    s.slug AS sport_slug,
                    s.name AS sport_name,
                    COALESCE((
                        SELECT array_agg(cdut.unique_tournament_id ORDER BY cdut.ordinal)
                        FROM category_daily_unique_tournament AS cdut
                        WHERE cdut.observed_date = cds.observed_date
                          AND cdut.timezone_offset_seconds = cds.timezone_offset_seconds
                          AND cdut.category_id = cds.category_id
                    ), ARRAY[]::BIGINT[]) AS unique_tournament_ids,
                    COALESCE((
                        SELECT array_agg(cdt.team_id ORDER BY cdt.ordinal)
                        FROM category_daily_team AS cdt
                        WHERE cdt.observed_date = cds.observed_date
                          AND cdt.timezone_offset_seconds = cds.timezone_offset_seconds
                          AND cdt.category_id = cds.category_id
                    ), ARRAY[]::BIGINT[]) AS team_ids
                FROM category_daily_summary AS cds
                JOIN category AS c ON c.id = cds.category_id
                JOIN sport AS s ON s.id = c.sport_id
                WHERE s.slug = $1
                  AND cds.observed_date = $2::date
                  AND cds.timezone_offset_seconds = $3
                ORDER BY c.priority NULLS LAST, c.name, c.id
                """,
                sport_slug,
                observed_date,
                timezone_offset_seconds,
            )
        finally:
            await connection.close()

        if not rows:
            return None

        return {
            "categories": [
                {
                    "observedDate": _serialize_scalar(row["observed_date"]),
                    "timezoneOffsetSeconds": int(row["timezone_offset_seconds"] or 0),
                    "category": _serialize_category_row(row),
                    "totalEvents": _serialize_optional_int(row["total_events"]),
                    "totalEventPlayerStatistics": _serialize_optional_int(row["total_event_player_statistics"]),
                    "totalVideos": _serialize_optional_int(row["total_videos"]),
                    "uniqueTournamentIds": [int(value) for value in (row["unique_tournament_ids"] or [])],
                    "teamIds": [int(value) for value in (row["team_ids"] or [])],
                }
                for row in rows
            ]
        }

    async def _fetch_ops_health_payload(self) -> dict[str, Any]:
        connection = await self._connect()
        try:
            report = await collect_health_report(
                sql_executor=connection,
                live_state_store=self.live_state_store,
                redis_backend=self.redis_backend,
            )
        finally:
            await connection.close()
        payload = dataclasses.asdict(report)
        payload["service"] = "local-multisport-api"
        return payload

    async def _fetch_ops_snapshots_summary_payload(self) -> dict[str, Any]:
        connection = await self._connect()
        try:
            snapshot_row = await connection.fetchrow(
                """
                SELECT
                    COUNT(*)::bigint AS raw_snapshots,
                    COUNT(DISTINCT trace_id)::bigint AS trace_count,
                    COUNT(DISTINCT endpoint_pattern)::bigint AS endpoint_pattern_count,
                    COUNT(DISTINCT context_event_id)::bigint AS event_context_count,
                    COUNT(*) FILTER (WHERE http_status = 200)::bigint AS http_200_count,
                    COUNT(*) FILTER (WHERE http_status = 404)::bigint AS http_404_count,
                    COUNT(*) FILTER (WHERE http_status >= 500)::bigint AS http_5xx_count,
                    MAX(fetched_at) AS latest_fetched_at
                FROM api_payload_snapshot
                """
            )
            request_count = await connection.fetchval("SELECT COUNT(*) FROM api_request_log")
        finally:
            await connection.close()
        return {
            "raw_requests": int(request_count or 0),
            "raw_snapshots": int(snapshot_row["raw_snapshots"] or 0),
            "trace_count": int(snapshot_row["trace_count"] or 0),
            "endpoint_pattern_count": int(snapshot_row["endpoint_pattern_count"] or 0),
            "event_context_count": int(snapshot_row["event_context_count"] or 0),
            "http_200_count": int(snapshot_row["http_200_count"] or 0),
            "http_404_count": int(snapshot_row["http_404_count"] or 0),
            "http_5xx_count": int(snapshot_row["http_5xx_count"] or 0),
            "latest_fetched_at": _serialize_scalar(snapshot_row["latest_fetched_at"]),
        }

    async def _fetch_ops_queue_summary_payload(self) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        lane_counts = {
            "hot": _lane_count(self.live_state_store, "hot"),
            "warm": _lane_count(self.live_state_store, "warm"),
            "cold": _lane_count(self.live_state_store, "cold"),
        }
        streams: list[dict[str, Any]] = []
        if self.stream_queue is not None:
            for stream_name, group_name in _QUEUE_GROUPS:
                try:
                    summary = self.stream_queue.pending_summary(stream_name, group_name)
                except Exception:
                    summary = None
                try:
                    stream_length = self.stream_queue.stream_length(stream_name)
                except Exception:
                    stream_length = 0
                try:
                    group_info = self.stream_queue.group_info(stream_name, group_name)
                except Exception:
                    group_info = None
                streams.append(
                    {
                        "stream": stream_name,
                        "group": group_name,
                        "length": int(stream_length),
                        "pending_total": 0 if summary is None else int(summary.total),
                        "smallest_id": None if summary is None else summary.smallest_id,
                        "largest_id": None if summary is None else summary.largest_id,
                        "consumers": {} if summary is None else dict(summary.consumers),
                        "group_consumers": 0 if group_info is None else int(group_info.consumers),
                        "entries_read": None if group_info is None else group_info.entries_read,
                        "lag": None if group_info is None else group_info.lag,
                        "last_delivered_id": None if group_info is None else group_info.last_delivered_id,
                    }
                )
        delayed_total = 0
        delayed_due = 0
        if self.redis_backend is not None:
            delayed_total = len(tuple(self.redis_backend.zrangebyscore(DELAYED_JOBS_KEY, float("-inf"), float("inf"))))
            delayed_due = len(tuple(self.redis_backend.zrangebyscore(DELAYED_JOBS_KEY, float("-inf"), float(now_ms))))
        return {
            "redis_backend_kind": type(self.redis_backend).__name__ if self.redis_backend is not None else "none",
            "live_lanes": lane_counts,
            "streams": streams,
            "delayed_total": delayed_total,
            "delayed_due": delayed_due,
        }

    async def _fetch_ops_job_runs_payload(self, limit: int) -> dict[str, Any]:
        connection = await self._connect()
        try:
            rows = await connection.fetch(
                """
                SELECT
                    job_run_id,
                    job_id,
                    job_type,
                    sport_slug,
                    entity_type,
                    entity_id,
                    scope,
                    worker_id,
                    attempt,
                    status,
                    started_at,
                    finished_at,
                    duration_ms,
                    error_class,
                    error_message,
                    retry_scheduled_for
                FROM etl_job_run
                ORDER BY started_at DESC
                LIMIT $1
                """,
                int(limit),
            )
            status_rows = await connection.fetch(
                """
                SELECT status, COUNT(*)::bigint AS row_count
                FROM etl_job_run
                GROUP BY status
                ORDER BY status
                """
            )
        finally:
            await connection.close()
        return {
            "limit": int(limit),
            "status_counts": {
                str(row["status"]): int(row["row_count"] or 0)
                for row in status_rows
            },
            "jobRuns": [
                {
                    "job_run_id": str(row["job_run_id"]),
                    "job_id": str(row["job_id"]),
                    "job_type": str(row["job_type"]),
                    "sport_slug": _serialize_scalar(row["sport_slug"]),
                    "entity_type": _serialize_scalar(row["entity_type"]),
                    "entity_id": _serialize_scalar(row["entity_id"]),
                    "scope": _serialize_scalar(row["scope"]),
                    "worker_id": _serialize_scalar(row["worker_id"]),
                    "attempt": int(row["attempt"] or 0),
                    "status": str(row["status"]),
                    "started_at": _serialize_scalar(row["started_at"]),
                    "finished_at": _serialize_scalar(row["finished_at"]),
                    "duration_ms": _serialize_scalar(row["duration_ms"]),
                    "error_class": _serialize_scalar(row["error_class"]),
                    "error_message": _serialize_scalar(row["error_message"]),
                    "retry_scheduled_for": _serialize_scalar(row["retry_scheduled_for"]),
                }
                for row in rows
            ],
        }

    async def _connect(self):
        try:
            import asyncpg
        except ImportError as exc:
            raise RuntimeError("asyncpg is required to serve the local multi-sport API.") from exc
        return await asyncpg.connect(
            self.database_config.dsn,
            command_timeout=self.database_config.command_timeout,
        )


class LocalApiHttpServer(ThreadingHTTPServer):
    """HTTP server that carries the local application instance."""

    def __init__(self, server_address: tuple[str, int], handler_class, application: LocalApiApplication) -> None:
        super().__init__(server_address, handler_class)
        self.application = application


class LocalApiRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for Swagger UI and local API endpoints."""

    server: LocalApiHttpServer

    def do_GET(self) -> None:  # noqa: N802
        split = urlsplit(self.path)
        path = split.path or "/"
        if path in {"/", "/docs"}:
            self._send_html(HTTPStatus.OK, self.server.application.swagger_html)
            return
        if path == "/openapi.json":
            self._send_bytes(HTTPStatus.OK, self.server.application.openapi_json, "application/json; charset=utf-8")
            return
        if path == "/healthz":
            payload = {"ok": True, "service": "local-multisport-api"}
            self._send_json(HTTPStatus.OK, payload)
            return
        if path.startswith("/ops/"):
            response = asyncio.run(self.server.application.handle_ops_get(path, split.query))
            self._send_json(response.status_code, response.payload)
            return
        if path.startswith("/api/"):
            response = asyncio.run(self.server.application.handle_api_get(path, split.query))
            self._send_json(response.status_code, response.payload)
            return
        self._send_json(
            HTTPStatus.NOT_FOUND,
            {"error": "Unknown route.", "path": path},
        )

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self._send_common_headers("application/json; charset=utf-8")
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        message = format % args
        print(message, flush=True)

    def _send_html(self, status_code: int, payload: str) -> None:
        self._send_bytes(status_code, payload.encode("utf-8"), "text/html; charset=utf-8")

    def _send_json(self, status_code: int, payload: Any) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2, default=_json_default).encode("utf-8")
        self._send_bytes(status_code, body, "application/json; charset=utf-8")

    def _send_bytes(self, status_code: int, body: bytes, content_type: str) -> None:
        self.send_response(status_code)
        self._send_common_headers(content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_common_headers(self, content_type: str) -> None:
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Cache-Control", "no-store")


def build_route_specs() -> tuple[RouteSpec, ...]:
    routes: list[RouteSpec] = []
    for endpoint in _ALL_ENDPOINTS:
        routes.append(
            RouteSpec(
                endpoint=endpoint,
                path_regex=_compile_path_template(endpoint.path_template),
                context_entity_type=_infer_context_entity_type(endpoint.path_template),
                context_param_name=_infer_context_param_name(endpoint.path_template),
            )
        )
    return tuple(routes)


def match_route(path: str, routes: tuple[RouteSpec, ...]) -> tuple[RouteSpec, dict[str, str]] | None:
    for route in routes:
        params = route.match(path)
        if params is not None:
            return route, params
    return None


def _compile_path_template(path_template: str) -> re.Pattern[str]:
    pieces = ["^"]
    last_index = 0
    for match in re.finditer(r"\{([a-zA-Z0-9_]+)\}", path_template):
        pieces.append(re.escape(path_template[last_index:match.start()]))
        pieces.append(f"(?P<{match.group(1)}>[^/]+)")
        last_index = match.end()
    pieces.append(re.escape(path_template[last_index:]))
    pieces.append("$")
    return re.compile("".join(pieces))


def _infer_context_entity_type(path_template: str) -> str | None:
    if "team-of-the-week/{period_id}" in path_template:
        return "period"
    if path_template.startswith("/api/v1/event/"):
        return "event"
    if path_template.startswith("/api/v1/player/"):
        return "player"
    if path_template.startswith("/api/v1/team/"):
        return "team"
    if "/team/{team_id}/" in path_template:
        return "team"
    if "/season/{season_id}/" in path_template or path_template.endswith("/season/{season_id}"):
        return "season"
    if path_template.startswith("/api/v1/unique-tournament/"):
        return "unique_tournament"
    return None


def _infer_context_param_name(path_template: str) -> str | None:
    if "team-of-the-week/{period_id}" in path_template:
        return "period_id"
    if path_template.startswith("/api/v1/event/"):
        return "event_id"
    if path_template.startswith("/api/v1/player/"):
        return "player_id"
    if path_template.startswith("/api/v1/team/"):
        return "team_id"
    if "/team/{team_id}/" in path_template:
        return "team_id"
    if "/season/{season_id}/" in path_template or path_template.endswith("/season/{season_id}"):
        return "season_id"
    if path_template.startswith("/api/v1/unique-tournament/"):
        return "unique_tournament_id"
    return None


def _parse_context_value(route: RouteSpec, path_params: dict[str, str]) -> int | None:
    if route.context_param_name is None:
        return None
    raw_value = path_params.get(route.context_param_name)
    if raw_value is None:
        return None
    try:
        return int(raw_value)
    except ValueError:
        return None


def _decode_snapshot_payload(payload: Any) -> Any:
    if isinstance(payload, str):
        stripped = payload.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                return json.loads(stripped)
            except json.JSONDecodeError:
                return payload
    return payload


def _query_maps_equal(left_raw: str, right_raw: str) -> bool:
    return _normalized_query_map(left_raw) == _normalized_query_map(right_raw)


def _normalized_query_map(raw_query: str) -> dict[str, list[str]]:
    parsed = parse_qs(raw_query or "", keep_blank_values=True)
    return {key: sorted(values) for key, values in parsed.items()}


def _query_int(raw_query: str, name: str, *, default: int, minimum: int, maximum: int) -> int:
    parsed = parse_qs(raw_query or "", keep_blank_values=True)
    raw_value = next(iter(parsed.get(name, [])), None)
    if raw_value is None:
        return default
    try:
        value = int(raw_value)
    except ValueError:
        return default
    return max(minimum, min(maximum, value))


def _lane_count(live_state_store: LiveEventStateStore | None, lane: str) -> int:
    if live_state_store is None:
        return 0
    return len(tuple(live_state_store.backend.zrangebyscore(live_state_store._lane_key(lane), float("-inf"), float("inf"))))


def _serialize_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def _serialize_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _serialize_category_row(row: Any) -> dict[str, Any]:
    return {
        "id": int(row["id"]),
        "slug": _serialize_scalar(row["slug"]),
        "name": _serialize_scalar(row["name"]),
        "flag": _serialize_scalar(row["flag"]),
        "alpha2": _serialize_scalar(row["alpha2"]),
        "priority": _serialize_optional_int(row["priority"]),
        "countryAlpha2": _serialize_scalar(row["country_alpha2"]),
        "fieldTranslations": _serialize_scalar(row["field_translations"]),
        "sport": {
            "id": int(row["sport_id"]),
            "slug": _serialize_scalar(row["sport_slug"]),
            "name": _serialize_scalar(row["sport_name"]),
        },
    }


def _extract_sport_slug_from_categories_path(path: str) -> str | None:
    match = re.fullmatch(r"/api/v1/sport/(?P<sport_slug>[^/]+)/categories(?:/all)?", path)
    if match is None:
        return None
    return str(match.group("sport_slug"))


def _extract_category_seed_request(
    path: str,
    path_params: dict[str, str],
) -> tuple[str, str, int] | None:
    match = re.fullmatch(
        r"/api/v1/sport/(?P<sport_slug>[^/]+)/(?P<date>\d{4}-\d{2}-\d{2})/(?P<timezone_offset_seconds>-?\d+)/categories",
        path,
    )
    if match is None:
        return None
    try:
        timezone_offset_seconds = int(path_params.get("timezone_offset_seconds", match.group("timezone_offset_seconds")))
    except ValueError:
        return None
    return (
        str(match.group("sport_slug")),
        str(path_params.get("date", match.group("date"))),
        timezone_offset_seconds,
    )


def _load_optional_redis_backend(redis_url: str | None) -> Any | None:
    env = _load_project_env()
    resolved_url = redis_url or env.get("REDIS_URL") or env.get("SOFASCORE_REDIS_URL")
    if not resolved_url:
        return None
    try:
        import redis
    except ImportError:
        return None
    backend = redis.Redis.from_url(resolved_url, decode_responses=True)
    try:
        backend.ping()
    except Exception:
        return None
    return backend


def _load_project_env() -> dict[str, str]:
    merged = dict(os.environ)
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return merged
    for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        merged.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return merged


if __name__ == "__main__":
    raise SystemExit(main())
