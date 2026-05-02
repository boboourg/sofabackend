"""Serve the ingested multi-sport dataset through local Sofascore-style HTTP routes."""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import dataclasses
import logging
import os
import re
import threading
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import parse_qs, urlsplit

import orjson
from fastapi import FastAPI, Request, Response

from .db import DatabaseConfig, create_pool_with_fallback, load_database_config
from .endpoints import (
    SPORT_ALL_EVENT_COUNT_ENDPOINT,
    TEAM_PLAYER_STATISTICS_SEASONS_ENDPOINT,
    TEAM_TEAM_STATISTICS_SEASONS_ENDPOINT,
    SofascoreEndpoint,
    local_api_endpoints,
)
from .ops.health import collect_health_report
from .ops.queue_summary import collect_queue_summary
from .queue.live_state import LiveEventStateStore
from .queue.streams import ALL_CONSUMER_GROUPS, RedisStreamQueue

from .local_swagger_builder import (
    _build_viewer_html,
    _empty_summary,
    _load_summary,
    build_openapi_document,
    load_cached_openapi_bytes,
    resolve_openapi_base_urls,
    resolve_request_openapi_base_urls,
    write_cached_openapi_bytes,
)

_ALL_ENDPOINTS = local_api_endpoints()
# Mirrors schema_inspector/services/housekeeping.ZOMBIE_TERMINAL_STATUS.
# Housekeeping stamps this sentinel into event_terminal_state for events whose
# live polling went quiet past the zombie cutoff. It is NOT an authoritative
# natural end-state, so the read-layer must not use it to override status or
# drop events that the upstream snapshot still returns.
_ZOMBIE_TERMINAL_STATUS = "zombie_stale"

_NATURAL_TERMINAL_STATUSES: frozenset[str] = frozenset(
    {"finished", "afterextra", "afterpen", "cancelled", "postponed"}
)
_QUEUE_GROUPS = ALL_CONSUMER_GROUPS
_OPENAPI_WARMUP_DELAY_SECONDS = 120.0

logger = logging.getLogger(__name__)

_SAFE_SQL_IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
_SOURCE_TABLES_BY_PATH_TEMPLATE: dict[str, tuple[str, ...]] | None = None
_SEASON_EVENTS_PAGE_SIZE = 30
_SEASON_EVENTS_LAST_TEMPLATE = "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/last/{page}"
_SEASON_EVENTS_NEXT_TEMPLATE = "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/next/{page}"


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


@dataclass(frozen=True)
class SerializedApiResponse:
    status_code: int
    body: bytes
    cache_control: str


@dataclass(frozen=True)
class _CachedSerializedResponse:
    response: SerializedApiResponse
    expires_at: float


class _PooledConnectionLease:
    def __init__(self, pool: Any, connection: Any) -> None:
        self._pool = pool
        self._connection = connection
        self._released = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connection, name)

    async def close(self) -> None:
        if self._released:
            return
        await self._pool.release(self._connection)
        self._released = True


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
    asyncio.run(application.startup())
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
        asyncio.run(application.shutdown())
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
        self._db_pool: Any | None = None
        self._runtime_loop: asyncio.AbstractEventLoop | None = None
        self._runtime_thread: threading.Thread | None = None
        self._runtime_ready = threading.Event()
        self._openapi_build_future: concurrent.futures.Future[bytes] | None = None
        self._openapi_warmup_future: concurrent.futures.Future[None] | None = None
        self._openapi_build_lock = threading.Lock()
        self._response_cache: dict[tuple[str, tuple[tuple[str, tuple[str, ...]], ...]], _CachedSerializedResponse] = {}
        self._response_cache_lock: threading.Lock | None = threading.Lock()
        self._cache_now = time.monotonic
        self.swagger_html = _build_viewer_html("openapi.json")
        self._openapi_json = load_cached_openapi_bytes(base_urls=self.openapi_base_urls)
        self._openapi_json_variants: dict[tuple[str, ...], bytes] = {}
        if self._openapi_json is not None:
            self._openapi_json_variants[self.openapi_base_urls] = self._openapi_json

    async def _build_openapi_document(self) -> dict[str, Any]:
        try:
            summary = await _load_summary(self.database_config.dsn)
        except Exception:
            summary = _empty_summary()
        return build_openapi_document(summary, base_urls=self.openapi_base_urls)

    async def startup(self) -> None:
        if self._db_pool is not None:
            return
        self._ensure_runtime_loop()
        await asyncio.wrap_future(self._submit_to_runtime(self._startup_async()))
        self._schedule_deferred_openapi_warmup()

    async def shutdown(self) -> None:
        if self._runtime_loop is None:
            return
        warmup_future = self._openapi_warmup_future
        self._openapi_warmup_future = None
        if warmup_future is not None:
            warmup_future.cancel()
        await asyncio.wrap_future(self._submit_to_runtime(self._shutdown_async()))
        runtime_loop = self._runtime_loop
        runtime_thread = self._runtime_thread
        self._runtime_loop = None
        self._runtime_thread = None
        self._runtime_ready.clear()
        if runtime_loop is not None:
            runtime_loop.call_soon_threadsafe(runtime_loop.stop)
        if runtime_thread is not None:
            runtime_thread.join(timeout=5)

    def run_async(self, awaitable: Any) -> Any:
        self._ensure_runtime_loop()
        return self._submit_to_runtime(awaitable).result()

    async def run_in_runtime(self, awaitable: Any) -> Any:
        self._ensure_runtime_loop()
        return await asyncio.wrap_future(self._submit_to_runtime(awaitable))

    @property
    def openapi_json(self) -> bytes:
        cached = self._openapi_json
        if cached is not None:
            return cached
        self._ensure_runtime_loop()
        return self._schedule_openapi_build().result()

    def openapi_json_for_request(self, request_headers: dict[str, str] | None = None) -> bytes:
        resolved_base_urls = resolve_request_openapi_base_urls(
            request_headers=request_headers,
            configured_base_urls=self.openapi_base_urls,
        )
        if resolved_base_urls == self.openapi_base_urls:
            return self.openapi_json
        cached_variants = getattr(self, "_openapi_json_variants", None)
        if cached_variants is None:
            cached_variants = {}
            self._openapi_json_variants = cached_variants
        cached = cached_variants.get(resolved_base_urls)
        if cached is not None:
            return cached
        cached = load_cached_openapi_bytes(base_urls=resolved_base_urls)
        if cached is not None:
            cached_variants[resolved_base_urls] = cached
            return cached
        self._ensure_runtime_loop()
        payload = self._submit_to_runtime(self._build_and_cache_openapi_json_variant(resolved_base_urls)).result()
        cached_variants[resolved_base_urls] = payload
        return payload

    async def _startup_async(self) -> None:
        if self._db_pool is not None:
            return
        self._db_pool = await create_pool_with_fallback(self.database_config)

    async def _shutdown_async(self) -> None:
        if self._db_pool is not None:
            await self._db_pool.close()
            self._db_pool = None
        await _close_redis_backend(getattr(self, "redis_backend", None))

    def _ensure_runtime_loop(self) -> None:
        if self._runtime_loop is not None:
            return
        self._runtime_ready.clear()
        runtime_thread = threading.Thread(target=self._run_runtime_loop, name="local-api-runtime", daemon=True)
        runtime_thread.start()
        if not self._runtime_ready.wait(timeout=5):
            raise RuntimeError("Local API runtime loop failed to start.")
        self._runtime_thread = runtime_thread

    def _run_runtime_loop(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._runtime_loop = loop
        self._runtime_ready.set()
        try:
            loop.run_forever()
        finally:
            pending = asyncio.all_tasks(loop)
            for task in pending:
                task.cancel()
            if pending:
                loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
            loop.close()

    def _submit_to_runtime(self, awaitable: Any) -> concurrent.futures.Future[Any]:
        if self._runtime_loop is None:
            raise RuntimeError("Local API runtime loop is not running.")
        return asyncio.run_coroutine_threadsafe(awaitable, self._runtime_loop)

    def _schedule_openapi_build(self) -> concurrent.futures.Future[bytes]:
        with self._openapi_build_lock:
            if self._openapi_json is not None:
                future: concurrent.futures.Future[bytes] = concurrent.futures.Future()
                future.set_result(self._openapi_json)
                return future
            if self._openapi_build_future is None:
                self._openapi_build_future = self._submit_to_runtime(self._build_and_cache_openapi_json())
            return self._openapi_build_future

    async def _build_and_cache_openapi_json(self) -> bytes:
        try:
            document = await self._build_openapi_document()
            payload = write_cached_openapi_bytes(document, base_urls=self.openapi_base_urls)
            self._openapi_json = payload
            self._openapi_json_variants[self.openapi_base_urls] = payload
            return payload
        finally:
            with self._openapi_build_lock:
                self._openapi_build_future = None

    async def _build_and_cache_openapi_json_variant(self, base_urls: tuple[str, ...]) -> bytes:
        try:
            summary = await _load_summary(self.database_config.dsn)
        except Exception:
            summary = _empty_summary()
        document = build_openapi_document(summary, base_urls=base_urls)
        return write_cached_openapi_bytes(document, base_urls=base_urls)

    def _schedule_deferred_openapi_warmup(self) -> None:
        if self._openapi_json is not None:
            return
        future = self._openapi_warmup_future
        if future is not None and not future.done():
            return
        self._openapi_warmup_future = self._submit_to_runtime(self._deferred_openapi_warmup())

    async def _deferred_openapi_warmup(self) -> None:
        try:
            await asyncio.sleep(_OPENAPI_WARMUP_DELAY_SECONDS)
            if self._openapi_json is not None:
                return
            await self._build_and_cache_openapi_json()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Deferred OpenAPI warmup failed.")
        finally:
            self._openapi_warmup_future = None

    async def handle_api_get(self, path: str, raw_query: str) -> ApiResponse:
        route_match = match_route(path, self.routes)
        if route_match is None:
            return ApiResponse(
                status_code=HTTPStatus.NOT_FOUND,
                payload={"error": "Route is not registered in the local multi-sport API.", "path": path},
            )

        route, path_params = route_match
        if route.endpoint.path_template == SPORT_ALL_EVENT_COUNT_ENDPOINT.path_template:
            payload = await self._fetch_sport_event_count_payload()
            return ApiResponse(status_code=HTTPStatus.OK, payload=payload)

        fast_path_handled, payload = await self._fetch_entity_root_fast_path(path, raw_query)
        if not fast_path_handled:
            payload = await self._fetch_snapshot_payload(route, path, raw_query, path_params)
        if payload is None:
            payload = await self._fetch_normalized_payload(route, path, raw_query, path_params)
        if payload is None:
            empty_payload = _empty_payload_for_missing_route(route)
            if empty_payload is not None:
                return ApiResponse(status_code=HTTPStatus.OK, payload=empty_payload)
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

    async def _fetch_entity_root_fast_path(self, path: str, raw_query: str) -> tuple[bool, Any | None]:
        if raw_query:
            return False, None

        live_sport_slug = _extract_sport_slug_from_live_events_path(path)
        if live_sport_slug is not None:
            return True, await self._fetch_sport_live_events_payload(live_sport_slug)

        event_root_id = _extract_event_id_from_entity_root_path(path, "event")
        if event_root_id is not None:
            return True, await self._fetch_event_root_payload(event_root_id)

        team_root_id = _extract_event_id_from_entity_root_path(path, "team")
        if team_root_id is not None:
            return True, await self._fetch_team_root_payload(team_root_id)

        player_root_id = _extract_event_id_from_entity_root_path(path, "player")
        if player_root_id is not None:
            return True, await self._fetch_player_root_payload(player_root_id)

        manager_root_id = _extract_event_id_from_entity_root_path(path, "manager")
        if manager_root_id is not None:
            return True, await self._fetch_manager_root_payload(manager_root_id)

        unique_tournament_root_id = _extract_event_id_from_entity_root_path(path, "unique-tournament")
        if unique_tournament_root_id is not None:
            return True, await self._fetch_unique_tournament_root_payload(unique_tournament_root_id)

        return False, None

    async def handle_api_get_http_response(self, path: str, raw_query: str) -> SerializedApiResponse:
        route_match = match_route(path, self.routes)
        route = route_match[0] if route_match is not None else None
        cache_key = _response_cache_key(path, raw_query)
        cached = self._cache_get(cache_key)
        if cached is not None:
            return cached
        response = await self.handle_api_get(path, raw_query)
        cache_control = _cache_control_for_response(route, response.payload)
        serialized = SerializedApiResponse(
            status_code=response.status_code,
            body=_json_dumps_bytes(response.payload),
            cache_control=cache_control,
        )
        ttl_seconds = _response_ttl_seconds(route, response.payload) if response.status_code == HTTPStatus.OK else 0.0
        if ttl_seconds > 0:
            self._cache_put(cache_key, serialized, ttl_seconds)
        return serialized

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
        if path == "/ops/coverage/summary":
            return ApiResponse(status_code=HTTPStatus.OK, payload=await self._fetch_ops_coverage_summary_payload())
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
        context_value = _parse_context_value(route, path_params)
        query = "SELECT source_url, payload FROM api_payload_snapshot WHERE endpoint_pattern = $1"
        arguments: list[Any] = [route.endpoint.pattern]
        source_slug = getattr(route.endpoint, "source_slug", None)
        if source_slug:
            query += f" AND source_slug = ${len(arguments) + 1}"
            arguments.append(source_slug)
        if route.context_entity_type is not None and context_value is not None:
            query += f" AND context_entity_type = ${len(arguments) + 1} AND context_entity_id = ${len(arguments) + 2}"
            arguments.extend([route.context_entity_type, context_value])
        query += " ORDER BY id DESC LIMIT 500"

        connection = await self._connect()
        try:
            rows = await connection.fetch(query, *arguments)
            latest_path_match: Any | None = None
            for row in rows:
                split = urlsplit(str(row["source_url"]))
                if split.path != path:
                    continue
                decoded_payload = _decode_snapshot_payload(row["payload"])
                if _query_maps_equal(split.query, raw_query):
                    return await self._reconcile_snapshot_payload(connection, route, decoded_payload)
                if not raw_query and latest_path_match is None:
                    latest_path_match = decoded_payload

            if route.endpoint.query_template and not raw_query and latest_path_match is not None:
                return await self._reconcile_snapshot_payload(connection, route, latest_path_match)
        finally:
            await connection.close()
        return None

    async def _reconcile_snapshot_payload(
        self,
        executor: Any,
        route: RouteSpec,
        payload: Any,
    ) -> Any:
        if not isinstance(payload, dict):
            return payload

        envelope_key = getattr(route.endpoint, "envelope_key", None)
        if envelope_key not in {"events", "featuredEvents"}:
            return payload

        raw_items = payload.get(envelope_key)
        if not isinstance(raw_items, list):
            return payload

        event_ids = [int(item["id"]) for item in raw_items if isinstance(item, dict) and item.get("id") is not None]
        event_custom_ids = sorted(
            {
                str(item.get("customId"))
                for item in raw_items
                if isinstance(item, dict) and item.get("customId") is not None
            }
        )
        if not event_ids and not event_custom_ids:
            return payload

        surface_rows = await executor.fetch(
            """
            SELECT
                e.id AS event_id,
                e.custom_id,
                e.start_timestamp,
                e.home_team_id,
                e.away_team_id,
                e.status_code,
                es.type AS status_type,
                es.description AS status_description,
                e.winner_code,
                e.aggregated_winner_code,
                e.coverage,
                e.home_red_cards,
                e.away_red_cards,
                e.detail_id,
                e.correct_ai_insight,
                e.correct_halftime_ai_insight,
                e.feed_locked,
                e.is_editor,
                e.crowdsourcing_data_display_enabled,
                e.final_result_only,
                e.has_event_player_statistics,
                e.has_event_player_heat_map,
                e.has_global_highlights,
                e.has_xg,
                score_payload.scores,
                round_payload.round_info_payload,
                status_time_payload.status_time_payload,
                time_payload.time_payload,
                changes_payload.changes_payload,
                filters_payload.event_filters_payload
            FROM event AS e
            LEFT JOIN event_status AS es ON es.code = e.status_code
            LEFT JOIN LATERAL (
                SELECT jsonb_object_agg(
                    sc.side,
                    jsonb_strip_nulls(jsonb_build_object(
                        'current', sc.current,
                        'display', sc.display,
                        'aggregated', sc.aggregated,
                        'normaltime', sc.normaltime,
                        'overtime', sc.overtime,
                        'penalties', sc.penalties,
                        'period1', sc.period1,
                        'period2', sc.period2,
                        'period3', sc.period3,
                        'period4', sc.period4,
                        'extra1', sc.extra1,
                        'extra2', sc.extra2,
                        'series', sc.series
                    ))
                    ORDER BY sc.side
                ) AS scores
                FROM event_score AS sc
                WHERE sc.event_id = e.id
                  AND sc.side IN ('home', 'away')
            ) AS score_payload ON TRUE
            LEFT JOIN LATERAL (
                SELECT jsonb_strip_nulls(jsonb_build_object(
                    'round', eri.round_number,
                    'slug', eri.slug,
                    'name', eri.name,
                    'cupRoundType', eri.cup_round_type
                )) AS round_info_payload
                FROM event_round_info AS eri
                WHERE eri.event_id = e.id
                LIMIT 1
            ) AS round_payload ON TRUE
            LEFT JOIN LATERAL (
                SELECT jsonb_strip_nulls(jsonb_build_object(
                    'prefix', est.prefix,
                    'timestamp', est.timestamp,
                    'initial', est.initial,
                    'max', est.max,
                    'extra', est.extra
                )) AS status_time_payload
                FROM event_status_time AS est
                WHERE est.event_id = e.id
                LIMIT 1
            ) AS status_time_payload ON TRUE
            LEFT JOIN LATERAL (
                SELECT jsonb_strip_nulls(jsonb_build_object(
                    'currentPeriodStartTimestamp', et.current_period_start_timestamp,
                    'initial', et.initial,
                    'max', et.max,
                    'extra', et.extra,
                    'injuryTime1', et.injury_time1,
                    'injuryTime2', et.injury_time2,
                    'injuryTime3', et.injury_time3,
                    'injuryTime4', et.injury_time4,
                    'overtimeLength', et.overtime_length,
                    'periodLength', et.period_length,
                    'totalPeriodCount', et.total_period_count
                )) AS time_payload
                FROM event_time AS et
                WHERE et.event_id = e.id
                LIMIT 1
            ) AS time_payload ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    CASE
                        WHEN count(*) = 0 THEN NULL
                        ELSE jsonb_strip_nulls(jsonb_build_object(
                            'changes', jsonb_agg(eci.change_value ORDER BY eci.ordinal),
                            'changeTimestamp', max(eci.change_timestamp)
                        ))
                    END AS changes_payload
                FROM event_change_item AS eci
                WHERE eci.event_id = e.id
            ) AS changes_payload ON TRUE
            LEFT JOIN LATERAL (
                SELECT jsonb_object_agg(grouped.filter_name, grouped.filter_values ORDER BY grouped.filter_name) AS event_filters_payload
                FROM (
                    SELECT
                        efv.filter_name,
                        jsonb_agg(efv.filter_value ORDER BY efv.ordinal) AS filter_values
                    FROM event_filter_value AS efv
                    WHERE efv.event_id = e.id
                    GROUP BY efv.filter_name
                ) AS grouped
            ) AS filters_payload ON TRUE
            WHERE e.id = ANY($1::bigint[])
               OR (cardinality($2::text[]) > 0 AND e.custom_id = ANY($2::text[]))
            """,
            event_ids,
            event_custom_ids,
        )
        surface_by_event: dict[int, dict[str, Any]] = {}
        surface_by_custom_id: dict[str, dict[str, Any]] = {}
        for row in surface_rows:
            row_dict = dict(row)
            event_id = int(row_dict["event_id"])
            surface_by_event[event_id] = row_dict
            custom_id = row_dict.get("custom_id")
            if custom_id is not None:
                surface_by_custom_id[str(custom_id)] = row_dict

        terminal_rows = await executor.fetch(
            """
            SELECT
                ets.event_id,
                e.custom_id,
                e.start_timestamp,
                e.home_team_id,
                e.away_team_id,
                ets.terminal_status,
                ets.finalized_at,
                aps.payload AS final_payload
            FROM event_terminal_state AS ets
            JOIN event AS e
                ON e.id = ets.event_id
            LEFT JOIN api_payload_snapshot AS aps
                ON aps.id = ets.final_snapshot_id
            WHERE ets.event_id = ANY($1::bigint[])
               OR (cardinality($2::text[]) > 0 AND e.custom_id = ANY($2::text[]))
            """,
            event_ids,
            event_custom_ids,
        )

        terminal_by_event: dict[int, dict[str, Any]] = {}
        terminal_by_custom_id: dict[str, dict[str, Any]] = {}
        for row in terminal_rows:
            event_id = int(row["event_id"])
            terminal_status_raw = row.get("terminal_status")
            # Housekeeping's zombie-sweeper stamps ``zombie_stale`` for events
            # whose live-state polling went silent past the configured cutoff.
            # That is NOT an authoritative natural end — if upstream later
            # returns the event in a list snapshot, trust the snapshot rather
            # than overriding its status from a synthetic sentinel.
            if str(terminal_status_raw or "").strip().lower() == _ZOMBIE_TERMINAL_STATUS:
                continue
            status_payload = _extract_terminal_status_payload(row.get("final_payload"))
            if status_payload is None:
                status_payload = _fallback_terminal_status_payload(
                    terminal_status_raw,
                    _extract_existing_event_status(raw_items, event_id),
                )
            if status_payload is not None:
                candidate = {
                    "status": status_payload,
                    "finalized_at": row.get("finalized_at"),
                    "row": dict(row),
                }
                existing_by_event = terminal_by_event.get(event_id)
                if existing_by_event is None or _terminal_candidate_sort_key(candidate) > _terminal_candidate_sort_key(existing_by_event):
                    terminal_by_event[event_id] = candidate
                custom_id = row.get("custom_id")
                if custom_id is not None:
                    existing_by_custom_id = terminal_by_custom_id.get(str(custom_id))
                    if existing_by_custom_id is None or _terminal_candidate_sort_key(candidate) > _terminal_candidate_sort_key(existing_by_custom_id):
                        terminal_by_custom_id[str(custom_id)] = candidate

        if not surface_by_event and not surface_by_custom_id and not terminal_by_event and not terminal_by_custom_id:
            return payload

        # Previously this branch unconditionally dropped every event that had
        # *any* ``event_terminal_state`` row from the live list. That caused
        # near-100% under-counting whenever the upstream feed's grace window
        # briefly kept finished games in ``/events/live``, or when the
        # zombie-sweeper had stamped many events as terminal. The live list's
        # source of truth is the upstream snapshot, not ``event_terminal_state``:
        # events disappear naturally when the next snapshot stops returning
        # them. We now mirror the scheduled-list behaviour here — keep the
        # event, override its status with the freshest authoritative terminal
        # status so stale "inprogress" labels get corrected in-place.
        reconciled_items: list[Any] = []
        for item in raw_items:
            if not isinstance(item, dict):
                reconciled_items.append(item)
                continue
            event_id = item.get("id")
            if event_id is None:
                reconciled_items.append(item)
                continue
            event_id = int(event_id)
            updated = dict(item)
            surface_row = surface_by_event.get(event_id)
            if surface_row is None:
                custom_id = item.get("customId")
                if custom_id is not None:
                    by_custom_id_surface = surface_by_custom_id.get(str(custom_id))
                    if by_custom_id_surface is not None and _event_item_identity_matches_terminal_row(item, by_custom_id_surface):
                        surface_row = by_custom_id_surface
            if surface_row is not None:
                updated = _apply_event_surface_overlay(updated, surface_row)
            terminal_candidate = terminal_by_event.get(event_id)
            if terminal_candidate is None:
                custom_id = item.get("customId")
                if custom_id is not None:
                    by_custom_id = terminal_by_custom_id.get(str(custom_id))
                    if by_custom_id is not None and _event_item_identity_matches_terminal_row(item, by_custom_id["row"]):
                        terminal_candidate = by_custom_id
            if terminal_candidate is None:
                reconciled_items.append(updated)
                continue
            updated["status"] = terminal_candidate["status"]
            reconciled_items.append(updated)

        reconciled_payload = dict(payload)
        reconciled_payload[envelope_key] = reconciled_items
        return reconciled_payload

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

        if route.endpoint.path_template == TEAM_TEAM_STATISTICS_SEASONS_ENDPOINT.path_template:
            return await self._fetch_team_statistics_seasons_payload(int(path_params["team_id"]))

        if route.endpoint.path_template == TEAM_PLAYER_STATISTICS_SEASONS_ENDPOINT.path_template:
            return await self._fetch_team_player_statistics_seasons_payload(int(path_params["team_id"]))

        # Entity-root routes used to return a false 404 whenever the specific
        # raw snapshot for ``/api/v1/event/{id}``, ``/api/v1/team/{id}``,
        # ``/api/v1/player/{id}``, etc. was absent — even when child snapshots
        # (statistics / lineups / incidents / ...), a finalized end-state
        # snapshot, or the corresponding normalized row WAS ingested. For
        # zombie-resolved events this was the default. The fallback below
        # returns the authoritative final snapshot first, then a minimal
        # envelope synthesised from the normalized table so the route answers
        # 200 instead of 404 whenever ingested data exists.
        event_root_id = _extract_event_id_from_entity_root_path(path, "event")
        if event_root_id is not None:
            return await self._fetch_event_root_payload(event_root_id)

        team_root_id = _extract_event_id_from_entity_root_path(path, "team")
        if team_root_id is not None:
            return await self._fetch_team_root_payload(team_root_id)

        player_root_id = _extract_event_id_from_entity_root_path(path, "player")
        if player_root_id is not None:
            return await self._fetch_player_root_payload(player_root_id)

        manager_root_id = _extract_event_id_from_entity_root_path(path, "manager")
        if manager_root_id is not None:
            return await self._fetch_manager_root_payload(manager_root_id)

        unique_tournament_root_id = _extract_event_id_from_entity_root_path(
            path, "unique-tournament"
        )
        if unique_tournament_root_id is not None:
            return await self._fetch_unique_tournament_root_payload(unique_tournament_root_id)

        specialized_payload = await self._fetch_specialized_normalized_payload(route, path, path_params)
        if specialized_payload is not None:
            return specialized_payload

        generic_payload = await self._fetch_generic_normalized_payload(route, path_params)
        if generic_payload is not None:
            return generic_payload

        return None

    async def _fetch_specialized_normalized_payload(
        self,
        route: RouteSpec,
        path: str,
        path_params: dict[str, str],
    ) -> Any | None:
        del path
        template = route.endpoint.path_template
        if template == "/api/v1/event/{event_id}/statistics":
            return await self._fetch_event_statistics_payload(int(path_params["event_id"]))
        if template == "/api/v1/event/{event_id}/incidents":
            return await self._fetch_event_incidents_payload(int(path_params["event_id"]))
        if template == "/api/v1/event/{event_id}/lineups":
            return await self._fetch_event_lineups_payload(int(path_params["event_id"]))
        if template == "/api/v1/event/{event_id}/player/{player_id}/statistics":
            return await self._fetch_event_player_statistics_payload(
                int(path_params["event_id"]),
                int(path_params["player_id"]),
            )
        if template == "/api/v1/event/{event_id}/player/{player_id}/rating-breakdown":
            return await self._fetch_event_player_rating_breakdown_payload(
                int(path_params["event_id"]),
                int(path_params["player_id"]),
            )
        if template == "/api/v1/event/{event_id}/heatmap/{team_id}":
            return await self._fetch_event_team_heatmap_payload(
                int(path_params["event_id"]),
                int(path_params["team_id"]),
            )
        if template in {_SEASON_EVENTS_LAST_TEMPLATE, _SEASON_EVENTS_NEXT_TEMPLATE}:
            return await self._fetch_season_events_payload(
                unique_tournament_id=int(path_params["unique_tournament_id"]),
                season_id=int(path_params["season_id"]),
                page=max(int(path_params["page"]), 0),
                direction="last" if template == _SEASON_EVENTS_LAST_TEMPLATE else "next",
            )
        if route.endpoint.target_table == "top_player_snapshot":
            return await self._fetch_top_player_payload(route, path_params)
        if route.endpoint.target_table == "top_team_snapshot":
            return await self._fetch_top_team_payload(route, path_params)
        return None

    async def _fetch_season_events_payload(
        self,
        *,
        unique_tournament_id: int,
        season_id: int,
        page: int,
        direction: str,
    ) -> dict[str, Any]:
        offset = page * _SEASON_EVENTS_PAGE_SIZE
        limit = _SEASON_EVENTS_PAGE_SIZE + 1
        if direction == "last":
            status_filter = """
                  AND es.type IN ('finished', 'afterextra', 'afterpen', 'cancelled', 'postponed')
                  AND e.start_timestamp <= EXTRACT(EPOCH FROM NOW())::bigint
            """
            order_by = "e.start_timestamp DESC NULLS LAST, e.id DESC"
        else:
            status_filter = """
                  AND es.type = 'notstarted'
                  AND e.start_timestamp >= EXTRACT(EPOCH FROM NOW())::bigint
            """
            order_by = "e.start_timestamp ASC NULLS LAST, e.id ASC"

        connection = await self._connect()
        try:
            rows = await connection.fetch(
                f"""
                SELECT
                    e.id,
                    e.slug,
                    e.custom_id,
                    e.start_timestamp,
                    e.status_code,
                    e.winner_code,
                    e.aggregated_winner_code,
                    e.coverage,
                    e.home_red_cards,
                    e.away_red_cards,
                    es.type AS status_type,
                    es.description AS status_description,
                    e.home_team_id,
                    ht.slug AS home_team_slug,
                    ht.name AS home_team_name,
                    ht.short_name AS home_team_short_name,
                    e.away_team_id,
                    at.slug AS away_team_slug,
                    at.name AS away_team_name,
                    at.short_name AS away_team_short_name,
                    e.tournament_id,
                    t.slug AS tournament_slug,
                    t.name AS tournament_name,
                    e.unique_tournament_id,
                    ut.slug AS unique_tournament_slug,
                    ut.name AS unique_tournament_name,
                    e.season_id,
                    s.name AS season_name,
                    s.year AS season_year
                FROM event AS e
                LEFT JOIN event_status AS es ON es.code = e.status_code
                LEFT JOIN team AS ht ON ht.id = e.home_team_id
                LEFT JOIN team AS at ON at.id = e.away_team_id
                LEFT JOIN tournament AS t ON t.id = e.tournament_id
                LEFT JOIN unique_tournament AS ut ON ut.id = e.unique_tournament_id
                LEFT JOIN season AS s ON s.id = e.season_id
                WHERE e.unique_tournament_id = $1
                  AND e.season_id = $2
                  {status_filter}
                ORDER BY {order_by}
                OFFSET $3
                LIMIT $4
                """,
                unique_tournament_id,
                season_id,
                offset,
                limit,
            )
        finally:
            await connection.close()

        has_next_page = len(rows) > _SEASON_EVENTS_PAGE_SIZE
        return {
            "events": [
                _serialize_season_event_row(row)
                for row in rows[:_SEASON_EVENTS_PAGE_SIZE]
            ],
            "hasNextPage": has_next_page,
        }

    async def _fetch_event_statistics_payload(self, event_id: int) -> dict[str, Any] | None:
        connection = await self._connect()
        try:
            rows = await connection.fetch(
                """
                SELECT period, group_name, stat_name,
                       home_value_numeric, home_value_text, home_value_json,
                       away_value_numeric, away_value_text, away_value_json,
                       compare_code, statistics_type
                FROM event_statistic
                WHERE event_id = $1
                ORDER BY period, group_name, stat_name
                """,
                event_id,
            )
        finally:
            await connection.close()
        if not rows:
            return None

        periods: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for r in rows:
            period = str(r["period"])
            group = str(r["group_name"])
            home = _first_not_none(
                _serialize_scalar(r["home_value_json"]),
                _serialize_scalar(r["home_value_numeric"]),
                _serialize_scalar(r["home_value_text"]),
            )
            away = _first_not_none(
                _serialize_scalar(r["away_value_json"]),
                _serialize_scalar(r["away_value_numeric"]),
                _serialize_scalar(r["away_value_text"]),
            )
            item: dict[str, Any] = {"name": str(r["stat_name"])}
            if home is not None:
                item["home"] = home
            if away is not None:
                item["away"] = away
            if r["compare_code"] is not None:
                item["compareCode"] = str(r["compare_code"])
            if r["statistics_type"] is not None:
                item["statisticsType"] = str(r["statistics_type"])
            periods.setdefault(period, {}).setdefault(group, []).append(item)

        statistics_list = [
            {
                "period": period,
                "groups": [
                    {"groupName": group_name, "statisticsItems": items}
                    for group_name, items in groups.items()
                ],
            }
            for period, groups in periods.items()
        ]
        return {"statistics": statistics_list}

    async def _fetch_event_incidents_payload(self, event_id: int) -> dict[str, Any] | None:
        connection = await self._connect()
        try:
            rows = await connection.fetch(
                """
                SELECT ordinal, incident_id, incident_type, minute,
                       home_score_text, away_score_text, text_value
                FROM event_incident
                WHERE event_id = $1
                ORDER BY ordinal
                """,
                event_id,
            )
        finally:
            await connection.close()
        if not rows:
            return None

        incidents: list[dict[str, Any]] = []
        for r in rows:
            item: dict[str, Any] = {}
            if r["incident_id"] is not None:
                item["id"] = int(r["incident_id"])
            if r["incident_type"] is not None:
                item["incidentType"] = str(r["incident_type"])
            if r["minute"] is not None:
                item["time"] = int(r["minute"])
            if r["home_score_text"] is not None:
                item["homeScore"] = str(r["home_score_text"])
            if r["away_score_text"] is not None:
                item["awayScore"] = str(r["away_score_text"])
            if r["text_value"] is not None:
                item["text"] = str(r["text_value"])
            incidents.append(item)
        return {"incidents": incidents}

    async def _fetch_event_lineups_payload(self, event_id: int) -> dict[str, Any] | None:
        connection = await self._connect()
        try:
            lineup_rows = await connection.fetch(
                """
                SELECT side, formation, player_color, goalkeeper_color, support_staff
                FROM event_lineup
                WHERE event_id = $1
                """,
                event_id,
            )
            player_rows = await connection.fetch(
                """
                SELECT elp.side, elp.player_id, elp.team_id,
                       elp.position, elp.substitute, elp.shirt_number,
                       elp.jersey_number, elp.avg_rating,
                       p.slug AS player_slug, p.name AS player_name,
                       p.short_name AS player_short_name
                FROM event_lineup_player AS elp
                LEFT JOIN player AS p ON p.id = elp.player_id
                WHERE elp.event_id = $1
                ORDER BY elp.side,
                         elp.substitute NULLS LAST,
                         elp.shirt_number NULLS LAST,
                         elp.player_id
                """,
                event_id,
            )
        finally:
            await connection.close()
        if not lineup_rows and not player_rows:
            return None

        sides: dict[str, dict[str, Any]] = {}
        for r in lineup_rows:
            side = str(r["side"])
            block: dict[str, Any] = {"players": []}
            if r["formation"] is not None:
                block["formation"] = str(r["formation"])
            if r["player_color"] is not None:
                block["playerColor"] = _serialize_scalar(r["player_color"])
            if r["goalkeeper_color"] is not None:
                block["goalkeeperColor"] = _serialize_scalar(r["goalkeeper_color"])
            if r["support_staff"] is not None:
                block["supportStaff"] = _serialize_scalar(r["support_staff"])
            sides[side] = block

        for r in player_rows:
            side = str(r["side"])
            entry: dict[str, Any] = {
                "player": _minimal_entity_payload(
                    r["player_id"],
                    slug=r["player_slug"],
                    name=r["player_name"],
                    short_name=r["player_short_name"],
                ),
            }
            if r["team_id"] is not None:
                entry["teamId"] = int(r["team_id"])
            if r["shirt_number"] is not None:
                entry["shirtNumber"] = int(r["shirt_number"])
            if r["jersey_number"] is not None:
                entry["jerseyNumber"] = str(r["jersey_number"])
            if r["position"] is not None:
                entry["position"] = str(r["position"])
            if r["substitute"] is not None:
                entry["substitute"] = bool(r["substitute"])
            if r["avg_rating"] is not None:
                entry["avgRating"] = _serialize_scalar(r["avg_rating"])
            sides.setdefault(side, {"players": []}).setdefault("players", []).append(entry)

        if not sides:
            return None
        payload: dict[str, Any] = {}
        if "home" in sides:
            payload["home"] = sides["home"]
        if "away" in sides:
            payload["away"] = sides["away"]
        return payload

    async def _fetch_sport_live_events_payload(self, sport_slug: str) -> dict[str, Any]:
        # Sofascore's /events/live returns events that are ACTUALLY in flight
        # right now. Our DB carries a long tail of stale 'interrupted' /
        # 'inprogress' rows from 2019+ that were never finalised. To match
        # upstream semantics:
        #   * limit to events whose start_timestamp is within the last 12h,
        #   * skip events that already have an event_terminal_state row,
        #   * order by start_timestamp DESC so the freshest match comes first.
        active_statuses = (
            "inprogress",
            "live",
            "overtime",
            "extra",
            "awaitingextra",
            "awaitingpenalties",
            "penalties",
            "interrupted",
            "halftime",
            "paused",
            "pause",
            "break",
        )
        connection = await self._connect()
        try:
            rows = await connection.fetch(
                """
                SELECT
                    e.id, e.slug, e.custom_id, e.start_timestamp,
                    e.status_code, e.winner_code, e.aggregated_winner_code,
                    e.coverage, e.home_red_cards, e.away_red_cards,
                    es.type AS status_type,
                    es.description AS status_description,
                    e.home_team_id,
                    ht.slug AS home_team_slug, ht.name AS home_team_name,
                    ht.short_name AS home_team_short_name,
                    e.away_team_id,
                    at.slug AS away_team_slug, at.name AS away_team_name,
                    at.short_name AS away_team_short_name,
                    e.tournament_id,
                    t.slug AS tournament_slug, t.name AS tournament_name,
                    e.unique_tournament_id,
                    ut.slug AS unique_tournament_slug, ut.name AS unique_tournament_name,
                    e.season_id,
                    s.name AS season_name, s.year AS season_year
                FROM event AS e
                JOIN event_status AS es ON es.code = e.status_code
                LEFT JOIN team AS ht ON ht.id = e.home_team_id
                LEFT JOIN team AS at ON at.id = e.away_team_id
                LEFT JOIN tournament AS t ON t.id = e.tournament_id
                LEFT JOIN unique_tournament AS ut ON ut.id = e.unique_tournament_id
                LEFT JOIN season AS s ON s.id = e.season_id
                JOIN category AS cat ON cat.id = t.category_id
                JOIN sport AS sp ON sp.id = cat.sport_id
                WHERE sp.slug = $1
                  AND es.type = ANY($2::text[])
                  AND e.start_timestamp >= EXTRACT(EPOCH FROM NOW() - INTERVAL '12 hours')::bigint
                  AND NOT EXISTS (
                      SELECT 1 FROM event_terminal_state ets WHERE ets.event_id = e.id
                  )
                ORDER BY e.start_timestamp DESC NULLS LAST, e.id DESC
                LIMIT 500
                """,
                sport_slug,
                list(active_statuses),
            )
        finally:
            await connection.close()
        return {"events": [_serialize_season_event_row(row) for row in rows]}

    async def _fetch_event_player_statistics_payload(
        self,
        event_id: int,
        player_id: int,
    ) -> dict[str, Any] | None:
        connection = await self._connect()
        try:
            summary = await connection.fetchrow(
                """
                SELECT
                    eps.event_id,
                    eps.player_id,
                    eps.team_id,
                    eps.position,
                    eps.rating,
                    eps.rating_original,
                    eps.rating_alternative,
                    eps.statistics_type,
                    eps.sport_slug,
                    eps.extra_json,
                    p.slug AS player_slug,
                    p.name AS player_name,
                    p.short_name AS player_short_name,
                    t.slug AS team_slug,
                    t.name AS team_name,
                    t.short_name AS team_short_name
                FROM event_player_statistics AS eps
                LEFT JOIN player AS p ON p.id = eps.player_id
                LEFT JOIN team AS t ON t.id = eps.team_id
                WHERE eps.event_id = $1
                  AND eps.player_id = $2
                """,
                event_id,
                player_id,
            )
            if summary is None:
                return None
            value_rows = await connection.fetch(
                """
                SELECT stat_name, stat_value_numeric, stat_value_text, stat_value_json
                FROM event_player_stat_value
                WHERE event_id = $1
                  AND player_id = $2
                ORDER BY stat_name
                """,
                event_id,
                player_id,
            )
        finally:
            await connection.close()

        statistics: dict[str, Any] = {}
        if summary["rating"] is not None:
            statistics["rating"] = _serialize_scalar(summary["rating"])
        rating_versions: dict[str, Any] = {}
        if summary["rating_original"] is not None:
            rating_versions["original"] = _serialize_scalar(summary["rating_original"])
        if summary["rating_alternative"] is not None:
            rating_versions["alternative"] = _serialize_scalar(summary["rating_alternative"])
        if rating_versions:
            statistics["ratingVersions"] = rating_versions
        statistics_type: dict[str, Any] = {}
        if summary["statistics_type"] is not None:
            statistics_type["statisticsType"] = str(summary["statistics_type"])
        if summary["sport_slug"] is not None:
            statistics_type["sportSlug"] = str(summary["sport_slug"])
        if statistics_type:
            statistics["statisticsType"] = statistics_type
        for row in value_rows:
            statistics[str(row["stat_name"])] = _first_not_none(
                _serialize_scalar(row["stat_value_json"]),
                _serialize_scalar(row["stat_value_numeric"]),
                _serialize_scalar(row["stat_value_text"]),
            )

        payload: dict[str, Any] = {
            "player": _minimal_entity_payload(
                summary["player_id"],
                slug=summary["player_slug"],
                name=summary["player_name"],
                short_name=summary["player_short_name"],
            ),
            "statistics": statistics,
        }
        if summary["team_id"] is not None:
            payload["team"] = _minimal_entity_payload(
                summary["team_id"],
                slug=summary["team_slug"],
                name=summary["team_name"],
                short_name=summary["team_short_name"],
            )
        if summary["position"] is not None:
            payload["position"] = str(summary["position"])
        if summary["extra_json"] is not None:
            payload["extra"] = _serialize_scalar(summary["extra_json"])
        return payload

    async def _fetch_event_player_rating_breakdown_payload(
        self,
        event_id: int,
        player_id: int,
    ) -> dict[str, list[dict[str, Any]]] | None:
        connection = await self._connect()
        try:
            rows = await connection.fetch(
                """
                SELECT
                    action_group,
                    ordinal,
                    event_action_type,
                    is_home,
                    keypass,
                    outcome,
                    start_x,
                    start_y,
                    end_x,
                    end_y
                FROM event_player_rating_breakdown_action
                WHERE event_id = $1
                  AND player_id = $2
                ORDER BY action_group, ordinal
                """,
                event_id,
                player_id,
            )
        finally:
            await connection.close()
        if not rows:
            return None

        payload: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            item: dict[str, Any] = {}
            if row["event_action_type"] is not None:
                item["eventActionType"] = str(row["event_action_type"])
            if row["is_home"] is not None:
                item["isHome"] = bool(row["is_home"])
            if row["keypass"] is not None:
                item["keypass"] = bool(row["keypass"])
            if row["outcome"] is not None:
                item["outcome"] = bool(row["outcome"])
            if row["start_x"] is not None or row["start_y"] is not None:
                item["playerCoordinates"] = {
                    "x": _serialize_scalar(row["start_x"]),
                    "y": _serialize_scalar(row["start_y"]),
                }
            if row["end_x"] is not None or row["end_y"] is not None:
                item["passEndCoordinates"] = {
                    "x": _serialize_scalar(row["end_x"]),
                    "y": _serialize_scalar(row["end_y"]),
                }
            payload.setdefault(str(row["action_group"]), []).append(item)
        return payload

    async def _fetch_event_team_heatmap_payload(
        self,
        event_id: int,
        team_id: int,
    ) -> dict[str, list[dict[str, Any]]] | None:
        connection = await self._connect()
        try:
            rows = await connection.fetch(
                """
                SELECT point_type, ordinal, x, y
                FROM event_team_heatmap_point
                WHERE event_id = $1
                  AND team_id = $2
                ORDER BY point_type, ordinal
                """,
                event_id,
                team_id,
            )
        finally:
            await connection.close()
        if not rows:
            return None

        payload = {"playerPoints": [], "goalkeeperPoints": []}
        for row in rows:
            point = {"x": _serialize_scalar(row["x"]), "y": _serialize_scalar(row["y"])}
            if row["point_type"] == "goalkeeper":
                payload["goalkeeperPoints"].append(point)
            else:
                payload["playerPoints"].append(point)
        return payload

    async def _fetch_top_player_payload(
        self,
        route: RouteSpec,
        path_params: dict[str, str],
    ) -> dict[str, Any] | None:
        connection = await self._connect()
        try:
            snapshot = await connection.fetchrow(
                _top_snapshot_query("top_player_snapshot", path_params),
                route.endpoint.pattern,
                int(path_params["unique_tournament_id"]),
                int(path_params["season_id"]),
                _optional_int_path_param(path_params, "team_id"),
            )
            if snapshot is None:
                return None
            rows = await connection.fetch(
                """
                SELECT
                    e.metric_name,
                    e.ordinal,
                    e.player_id,
                    p.slug AS player_slug,
                    p.name AS player_name,
                    p.short_name AS player_short_name,
                    e.team_id,
                    t.slug AS team_slug,
                    t.name AS team_name,
                    t.short_name AS team_short_name,
                    e.event_id,
                    e.played_enough,
                    e.statistic,
                    e.statistics_id,
                    e.statistics_payload
                FROM top_player_entry AS e
                LEFT JOIN player AS p ON p.id = e.player_id
                LEFT JOIN team AS t ON t.id = e.team_id
                WHERE e.snapshot_id = $1
                ORDER BY e.metric_name, e.ordinal
                """,
                int(snapshot["id"]),
            )
        finally:
            await connection.close()
        if not rows:
            return None

        top_players: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            item: dict[str, Any] = {}
            if row["player_id"] is not None:
                item["player"] = _minimal_entity_payload(
                    row["player_id"],
                    slug=row["player_slug"],
                    name=row["player_name"],
                    short_name=row["player_short_name"],
                )
            if row["team_id"] is not None:
                item["team"] = _minimal_entity_payload(
                    row["team_id"],
                    slug=row["team_slug"],
                    name=row["team_name"],
                    short_name=row["team_short_name"],
                )
            if row["event_id"] is not None:
                item["event"] = {"id": int(row["event_id"])}
            if row["played_enough"] is not None:
                item["playedEnough"] = bool(row["played_enough"])
            if row["statistic"] is not None:
                item["statistic"] = _serialize_scalar(row["statistic"])
            if row["statistics_id"] is not None:
                item["statisticsId"] = int(row["statistics_id"])
            if row["statistics_payload"] is not None:
                item["statistics"] = _serialize_scalar(row["statistics_payload"])
            top_players.setdefault(str(row["metric_name"]), []).append(item)

        payload: dict[str, Any] = {"topPlayers": top_players}
        if snapshot["statistics_type"] is not None:
            payload["statisticsType"] = _serialize_scalar(snapshot["statistics_type"])
        return payload

    async def _fetch_top_team_payload(
        self,
        route: RouteSpec,
        path_params: dict[str, str],
    ) -> dict[str, Any] | None:
        connection = await self._connect()
        try:
            snapshot = await connection.fetchrow(
                _top_snapshot_query("top_team_snapshot", path_params),
                route.endpoint.pattern,
                int(path_params["unique_tournament_id"]),
                int(path_params["season_id"]),
                None,
            )
            if snapshot is None:
                return None
            rows = await connection.fetch(
                """
                SELECT
                    e.metric_name,
                    e.ordinal,
                    e.team_id,
                    t.slug AS team_slug,
                    t.name AS team_name,
                    t.short_name AS team_short_name,
                    e.statistics_id,
                    e.statistics_payload
                FROM top_team_entry AS e
                LEFT JOIN team AS t ON t.id = e.team_id
                WHERE e.snapshot_id = $1
                ORDER BY e.metric_name, e.ordinal
                """,
                int(snapshot["id"]),
            )
        finally:
            await connection.close()
        if not rows:
            return None

        top_teams: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            item: dict[str, Any] = {
                "team": _minimal_entity_payload(
                    row["team_id"],
                    slug=row["team_slug"],
                    name=row["team_name"],
                    short_name=row["team_short_name"],
                )
            }
            if row["statistics_id"] is not None:
                item["statisticsId"] = int(row["statistics_id"])
            if row["statistics_payload"] is not None:
                item["statistics"] = _serialize_scalar(row["statistics_payload"])
            top_teams.setdefault(str(row["metric_name"]), []).append(item)
        return {"topTeams": top_teams}

    async def _fetch_generic_normalized_payload(
        self,
        route: RouteSpec,
        path_params: dict[str, str],
    ) -> dict[str, Any] | None:
        candidate_tables = _generic_fallback_candidate_tables(route)
        if not candidate_tables:
            return None

        connection = await self._connect()
        try:
            for table_name in candidate_tables:
                columns = await _fetch_table_columns(connection, table_name)
                if not columns:
                    continue
                filters = _generic_filters_for_columns(path_params, columns)
                if "endpoint_pattern" in columns:
                    filters["endpoint_pattern"] = route.endpoint.pattern
                if not filters:
                    continue
                rows = await _fetch_generic_rows(connection, table_name, filters)
                if not rows:
                    continue
                envelope_key = _generic_envelope_key(route)
                return {
                    envelope_key: [_serialize_generic_row(_generic_row_mapping(row)) for row in rows]
                }
        finally:
            await connection.close()
        return None

    async def _fetch_sport_event_count_payload(self) -> dict[str, dict[str, int]]:
        connection = await self._connect()
        try:
            rows = await connection.fetch(
                """
                WITH daily_events AS (
                    SELECT
                        e.id,
                        COALESCE(unique_category.sport_id, tournament_category.sport_id) AS sport_id,
                        es.type AS status_type
                    FROM event AS e
                    LEFT JOIN unique_tournament AS ut
                        ON ut.id = e.unique_tournament_id
                    LEFT JOIN category AS unique_category
                        ON unique_category.id = ut.category_id
                    LEFT JOIN tournament AS t
                        ON t.id = e.tournament_id
                    LEFT JOIN category AS tournament_category
                        ON tournament_category.id = t.category_id
                    LEFT JOIN event_status AS es
                        ON es.code = e.status_code
                    WHERE e.start_timestamp >= EXTRACT(EPOCH FROM CURRENT_DATE::timestamptz)::bigint
                      AND e.start_timestamp < EXTRACT(EPOCH FROM (CURRENT_DATE + INTERVAL '1 day')::timestamptz)::bigint
                )
                SELECT
                    s.slug AS sport_slug,
                    COUNT(daily_events.id)::bigint AS total_events,
                    COUNT(daily_events.id) FILTER (WHERE daily_events.status_type = 'inprogress')::bigint AS live_events
                FROM sport AS s
                LEFT JOIN daily_events
                    ON daily_events.sport_id = s.id
                GROUP BY s.slug
                ORDER BY s.slug
                """
            )
        finally:
            await connection.close()

        return {
            str(row["sport_slug"]): {
                "live": int(row["live_events"] or 0),
                "total": int(row["total_events"] or 0),
            }
            for row in rows
        }

    async def _fetch_team_statistics_seasons_payload(self, team_id: int) -> dict[str, Any] | None:
        connection = await self._connect()
        try:
            season_rows = await connection.fetch(
                """
                SELECT
                    ess.unique_tournament_id,
                    ut.slug AS unique_tournament_slug,
                    ut.name AS unique_tournament_name,
                    c.id AS category_id,
                    c.slug AS category_slug,
                    c.name AS category_name,
                    s.id AS sport_id,
                    s.slug AS sport_slug,
                    s.name AS sport_name,
                    ess.season_id,
                    season.name AS season_name,
                    season.year AS season_year,
                    ess.all_time_season_id
                FROM entity_statistics_season AS ess
                JOIN unique_tournament AS ut
                    ON ut.id = ess.unique_tournament_id
                LEFT JOIN category AS c
                    ON c.id = ut.category_id
                LEFT JOIN sport AS s
                    ON s.id = c.sport_id
                JOIN season
                    ON season.id = ess.season_id
                WHERE ess.subject_type = 'team'
                  AND ess.subject_id = $1
                ORDER BY ut.name NULLS LAST, ess.unique_tournament_id, season.year DESC NULLS LAST, ess.season_id DESC
                """,
                team_id,
            )
            type_rows = await connection.fetch(
                """
                SELECT unique_tournament_id, season_id, stat_type
                FROM entity_statistics_type
                WHERE subject_type = 'team'
                  AND subject_id = $1
                ORDER BY unique_tournament_id, season_id, stat_type
                """,
                team_id,
            )
        finally:
            await connection.close()

        if not season_rows and not type_rows:
            return None
        return _build_statistics_seasons_payload(season_rows, type_rows)

    async def _fetch_team_player_statistics_seasons_payload(self, team_id: int) -> dict[str, Any] | None:
        connection = await self._connect()
        try:
            season_rows = await connection.fetch(
                """
                SELECT
                    tps.unique_tournament_id,
                    ut.slug AS unique_tournament_slug,
                    ut.name AS unique_tournament_name,
                    c.id AS category_id,
                    c.slug AS category_slug,
                    c.name AS category_name,
                    s.id AS sport_id,
                    s.slug AS sport_slug,
                    s.name AS sport_name,
                    tps.season_id,
                    season.name AS season_name,
                    season.year AS season_year,
                    NULL::bigint AS all_time_season_id
                FROM team_player_statistics_season AS tps
                JOIN unique_tournament AS ut
                    ON ut.id = tps.unique_tournament_id
                LEFT JOIN category AS c
                    ON c.id = ut.category_id
                LEFT JOIN sport AS s
                    ON s.id = c.sport_id
                JOIN season
                    ON season.id = tps.season_id
                WHERE tps.team_id = $1
                ORDER BY ut.name NULLS LAST, tps.unique_tournament_id, season.year DESC NULLS LAST, tps.season_id DESC
                """,
                team_id,
            )
            type_rows = await connection.fetch(
                """
                SELECT unique_tournament_id, season_id, stat_type
                FROM team_player_statistics_type
                WHERE team_id = $1
                ORDER BY unique_tournament_id, season_id, stat_type
                """,
                team_id,
            )
        finally:
            await connection.close()

        if not season_rows and not type_rows:
            return None
        return _build_statistics_seasons_payload(season_rows, type_rows)

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

    async def _fetch_event_root_payload(self, event_id: int) -> dict[str, Any] | None:
        """Return a payload for ``/api/v1/event/{event_id}`` via fallback layers.

        Order of preference:
          1. The finalized snapshot referenced by ``event_terminal_state.final_snapshot_id``
             -- this is the exact upstream response we captured at end-of-match.
          2. The most recent ``endpoint_pattern = /api/v1/event/{event_id}`` raw
             snapshot pinned to this event id.
          3. A minimal envelope synthesised from the normalized ``event`` row.

        Returning ``None`` is reserved for the genuinely-unknown case: no
        finalized state, no raw root snapshot, and no normalized row.
        """

        connection = await self._connect()
        try:
            terminal_row = await connection.fetchrow(
                """
                SELECT ets.terminal_status, ets.finalized_at, aps.payload AS final_payload
                FROM event_terminal_state AS ets
                LEFT JOIN api_payload_snapshot AS aps
                    ON aps.id = ets.final_snapshot_id
                WHERE ets.event_id = $1
                """,
                event_id,
            )
            terminal_status_raw = terminal_row.get("terminal_status") if terminal_row is not None else None
            if terminal_row is not None:
                decoded = _decode_snapshot_payload(terminal_row.get("final_payload"))
                if isinstance(decoded, dict):
                    return decoded

            snapshot_row = await connection.fetchrow(
                """
                SELECT payload
                FROM api_payload_snapshot
                WHERE endpoint_pattern = '/api/v1/event/{event_id}'
                  AND context_entity_type = 'event'
                  AND context_entity_id = $1
                ORDER BY id DESC
                LIMIT 1
                """,
                event_id,
            )
            if snapshot_row is not None:
                decoded = _decode_snapshot_payload(snapshot_row["payload"])
                if isinstance(decoded, dict):
                    return _apply_terminal_status_to_event_payload(decoded, terminal_status_raw)

            normalized_row = await connection.fetchrow(
                """
                SELECT id, slug, tournament_id, unique_tournament_id, season_id,
                       home_team_id, away_team_id, venue_id, start_timestamp
                FROM event
                WHERE id = $1
                """,
                event_id,
            )
            if normalized_row is None:
                return None
            return _apply_terminal_status_to_event_payload(
                _synthesize_event_root_payload(normalized_row),
                terminal_status_raw,
            )
        finally:
            await connection.close()

    async def _fetch_team_root_payload(self, team_id: int) -> dict[str, Any] | None:
        """Return a payload for ``/api/v1/team/{team_id}`` via fallback layers."""

        connection = await self._connect()
        try:
            snapshot_row = await connection.fetchrow(
                """
                SELECT payload
                FROM api_payload_snapshot
                WHERE endpoint_pattern = '/api/v1/team/{team_id}'
                  AND context_entity_type = 'team'
                  AND context_entity_id = $1
                ORDER BY id DESC
                LIMIT 1
                """,
                team_id,
            )
            if snapshot_row is not None:
                decoded = _decode_snapshot_payload(snapshot_row["payload"])
                if isinstance(decoded, dict):
                    return decoded

            normalized_row = await connection.fetchrow(
                """
                SELECT id, slug, name, short_name, sport_id, category_id,
                       country_alpha2, manager_id, venue_id, tournament_id,
                       primary_unique_tournament_id, parent_team_id
                FROM team
                WHERE id = $1
                """,
                team_id,
            )
            if normalized_row is None:
                return None
            return _synthesize_team_root_payload(normalized_row)
        finally:
            await connection.close()

    async def _fetch_player_root_payload(self, player_id: int) -> dict[str, Any] | None:
        """Return a payload for ``/api/v1/player/{player_id}`` via fallback layers."""

        connection = await self._connect()
        try:
            snapshot_row = await connection.fetchrow(
                """
                SELECT payload
                FROM api_payload_snapshot
                WHERE endpoint_pattern = '/api/v1/player/{player_id}'
                  AND context_entity_type = 'player'
                  AND context_entity_id = $1
                ORDER BY id DESC
                LIMIT 1
                """,
                player_id,
            )
            if snapshot_row is not None:
                decoded = _decode_snapshot_payload(snapshot_row["payload"])
                if isinstance(decoded, dict):
                    return decoded

            normalized_row = await connection.fetchrow(
                """
                SELECT id, slug, name, short_name, team_id
                FROM player
                WHERE id = $1
                """,
                player_id,
            )
            if normalized_row is None:
                return None
            return _synthesize_player_root_payload(normalized_row)
        finally:
            await connection.close()

    async def _fetch_manager_root_payload(self, manager_id: int) -> dict[str, Any] | None:
        """Return a payload for ``/api/v1/manager/{manager_id}`` via fallback layers."""

        connection = await self._connect()
        try:
            snapshot_row = await connection.fetchrow(
                """
                SELECT payload
                FROM api_payload_snapshot
                WHERE endpoint_pattern = '/api/v1/manager/{manager_id}'
                  AND context_entity_type = 'manager'
                  AND context_entity_id = $1
                ORDER BY id DESC
                LIMIT 1
                """,
                manager_id,
            )
            if snapshot_row is not None:
                decoded = _decode_snapshot_payload(snapshot_row["payload"])
                if isinstance(decoded, dict):
                    return decoded

            normalized_row = await connection.fetchrow(
                """
                SELECT id, slug, name, short_name, team_id
                FROM manager
                WHERE id = $1
                """,
                manager_id,
            )
            if normalized_row is None:
                return None
            return _synthesize_manager_root_payload(normalized_row)
        finally:
            await connection.close()

    async def _fetch_unique_tournament_root_payload(
        self, unique_tournament_id: int
    ) -> dict[str, Any] | None:
        """Return a payload for ``/api/v1/unique-tournament/{unique_tournament_id}``
        via fallback layers."""

        connection = await self._connect()
        try:
            snapshot_row = await connection.fetchrow(
                """
                SELECT payload
                FROM api_payload_snapshot
                WHERE endpoint_pattern = '/api/v1/unique-tournament/{unique_tournament_id}'
                  AND context_entity_type = 'unique_tournament'
                  AND context_entity_id = $1
                ORDER BY id DESC
                LIMIT 1
                """,
                unique_tournament_id,
            )
            if snapshot_row is not None:
                decoded = _decode_snapshot_payload(snapshot_row["payload"])
                if isinstance(decoded, dict):
                    return decoded

            normalized_row = await connection.fetchrow(
                """
                SELECT id, slug, name, category_id, country_alpha2
                FROM unique_tournament
                WHERE id = $1
                """,
                unique_tournament_id,
            )
            if normalized_row is None:
                return None
            return _synthesize_unique_tournament_root_payload(normalized_row)
        finally:
            await connection.close()

    async def _fetch_ops_health_payload(self) -> dict[str, Any]:
        connection = await self._connect()
        try:
            report = await collect_health_report(
                sql_executor=connection,
                live_state_store=getattr(self, "live_state_store", None),
                redis_backend=getattr(self, "redis_backend", None),
                stream_queue=getattr(self, "stream_queue", None),
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
        summary = await collect_queue_summary(
            stream_queue=getattr(self, "stream_queue", None),
            live_state_store=getattr(self, "live_state_store", None),
            redis_backend=getattr(self, "redis_backend", None),
            now_ms=time.time() * 1000.0,
        )
        return dataclasses.asdict(summary)

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

    async def _fetch_ops_coverage_summary_payload(self) -> dict[str, Any]:
        connection = await self._connect()
        try:
            rows = await connection.fetch(
                """
                SELECT
                    source_slug,
                    sport_slug,
                    surface_name,
                    freshness_status,
                    COUNT(*)::bigint AS tracked_scopes
                FROM coverage_ledger
                GROUP BY source_slug, sport_slug, surface_name, freshness_status
                ORDER BY source_slug, sport_slug, surface_name, freshness_status
                """
            )
        finally:
            await connection.close()
        return {
            "coverage": [
                {
                    "source_slug": str(row["source_slug"]),
                    "sport_slug": str(row["sport_slug"]),
                    "surface_name": str(row["surface_name"]),
                    "freshness_status": str(row["freshness_status"]),
                    "tracked_scopes": int(row["tracked_scopes"] or 0),
                }
                for row in rows
            ]
        }

    async def _connect(self):
        if self._db_pool is None:
            await self.startup()
        assert self._db_pool is not None
        connection = await self._db_pool.acquire()
        return _PooledConnectionLease(self._db_pool, connection)

    def _cache_get(
        self,
        key: tuple[str, tuple[tuple[str, tuple[str, ...]], ...]],
    ) -> SerializedApiResponse | None:
        now = self._cache_now()
        lock = self._response_cache_lock
        if lock is None:
            entry = self._response_cache.get(key)
            if entry is None or entry.expires_at <= now:
                self._response_cache.pop(key, None)
                return None
            return entry.response
        with lock:
            entry = self._response_cache.get(key)
            if entry is None or entry.expires_at <= now:
                self._response_cache.pop(key, None)
                return None
            return entry.response

    def _cache_put(
        self,
        key: tuple[str, tuple[tuple[str, tuple[str, ...]], ...]],
        response: SerializedApiResponse,
        ttl_seconds: float,
    ) -> None:
        entry = _CachedSerializedResponse(response=response, expires_at=self._cache_now() + ttl_seconds)
        lock = self._response_cache_lock
        if lock is None:
            self._response_cache[key] = entry
            return
        with lock:
            self._response_cache[key] = entry


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
            self._send_bytes(
                HTTPStatus.OK,
                self.server.application.openapi_json_for_request(dict(self.headers.items())),
                "application/json; charset=utf-8",
                cache_control="public, max-age=300",
            )
            return
        if path == "/healthz":
            payload = {"ok": True, "service": "local-multisport-api"}
            self._send_json(HTTPStatus.OK, payload, cache_control="no-cache")
            return
        if path.startswith("/ops/"):
            response = self.server.application.run_async(self.server.application.handle_ops_get(path, split.query))
            self._send_json(response.status_code, response.payload, cache_control="no-cache")
            return
        if path.startswith("/api/"):
            response = self.server.application.run_async(
                self.server.application.handle_api_get_http_response(path, split.query)
            )
            self._send_bytes(
                response.status_code,
                response.body,
                "application/json; charset=utf-8",
                cache_control=response.cache_control,
            )
            return
        self._send_json(
            HTTPStatus.NOT_FOUND,
            {"error": "Unknown route.", "path": path},
            cache_control="no-cache",
        )

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self._send_common_headers("application/json; charset=utf-8", cache_control="no-cache")
        self.end_headers()

    def log_message(self, format: str, *args: Any) -> None:
        message = format % args
        print(message, flush=True)

    def _send_html(self, status_code: int, payload: str) -> None:
        self._send_bytes(
            status_code,
            payload.encode("utf-8"),
            "text/html; charset=utf-8",
            cache_control="public, max-age=300",
        )

    def _send_json(self, status_code: int, payload: Any, *, cache_control: str) -> None:
        body = _json_dumps_bytes(payload)
        self._send_bytes(status_code, body, "application/json; charset=utf-8", cache_control=cache_control)

    def _send_bytes(self, status_code: int, body: bytes, content_type: str, *, cache_control: str) -> None:
        self.send_response(status_code)
        self._send_common_headers(content_type, cache_control=cache_control)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_common_headers(self, content_type: str, *, cache_control: str) -> None:
        self.send_header("Content-Type", content_type)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Cache-Control", cache_control)


def create_local_api_application(
    *,
    host: str = "0.0.0.0",
    port: int = 8000,
    database_url: str | None = None,
    redis_url: str | None = None,
    public_base_urls: tuple[str, ...] = (),
) -> LocalApiApplication:
    database_config = load_database_config(dsn=database_url)
    base_url = f"http://{host}:{port}"
    return LocalApiApplication(
        database_config=database_config,
        base_url=base_url,
        openapi_base_urls=resolve_openapi_base_urls(
            primary_base_url=base_url,
            public_base_urls=public_base_urls,
        ),
        redis_backend=_load_optional_redis_backend(redis_url),
    )


def create_asgi_app(
    *,
    application: LocalApiApplication | None = None,
    host: str = "0.0.0.0",
    port: int = 8000,
    database_url: str | None = None,
    redis_url: str | None = None,
    public_base_urls: tuple[str, ...] = (),
):
    local_application = application or create_local_api_application(
        host=host,
        port=port,
        database_url=database_url,
        redis_url=redis_url,
        public_base_urls=public_base_urls,
    )

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await local_application.startup()
        try:
            yield
        finally:
            await local_application.shutdown()

    app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    app.state.local_api_application = local_application

    @app.api_route("/{path:path}", methods=["OPTIONS"])
    async def _options(path: str) -> Response:
        del path
        return Response(status_code=HTTPStatus.NO_CONTENT, headers=_asgi_common_headers("no-cache"))

    @app.get("/{path:path}")
    async def _get(path: str, request: Request) -> Response:
        raw_path = "/" + path if path else "/"
        raw_query = request.url.query
        if raw_path in {"/", "/docs"}:
            return Response(
                content=local_application.swagger_html,
                status_code=HTTPStatus.OK,
                media_type="text/html; charset=utf-8",
                headers=_asgi_common_headers("public, max-age=300"),
            )
        if raw_path == "/openapi.json":
            headers = {str(key): str(value) for key, value in request.headers.items()}
            body = await asyncio.to_thread(local_application.openapi_json_for_request, headers)
            return Response(
                content=body,
                status_code=HTTPStatus.OK,
                media_type="application/json; charset=utf-8",
                headers=_asgi_common_headers("public, max-age=300"),
            )
        if raw_path == "/healthz":
            return Response(
                content=_json_dumps_bytes({"ok": True, "service": "local-multisport-api"}),
                status_code=HTTPStatus.OK,
                media_type="application/json; charset=utf-8",
                headers=_asgi_common_headers("no-cache"),
            )
        if raw_path.startswith("/ops/"):
            api_response = await local_application.run_in_runtime(
                local_application.handle_ops_get(raw_path, raw_query)
            )
            return Response(
                content=_json_dumps_bytes(api_response.payload),
                status_code=api_response.status_code,
                media_type="application/json; charset=utf-8",
                headers=_asgi_common_headers("no-cache"),
            )
        if raw_path.startswith("/api/"):
            api_response = await local_application.run_in_runtime(
                local_application.handle_api_get_http_response(raw_path, raw_query)
            )
            return Response(
                content=api_response.body,
                status_code=api_response.status_code,
                media_type="application/json; charset=utf-8",
                headers=_asgi_common_headers(api_response.cache_control),
            )
        return Response(
            content=_json_dumps_bytes({"error": "Unknown route.", "path": raw_path}),
            status_code=HTTPStatus.NOT_FOUND,
            media_type="application/json; charset=utf-8",
            headers=_asgi_common_headers("no-cache"),
        )

    return app


def _asgi_common_headers(cache_control: str) -> dict[str, str]:
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Cache-Control": cache_control,
    }


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
    if path_template.startswith("/api/v1/category/"):
        return "category"
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
    if path_template.startswith("/api/v1/category/"):
        return "category_id"
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
                return orjson.loads(stripped)
            except orjson.JSONDecodeError:
                return payload
    return payload


def _json_dumps_bytes(payload: Any) -> bytes:
    return orjson.dumps(payload, default=_json_default)


def _response_cache_key(raw_path: str, raw_query: str) -> tuple[str, tuple[tuple[str, tuple[str, ...]], ...]]:
    normalized_query = tuple(sorted((key, tuple(values)) for key, values in _normalized_query_map(raw_query).items()))
    return raw_path, normalized_query


def _cache_control_for_response(route: RouteSpec | None, payload: Any) -> str:
    ttl = _response_ttl_seconds(route, payload)
    if ttl <= 0:
        return "no-cache"
    return f"public, max-age={int(ttl)}"


def _response_ttl_seconds(route: RouteSpec | None, payload: Any) -> float:
    if route is None:
        return 0.0
    path_template = route.endpoint.path_template
    if "/events/live" in path_template:
        return 2.0
    status_type = _payload_status_type(payload)
    if status_type is not None:
        normalized = status_type.strip().lower()
        if normalized and normalized not in _NATURAL_TERMINAL_STATUSES and normalized not in {"notstarted"}:
            return 2.0
        return 30.0
    if path_template.startswith("/api/v1/event/"):
        return 2.0
    return 30.0


def _empty_payload_for_missing_route(route: RouteSpec) -> dict[str, Any] | None:
    path_template = route.endpoint.path_template
    if route.endpoint.envelope_key == "events" and path_template.endswith("/scheduled-events/{date}"):
        return {"events": []}
    return None


def _payload_status_type(payload: Any) -> str | None:
    if isinstance(payload, dict):
        event_payload = payload.get("event")
        if isinstance(event_payload, dict):
            status_payload = event_payload.get("status")
            if isinstance(status_payload, dict) and isinstance(status_payload.get("type"), str):
                return str(status_payload["type"])
        status_payload = payload.get("status")
        if isinstance(status_payload, dict) and isinstance(status_payload.get("type"), str):
            return str(status_payload["type"])
        for envelope_key in ("events", "featuredEvents"):
            items = payload.get(envelope_key)
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    status = item.get("status")
                    if isinstance(status, dict) and isinstance(status.get("type"), str):
                        return str(status["type"])
    return None


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


def _serialize_scalar(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Mapping):
        return {str(key): _serialize_scalar(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_serialize_scalar(item) for item in value]
    if isinstance(value, tuple):
        return [_serialize_scalar(item) for item in value]
    return value


def _first_not_none(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def _minimal_entity_payload(
    entity_id: Any,
    *,
    slug: Any = None,
    name: Any = None,
    short_name: Any = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {"id": int(entity_id)}
    if slug is not None:
        payload["slug"] = str(slug)
    if name is not None:
        payload["name"] = str(name)
    if short_name is not None:
        payload["shortName"] = str(short_name)
    return payload


def _optional_int_path_param(path_params: dict[str, str], name: str) -> int | None:
    raw_value = path_params.get(name)
    if raw_value is None:
        return None
    try:
        return int(raw_value)
    except ValueError:
        return None


def _top_snapshot_query(table_name: str, path_params: dict[str, str]) -> str:
    if table_name not in {"top_player_snapshot", "top_team_snapshot"}:
        raise ValueError(f"Unsupported top snapshot table: {table_name}")
    statistics_type_expression = "statistics_type" if table_name == "top_player_snapshot" else "NULL::jsonb AS statistics_type"
    team_filter = "AND ($4::bigint IS NULL OR source_url LIKE '%/team/' || $4::text || '/%')"
    return f"""
        SELECT id, {statistics_type_expression}
        FROM {table_name}
        WHERE endpoint_pattern = $1
          AND unique_tournament_id = $2
          AND season_id = $3
          {team_filter}
        ORDER BY fetched_at DESC NULLS LAST, id DESC
        LIMIT 1
    """


def _source_tables_by_path_template() -> dict[str, tuple[str, ...]]:
    global _SOURCE_TABLES_BY_PATH_TEMPLATE
    if _SOURCE_TABLES_BY_PATH_TEMPLATE is None:
        document = build_openapi_document(_empty_summary())
        values: dict[str, tuple[str, ...]] = {}
        for path_template, operations in document.get("paths", {}).items():
            get_operation = operations.get("get") if isinstance(operations, dict) else None
            if not isinstance(get_operation, dict):
                continue
            source_tables = get_operation.get("x-source-tables")
            if isinstance(source_tables, list):
                values[str(path_template)] = tuple(str(item) for item in source_tables)
        _SOURCE_TABLES_BY_PATH_TEMPLATE = values
    return _SOURCE_TABLES_BY_PATH_TEMPLATE


def _generic_fallback_candidate_tables(route: RouteSpec) -> tuple[str, ...]:
    candidates: list[str] = []
    target_table = route.endpoint.target_table
    if target_table and target_table != "api_payload_snapshot":
        candidates.append(target_table)
    for table_name in _source_tables_by_path_template().get(route.endpoint.path_template, ()):
        if table_name == "api_payload_snapshot":
            continue
        if table_name not in candidates:
            candidates.append(table_name)
    return tuple(table_name for table_name in candidates if _is_safe_sql_identifier(table_name))


def _is_safe_sql_identifier(value: str) -> bool:
    return bool(_SAFE_SQL_IDENTIFIER_RE.fullmatch(value))


async def _fetch_table_columns(connection: Any, table_name: str) -> frozenset[str]:
    rows = await connection.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = $1
        """,
        table_name,
    )
    return frozenset(str(row["column_name"]) for row in rows)


def _generic_filters_for_columns(path_params: dict[str, str], columns: frozenset[str]) -> dict[str, Any]:
    filters: dict[str, Any] = {}
    for name, raw_value in path_params.items():
        if name in {"page", "date", "timezone_offset_seconds"}:
            continue
        if name not in columns:
            continue
        try:
            filters[name] = int(raw_value)
        except ValueError:
            filters[name] = raw_value
    return filters


async def _fetch_generic_rows(connection: Any, table_name: str, filters: dict[str, Any]) -> list[Any]:
    where_parts: list[str] = []
    args: list[Any] = []
    for column_name, value in filters.items():
        if not _is_safe_sql_identifier(column_name):
            continue
        args.append(value)
        where_parts.append(f"{column_name} = ${len(args)}")
    if not where_parts:
        return []
    query = f"""
        SELECT to_jsonb(t) AS row
        FROM (
            SELECT *
            FROM {table_name}
            WHERE {' AND '.join(where_parts)}
            LIMIT 500
        ) AS t
    """
    return list(await connection.fetch(query, *args))


def _generic_row_mapping(row: Any) -> dict[str, Any]:
    value = row["row"] if "row" in row else row
    if isinstance(value, str | bytes | bytearray):
        value = orjson.loads(value)
    if isinstance(value, Mapping):
        return dict(value)
    return {}


def _generic_envelope_key(route: RouteSpec) -> str:
    envelope_key = route.endpoint.envelope_key
    if envelope_key and "," not in envelope_key:
        return envelope_key
    target_table = route.endpoint.target_table
    if target_table and target_table != "api_payload_snapshot":
        return _snake_to_camel(_pluralize_table_name(target_table))
    return "rows"


def _serialize_generic_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        _snake_to_camel(str(key)): _serialize_scalar(value)
        for key, value in row.items()
    }


def _snake_to_camel(value: str) -> str:
    pieces = value.split("_")
    if not pieces:
        return value
    return pieces[0] + "".join(piece[:1].upper() + piece[1:] for piece in pieces[1:])


def _pluralize_table_name(value: str) -> str:
    if value.endswith("y"):
        return value[:-1] + "ies"
    if value.endswith("s"):
        return value
    return value + "s"


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


def _extract_sport_slug_from_live_events_path(path: str) -> str | None:
    match = re.fullmatch(r"/api/v1/sport/(?P<sport_slug>[^/]+)/events/live", path)
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


_ENTITY_ROOT_PATH_PATTERNS: dict[str, re.Pattern[str]] = {
    "event": re.compile(r"^/api/v1/event/(?P<id>\d+)$"),
    "team": re.compile(r"^/api/v1/team/(?P<id>\d+)$"),
    "player": re.compile(r"^/api/v1/player/(?P<id>\d+)$"),
    "manager": re.compile(r"^/api/v1/manager/(?P<id>\d+)$"),
    "unique-tournament": re.compile(r"^/api/v1/unique-tournament/(?P<id>\d+)$"),
}


def _extract_event_id_from_entity_root_path(path: str, entity_kind: str) -> int | None:
    """Return the numeric id embedded in an ``/api/v1/{kind}/{id}`` path.

    The helper is shared by the normalized-fallback entry points so that
    every ``<entity>`` root route -- event / team / player / manager /
    unique-tournament -- is extracted uniformly. Unknown kinds return
    ``None``; non-numeric ids return ``None`` instead of raising.
    """

    pattern = _ENTITY_ROOT_PATH_PATTERNS.get(entity_kind)
    if pattern is None:
        return None
    match = pattern.fullmatch(path)
    if match is None:
        return None
    try:
        return int(match.group("id"))
    except (TypeError, ValueError):
        return None


def _synthesize_event_root_payload(row: Any) -> dict[str, Any]:
    event_payload: dict[str, Any] = {
        "id": int(row["id"]),
    }
    slug = row["slug"]
    if slug is not None:
        event_payload["slug"] = str(slug)
    start_timestamp = row["start_timestamp"]
    if start_timestamp is not None:
        try:
            event_payload["startTimestamp"] = int(start_timestamp)
        except (TypeError, ValueError):
            event_payload["startTimestamp"] = _serialize_scalar(start_timestamp)
    home_team_id = row["home_team_id"]
    if home_team_id is not None:
        event_payload["homeTeam"] = {"id": int(home_team_id)}
    away_team_id = row["away_team_id"]
    if away_team_id is not None:
        event_payload["awayTeam"] = {"id": int(away_team_id)}
    tournament_id = row["tournament_id"]
    unique_tournament_id = row["unique_tournament_id"]
    season_id = row["season_id"]
    if tournament_id is not None or unique_tournament_id is not None:
        tournament_payload: dict[str, Any] = {}
        if tournament_id is not None:
            tournament_payload["id"] = int(tournament_id)
        if unique_tournament_id is not None:
            tournament_payload["uniqueTournament"] = {"id": int(unique_tournament_id)}
        event_payload["tournament"] = tournament_payload
    if season_id is not None:
        event_payload["season"] = {"id": int(season_id)}
    return {"event": event_payload}


def _serialize_season_event_row(row: Any) -> dict[str, Any]:
    event_payload: dict[str, Any] = {"id": int(row["id"])}
    for source_key, dest_key in (
        ("slug", "slug"),
        ("custom_id", "customId"),
    ):
        value = row.get(source_key)
        if value is not None:
            event_payload[dest_key] = str(value)
    start_timestamp = row.get("start_timestamp")
    if start_timestamp is not None:
        event_payload["startTimestamp"] = int(start_timestamp)

    status_code = row.get("status_code")
    if status_code is not None:
        status_payload: dict[str, Any] = {"code": int(status_code)}
        if row.get("status_type") is not None:
            status_payload["type"] = str(row["status_type"])
        if row.get("status_description") is not None:
            status_payload["description"] = str(row["status_description"])
        event_payload["status"] = status_payload

    home_team = _minimal_entity_payload(
        row["home_team_id"],
        slug=row.get("home_team_slug"),
        name=row.get("home_team_name"),
        short_name=row.get("home_team_short_name"),
    ) if row.get("home_team_id") is not None else None
    if home_team is not None:
        event_payload["homeTeam"] = home_team

    away_team = _minimal_entity_payload(
        row["away_team_id"],
        slug=row.get("away_team_slug"),
        name=row.get("away_team_name"),
        short_name=row.get("away_team_short_name"),
    ) if row.get("away_team_id") is not None else None
    if away_team is not None:
        event_payload["awayTeam"] = away_team

    tournament_id = row.get("tournament_id")
    unique_tournament_id = row.get("unique_tournament_id")
    if tournament_id is not None or unique_tournament_id is not None:
        tournament_payload: dict[str, Any] = {}
        if tournament_id is not None:
            tournament_payload.update(
                _minimal_entity_payload(
                    tournament_id,
                    slug=row.get("tournament_slug"),
                    name=row.get("tournament_name"),
                )
            )
        if unique_tournament_id is not None:
            tournament_payload["uniqueTournament"] = _minimal_entity_payload(
                unique_tournament_id,
                slug=row.get("unique_tournament_slug"),
                name=row.get("unique_tournament_name"),
            )
        event_payload["tournament"] = tournament_payload

    season_id = row.get("season_id")
    if season_id is not None:
        season_payload: dict[str, Any] = {"id": int(season_id)}
        if row.get("season_name") is not None:
            season_payload["name"] = str(row["season_name"])
        if row.get("season_year") is not None:
            season_payload["year"] = str(row["season_year"])
        event_payload["season"] = season_payload

    for source_key, dest_key in (
        ("winner_code", "winnerCode"),
        ("aggregated_winner_code", "aggregatedWinnerCode"),
        ("coverage", "coverage"),
        ("home_red_cards", "homeRedCards"),
        ("away_red_cards", "awayRedCards"),
    ):
        value = row.get(source_key)
        if value is not None:
            event_payload[dest_key] = int(value)
    return event_payload


def _synthesize_team_root_payload(row: Any) -> dict[str, Any]:
    team_payload: dict[str, Any] = {"id": int(row["id"])}
    for source_key, dest_key in (
        ("slug", "slug"),
        ("name", "name"),
        ("short_name", "shortName"),
    ):
        value = row[source_key]
        if value is not None:
            team_payload[dest_key] = str(value)
    if row["country_alpha2"] is not None:
        team_payload["country"] = {"alpha2": str(row["country_alpha2"])}
    if row["sport_id"] is not None:
        team_payload["sport"] = {"id": int(row["sport_id"])}
    if row["category_id"] is not None:
        team_payload["category"] = {"id": int(row["category_id"])}
    if row["venue_id"] is not None:
        team_payload["venue"] = {"id": int(row["venue_id"])}
    if row["manager_id"] is not None:
        team_payload["manager"] = {"id": int(row["manager_id"])}
    if row["tournament_id"] is not None:
        team_payload["tournament"] = {"id": int(row["tournament_id"])}
    if row["primary_unique_tournament_id"] is not None:
        team_payload["primaryUniqueTournament"] = {"id": int(row["primary_unique_tournament_id"])}
    if row["parent_team_id"] is not None:
        team_payload["parentTeam"] = {"id": int(row["parent_team_id"])}
    return {"team": team_payload}


def _synthesize_player_root_payload(row: Any) -> dict[str, Any]:
    player_payload: dict[str, Any] = {"id": int(row["id"])}
    for source_key, dest_key in (
        ("slug", "slug"),
        ("name", "name"),
        ("short_name", "shortName"),
    ):
        value = row[source_key]
        if value is not None:
            player_payload[dest_key] = str(value)
    if row["team_id"] is not None:
        player_payload["team"] = {"id": int(row["team_id"])}
    return {"player": player_payload}


def _synthesize_manager_root_payload(row: Any) -> dict[str, Any]:
    manager_payload: dict[str, Any] = {"id": int(row["id"])}
    for source_key, dest_key in (
        ("slug", "slug"),
        ("name", "name"),
        ("short_name", "shortName"),
    ):
        value = row[source_key]
        if value is not None:
            manager_payload[dest_key] = str(value)
    if row["team_id"] is not None:
        manager_payload["team"] = {"id": int(row["team_id"])}
    return {"manager": manager_payload}


def _synthesize_unique_tournament_root_payload(row: Any) -> dict[str, Any]:
    ut_payload: dict[str, Any] = {"id": int(row["id"])}
    for source_key, dest_key in (
        ("slug", "slug"),
        ("name", "name"),
    ):
        value = row[source_key]
        if value is not None:
            ut_payload[dest_key] = str(value)
    if row["category_id"] is not None:
        ut_payload["category"] = {"id": int(row["category_id"])}
    if row["country_alpha2"] is not None:
        ut_payload["country"] = {"alpha2": str(row["country_alpha2"])}
    return {"uniqueTournament": ut_payload}


def _build_statistics_seasons_payload(season_rows: list[Any], type_rows: list[Any]) -> dict[str, Any]:
    grouped: dict[int, dict[str, Any]] = {}
    for row in season_rows:
        unique_tournament_id = int(row["unique_tournament_id"])
        item = grouped.setdefault(
            unique_tournament_id,
            {
                "uniqueTournament": _statistics_unique_tournament_payload(row),
                "seasons": [],
            },
        )
        all_time_season_id = row["all_time_season_id"]
        if all_time_season_id is not None and "allTimeSeasonId" not in item:
            item["allTimeSeasonId"] = int(all_time_season_id)
        item["seasons"].append(_statistics_season_payload(row))

    types_map: dict[str, dict[str, list[str]]] = {}
    for row in type_rows:
        unique_tournament_id = str(int(row["unique_tournament_id"]))
        season_id = str(int(row["season_id"]))
        types_map.setdefault(unique_tournament_id, {}).setdefault(season_id, []).append(str(row["stat_type"]))
    for season_map in types_map.values():
        for stat_types in season_map.values():
            stat_types.sort()

    return {
        "uniqueTournamentSeasons": list(grouped.values()),
        "typesMap": types_map,
    }


def _statistics_unique_tournament_payload(row: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {"id": int(row["unique_tournament_id"])}
    if row["unique_tournament_slug"] is not None:
        payload["slug"] = str(row["unique_tournament_slug"])
    if row["unique_tournament_name"] is not None:
        payload["name"] = str(row["unique_tournament_name"])
    category_payload: dict[str, Any] = {}
    if row["category_id"] is not None:
        category_payload["id"] = int(row["category_id"])
    if row["category_slug"] is not None:
        category_payload["slug"] = str(row["category_slug"])
    if row["category_name"] is not None:
        category_payload["name"] = str(row["category_name"])
    sport_payload: dict[str, Any] = {}
    if row["sport_id"] is not None:
        sport_payload["id"] = int(row["sport_id"])
    if row["sport_slug"] is not None:
        sport_payload["slug"] = str(row["sport_slug"])
    if row["sport_name"] is not None:
        sport_payload["name"] = str(row["sport_name"])
    if sport_payload:
        category_payload["sport"] = sport_payload
    if category_payload:
        payload["category"] = category_payload
    return payload


def _statistics_season_payload(row: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {"id": int(row["season_id"])}
    if row["season_name"] is not None:
        payload["name"] = str(row["season_name"])
    if row["season_year"] is not None:
        payload["year"] = str(row["season_year"])
    return payload


def _apply_event_surface_overlay(item: dict[str, Any], row: Mapping[str, Any]) -> dict[str, Any]:
    updated = dict(item)

    status_code = row.get("status_code")
    if status_code is not None:
        status_payload: dict[str, Any] = {"code": int(status_code)}
        if row.get("status_type") is not None:
            status_payload["type"] = str(row["status_type"])
        if row.get("status_description") is not None:
            status_payload["description"] = str(row["status_description"])
        updated["status"] = status_payload

    for source_key, dest_key in (
        ("start_timestamp", "startTimestamp"),
        ("winner_code", "winnerCode"),
        ("aggregated_winner_code", "aggregatedWinnerCode"),
        ("coverage", "coverage"),
        ("home_red_cards", "homeRedCards"),
        ("away_red_cards", "awayRedCards"),
        ("detail_id", "detailId"),
    ):
        value = row.get(source_key)
        if value is not None:
            updated[dest_key] = _serialize_scalar(value)

    for source_key, dest_key in (
        ("correct_ai_insight", "correctAiInsight"),
        ("correct_halftime_ai_insight", "correctHalftimeAiInsight"),
        ("feed_locked", "feedLocked"),
        ("is_editor", "isEditor"),
        ("crowdsourcing_data_display_enabled", "crowdsourcingDataDisplayEnabled"),
        ("final_result_only", "finalResultOnly"),
        ("has_event_player_statistics", "hasEventPlayerStatistics"),
        ("has_event_player_heat_map", "hasEventPlayerHeatMap"),
        ("has_global_highlights", "hasGlobalHighlights"),
        ("has_xg", "hasXg"),
    ):
        value = row.get(source_key)
        if value is not None:
            updated[dest_key] = bool(value)

    scores = _decoded_mapping(row.get("scores"))
    home_score = _decoded_mapping(scores.get("home") if scores else None)
    away_score = _decoded_mapping(scores.get("away") if scores else None)
    if home_score is not None:
        updated["homeScore"] = _strip_none_mapping(home_score)
    if away_score is not None:
        updated["awayScore"] = _strip_none_mapping(away_score)

    for source_key, dest_key in (
        ("round_info_payload", "roundInfo"),
        ("status_time_payload", "statusTime"),
        ("time_payload", "time"),
        ("changes_payload", "changes"),
        ("event_filters_payload", "eventFilters"),
    ):
        payload = _decoded_mapping(row.get(source_key))
        if payload:
            updated[dest_key] = payload

    return updated


def _decoded_mapping(value: Any) -> dict[str, Any] | None:
    decoded = _decode_snapshot_payload(value)
    if isinstance(decoded, Mapping):
        return {str(key): _serialize_scalar(item) for key, item in decoded.items()}
    return None


def _strip_none_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): _serialize_scalar(item) for key, item in value.items() if item is not None}


def _extract_terminal_status_payload(final_payload: Any) -> dict[str, Any] | None:
    decoded_payload = _decode_snapshot_payload(final_payload)
    if not isinstance(decoded_payload, dict):
        return None
    event_payload = decoded_payload.get("event")
    if not isinstance(event_payload, dict):
        return None
    status_payload = event_payload.get("status")
    if not isinstance(status_payload, dict):
        return None
    return dict(status_payload)


def _extract_existing_event_status(items: list[Any], event_id: int) -> dict[str, Any] | None:
    for item in items:
        if not isinstance(item, dict):
            continue
        if int(item.get("id") or 0) != event_id:
            continue
        status_payload = item.get("status")
        if isinstance(status_payload, dict):
            return dict(status_payload)
        return None
    return None


def _terminal_candidate_sort_key(candidate: dict[str, Any]) -> tuple[str, int]:
    row = candidate.get("row")
    event_id = 0
    if isinstance(row, dict) and row.get("event_id") is not None:
        try:
            event_id = int(row["event_id"])
        except (TypeError, ValueError):
            event_id = 0
    return (str(candidate.get("finalized_at") or ""), event_id)


def _event_item_identity_matches_terminal_row(item: dict[str, Any], row: dict[str, Any]) -> bool:
    custom_id = item.get("customId")
    existing_custom_id = row.get("custom_id")
    if custom_id is None or existing_custom_id is None:
        return False
    if str(custom_id) != str(existing_custom_id):
        return False
    for item_key, row_key in (
        ("startTimestamp", "start_timestamp"),
        ("homeTeam", "home_team_id"),
        ("awayTeam", "away_team_id"),
    ):
        item_value = item.get(item_key)
        if isinstance(item_value, dict):
            item_value = item_value.get("id")
        row_value = row.get(row_key)
        if item_value is not None and row_value is not None:
            try:
                if int(item_value) != int(row_value):
                    return False
            except (TypeError, ValueError):
                if str(item_value) != str(row_value):
                    return False
    return True


def _fallback_terminal_status_payload(
    terminal_status: Any,
    existing_status: dict[str, Any] | None,
) -> dict[str, Any] | None:
    normalized = str(terminal_status or "").strip().lower()
    if not normalized:
        return existing_status

    base = dict(existing_status or {})
    fallback_by_type: dict[str, dict[str, Any]] = {
        "finished": {"code": 100, "type": "finished", "description": "Ended"},
        "afterextra": {"type": "afterextra", "description": "After extra time"},
        "afterpen": {"type": "afterpen", "description": "After penalties"},
        "cancelled": {"type": "cancelled", "description": "Cancelled"},
        "postponed": {"type": "postponed", "description": "Postponed"},
        "zombie_stale": {"code": 91, "type": "stale_live", "description": "Retired stale live event"},
    }
    fallback = fallback_by_type.get(normalized)
    if fallback is None:
        return base or None
    base.update(fallback)
    return base


def _apply_terminal_status_to_event_payload(
    payload: dict[str, Any],
    terminal_status: Any,
) -> dict[str, Any]:
    event = payload.get("event")
    if not isinstance(event, dict):
        return payload
    status_payload = _fallback_terminal_status_payload(terminal_status, event.get("status"))
    if status_payload is None:
        return payload
    updated_event = dict(event)
    updated_event["status"] = status_payload
    updated_payload = dict(payload)
    updated_payload["event"] = updated_event
    return updated_payload


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
        _close_redis_backend_sync(backend)
        return None
    return backend


async def _close_redis_backend(backend: Any | None) -> None:
    close_backend = getattr(backend, "close", None)
    if not callable(close_backend):
        return
    maybe_awaitable = close_backend()
    if asyncio.iscoroutine(maybe_awaitable):
        await maybe_awaitable


def _close_redis_backend_sync(backend: Any | None) -> None:
    close_backend = getattr(backend, "close", None)
    if callable(close_backend):
        close_backend()


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
