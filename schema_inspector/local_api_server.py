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
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import orjson

from .db import DatabaseConfig, create_pool_with_fallback, load_database_config
from .endpoints import SofascoreEndpoint, local_api_endpoints
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
        if self._db_pool is None:
            return
        await self._db_pool.close()
        self._db_pool = None

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

        rows = await executor.fetch(
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
        if not rows:
            return payload

        terminal_by_event: dict[int, dict[str, Any]] = {}
        terminal_by_custom_id: dict[str, dict[str, Any]] = {}
        for row in rows:
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

        if not terminal_by_event and not terminal_by_custom_id:
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
            terminal_candidate = terminal_by_event.get(event_id)
            if terminal_candidate is None:
                custom_id = item.get("customId")
                if custom_id is not None:
                    by_custom_id = terminal_by_custom_id.get(str(custom_id))
                    if by_custom_id is not None and _event_item_identity_matches_terminal_row(item, by_custom_id["row"]):
                        terminal_candidate = by_custom_id
            if terminal_candidate is None:
                reconciled_items.append(item)
                continue
            updated = dict(item)
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
            final_payload_row = await connection.fetchrow(
                """
                SELECT aps.payload AS final_payload
                FROM event_terminal_state AS ets
                JOIN api_payload_snapshot AS aps
                    ON aps.id = ets.final_snapshot_id
                WHERE ets.event_id = $1
                """,
                event_id,
            )
            if final_payload_row is not None:
                decoded = _decode_snapshot_payload(final_payload_row["final_payload"])
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
                    return decoded

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
            return _synthesize_event_root_payload(normalized_row)
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
    }
    fallback = fallback_by_type.get(normalized)
    if fallback is None:
        return base or None
    base.update(fallback)
    return base


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
