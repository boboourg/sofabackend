"""Serve the ingested multi-sport dataset through local Sofascore-style HTTP routes."""

from __future__ import annotations

import argparse
import asyncio
import json
import re
from dataclasses import dataclass
from decimal import Decimal
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlsplit

from .db import DatabaseConfig, load_database_config
from .endpoints import SofascoreEndpoint, local_api_endpoints

from .local_swagger_builder import _build_viewer_html, _empty_summary, _load_summary, build_openapi_document

_ALL_ENDPOINTS = local_api_endpoints()


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
    args = parser.parse_args()

    database_config = load_database_config(dsn=args.database_url)
    base_url = f"http://{args.host}:{args.port}"
    application = LocalApiApplication(database_config=database_config, base_url=base_url)
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

    def __init__(self, *, database_config: DatabaseConfig, base_url: str) -> None:
        self.database_config = database_config
        self.base_url = base_url.rstrip("/")
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
        document = build_openapi_document(summary)
        document["servers"] = [
            {
                "url": self.base_url,
                "description": "Live local multi-sport API server backed by PostgreSQL snapshots",
            }
        ]
        return document

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


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


if __name__ == "__main__":
    raise SystemExit(main())
