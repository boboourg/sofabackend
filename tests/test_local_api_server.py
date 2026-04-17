from __future__ import annotations

import unittest

from schema_inspector.local_api_server import (
    ApiResponse,
    LocalApiApplication,
    _compile_path_template,
    _decode_snapshot_payload,
    _normalized_query_map,
    _parse_context_value,
    build_route_specs,
    match_route,
)


class LocalApiServerTests(unittest.TestCase):
    def test_route_registry_contains_expected_paths(self) -> None:
        routes = build_route_specs()
        patterns = {route.endpoint.path_template for route in routes}
        self.assertIn("/api/v1/player/{player_id}/statistics", patterns)
        self.assertIn("/api/v1/event/{event_id}/lineups", patterns)
        self.assertIn("/api/v1/event/{event_id}/comments", patterns)
        self.assertIn("/api/v1/event/{event_id}/graph", patterns)
        self.assertIn("/api/v1/event/{event_id}/heatmap/{team_id}", patterns)
        self.assertIn("/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics", patterns)
        self.assertIn("/api/v1/sport/handball/scheduled-events/{date}", patterns)
        self.assertIn("/api/v1/sport/esports/events/live", patterns)
        self.assertIn("/api/v1/event/{event_id}/innings", patterns)
        self.assertIn("/api/v1/event/{event_id}/atbat/{at_bat_id}/pitches", patterns)
        self.assertIn("/api/v1/event/{event_id}/shotmap", patterns)
        self.assertIn("/api/v1/event/{event_id}/esports-games", patterns)

    def test_compile_path_template_extracts_named_params(self) -> None:
        regex = _compile_path_template(
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-of-the-week/{period_id}"
        )
        match = regex.fullmatch("/api/v1/unique-tournament/17/season/76986/team-of-the-week/9001")
        assert match is not None
        self.assertEqual(
            match.groupdict(),
            {
                "unique_tournament_id": "17",
                "season_id": "76986",
                "period_id": "9001",
            },
        )

    def test_match_route_finds_statistics_endpoint(self) -> None:
        routes = build_route_specs()
        result = match_route("/api/v1/player/288205/statistics", routes)
        assert result is not None
        route, params = result
        self.assertEqual(route.endpoint.path_template, "/api/v1/player/{player_id}/statistics")
        self.assertEqual(params["player_id"], "288205")

    def test_query_normalization_ignores_parameter_order(self) -> None:
        left = _normalized_query_map("limit=20&offset=0&fields=goals%2Cassists")
        right = _normalized_query_map("fields=goals%2Cassists&offset=0&limit=20")
        self.assertEqual(left, right)

    def test_decode_snapshot_payload_parses_json_strings(self) -> None:
        value = _decode_snapshot_payload('{"seasons":[{"year":"2026"}]}')
        self.assertEqual(value["seasons"][0]["year"], "2026")

    def test_parse_context_value_returns_integer(self) -> None:
        routes = build_route_specs()
        result = match_route("/api/v1/event/14083182", routes)
        assert result is not None
        route, params = result
        self.assertEqual(_parse_context_value(route, params), 14083182)

    def test_team_performance_graph_route_uses_team_context(self) -> None:
        routes = build_route_specs()
        result = match_route(
            "/api/v1/unique-tournament/17/season/76986/team/42/team-performance-graph-data",
            routes,
        )
        assert result is not None
        route, params = result
        self.assertEqual(
            route.endpoint.path_template,
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team/{team_id}/team-performance-graph-data",
        )
        self.assertEqual(route.context_entity_type, "team")
        self.assertEqual(route.context_param_name, "team_id")
        self.assertEqual(_parse_context_value(route, params), 42)


class LocalApiOperationsTests(unittest.IsolatedAsyncioTestCase):
    async def test_handle_ops_get_routes_supported_monitoring_paths(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        calls: list[tuple[str, int | None]] = []

        async def fake_health() -> dict[str, object]:
            calls.append(("health", None))
            return {"database_ok": True}

        async def fake_snapshots() -> dict[str, object]:
            calls.append(("snapshots", None))
            return {"raw_snapshots": 12}

        async def fake_queues() -> dict[str, object]:
            calls.append(("queues", None))
            return {"pending_total": 4}

        async def fake_jobs(limit: int) -> dict[str, object]:
            calls.append(("jobs", limit))
            return {"jobRuns": []}

        application._fetch_ops_health_payload = fake_health
        application._fetch_ops_snapshots_summary_payload = fake_snapshots
        application._fetch_ops_queue_summary_payload = fake_queues
        application._fetch_ops_job_runs_payload = fake_jobs

        health = await application.handle_ops_get("/ops/health", "")
        snapshots = await application.handle_ops_get("/ops/snapshots/summary", "")
        queues = await application.handle_ops_get("/ops/queues/summary", "")
        jobs = await application.handle_ops_get("/ops/jobs/runs", "limit=5")

        self.assertEqual(health, ApiResponse(status_code=200, payload={"database_ok": True}))
        self.assertEqual(snapshots, ApiResponse(status_code=200, payload={"raw_snapshots": 12}))
        self.assertEqual(queues, ApiResponse(status_code=200, payload={"pending_total": 4}))
        self.assertEqual(jobs, ApiResponse(status_code=200, payload={"jobRuns": []}))
        self.assertEqual(
            calls,
            [
                ("health", None),
                ("snapshots", None),
                ("queues", None),
                ("jobs", 5),
            ],
        )

    async def test_handle_ops_get_rejects_unknown_operations_route(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)

        response = await application.handle_ops_get("/ops/does-not-exist", "")

        self.assertEqual(response.status_code, 404)
        self.assertIn("Route is not registered", response.payload["error"])


class LocalApiNormalizedFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_handle_api_get_uses_normalized_category_fallback_when_snapshot_missing(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()

        async def fake_snapshot(route, path, raw_query, path_params):
            return None

        async def fake_normalized(route, path, raw_query, path_params):
            if path == "/api/v1/sport/baseball/categories":
                return {
                    "categories": [
                        {
                            "id": 42,
                            "slug": "usa",
                            "name": "USA",
                            "sport": {"id": 5, "slug": "baseball", "name": "Baseball"},
                        }
                    ]
                }
            return None

        application._fetch_snapshot_payload = fake_snapshot
        application._fetch_normalized_payload = fake_normalized

        response = await application.handle_api_get("/api/v1/sport/baseball/categories", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload["categories"][0]["sport"]["slug"], "baseball")

    async def test_handle_api_get_keeps_contract_404_when_no_snapshot_or_normalized_data(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()

        async def fake_snapshot(route, path, raw_query, path_params):
            return None

        async def fake_normalized(route, path, raw_query, path_params):
            return None

        application._fetch_snapshot_payload = fake_snapshot
        application._fetch_normalized_payload = fake_normalized

        response = await application.handle_api_get("/api/v1/sport/baseball/categories", "")

        self.assertEqual(response.status_code, 404)
        self.assertEqual(response.payload["endpointPattern"], "/api/v1/sport/baseball/categories")


if __name__ == "__main__":
    unittest.main()
