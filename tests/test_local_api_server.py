from __future__ import annotations

import unittest

from schema_inspector.local_api_server import (
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


if __name__ == "__main__":
    unittest.main()
