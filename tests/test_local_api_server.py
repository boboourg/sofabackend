from __future__ import annotations

from dataclasses import replace
import concurrent.futures
import orjson
import unittest
from unittest.mock import AsyncMock, patch
import threading
from types import SimpleNamespace
from unittest import mock

from schema_inspector.local_api_server import (
    ApiResponse,
    LocalApiApplication,
    SerializedApiResponse,
    _QUEUE_GROUPS,
    _compile_path_template,
    _decode_snapshot_payload,
    _extract_event_id_from_entity_root_path,
    _wrap_stripped_entity_payload,
    _normalized_query_map,
    _parse_context_value,
    _json_dumps_bytes,
    _synthesize_event_root_payload,
    _synthesize_manager_root_payload,
    _synthesize_player_root_payload,
    _synthesize_team_root_payload,
    _synthesize_unique_tournament_root_payload,
    build_route_specs,
    create_asgi_app,
    match_route,
)
from schema_inspector.local_swagger_builder import SwaggerDataSummary


def _swagger_summary(generated_at: str) -> SwaggerDataSummary:
    return SwaggerDataSummary(
        generated_at=generated_at,
        table_counts={},
        snapshot_counts={},
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
        self.assertIn("/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/rounds", patterns)
        self.assertIn("/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/cuptrees", patterns)
        self.assertIn("/api/v1/sport/handball/scheduled-events/{date}", patterns)
        self.assertIn("/api/v1/sport/esports/events/live", patterns)
        self.assertIn("/api/v1/event/{event_id}/innings", patterns)
        self.assertIn("/api/v1/event/{event_id}/atbat/{at_bat_id}/pitches", patterns)
        self.assertIn("/api/v1/event/{event_id}/shotmap", patterns)
        self.assertIn("/api/v1/event/{event_id}/esports-games", patterns)
        self.assertIn("/api/v1/sport/0/event-count", patterns)
        self.assertIn("/api/v1/unique-tournament/{unique_tournament_id}/media", patterns)

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

        async def fake_snapshots(*, detail: bool = False) -> dict[str, object]:
            calls.append(("snapshots", "detail" if detail else None))
            return {"raw_snapshots": 12, "detail": detail}

        async def fake_queues() -> dict[str, object]:
            calls.append(("queues", None))
            return {"pending_total": 4}

        async def fake_jobs(limit: int) -> dict[str, object]:
            calls.append(("jobs", limit))
            return {"jobRuns": []}

        async def fake_coverage() -> dict[str, object]:
            calls.append(("coverage", None))
            return {"coverage": []}

        application._fetch_ops_health_payload = fake_health
        application._fetch_ops_snapshots_summary_payload = fake_snapshots
        application._fetch_ops_queue_summary_payload = fake_queues
        application._fetch_ops_job_runs_payload = fake_jobs
        application._fetch_ops_coverage_summary_payload = fake_coverage

        health = await application.handle_ops_get("/ops/health", "")
        snapshots = await application.handle_ops_get("/ops/snapshots/summary", "")
        queues = await application.handle_ops_get("/ops/queues/summary", "")
        jobs = await application.handle_ops_get("/ops/jobs/runs", "limit=5")
        coverage = await application.handle_ops_get("/ops/coverage/summary", "")

        self.assertEqual(health, ApiResponse(status_code=200, payload={"database_ok": True}))
        self.assertEqual(
            snapshots,
            ApiResponse(status_code=200, payload={"raw_snapshots": 12, "detail": False}),
        )
        self.assertEqual(queues, ApiResponse(status_code=200, payload={"pending_total": 4}))
        self.assertEqual(jobs, ApiResponse(status_code=200, payload={"jobRuns": []}))
        self.assertEqual(coverage, ApiResponse(status_code=200, payload={"coverage": []}))
        self.assertEqual(
            calls,
            [
                ("health", None),
                ("snapshots", None),
                ("queues", None),
                ("jobs", 5),
                ("coverage", None),
            ],
        )

    async def test_handle_ops_get_rejects_unknown_operations_route(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)

        response = await application.handle_ops_get("/ops/does-not-exist", "")

        self.assertEqual(response.status_code, 404)
        self.assertIn("Route is not registered", response.payload["error"])

    async def test_queue_summary_tracks_historical_streams(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.live_state_store = None
        application.redis_backend = None
        application.stream_queue = _FakePendingQueue()

        payload = await application._fetch_ops_queue_summary_payload()

        stream_names = {item["stream"] for item in payload["streams"]}
        self.assertIn("stream:etl:historical_discovery", stream_names)
        self.assertIn("stream:etl:historical_hydrate", stream_names)
        self.assertIn("stream:etl:historical_tournament", stream_names)
        self.assertIn("stream:etl:historical_enrichment", stream_names)
        self.assertIn("stream:etl:historical_maintenance", stream_names)
        self.assertIn("stream:etl:live_discovery", stream_names)
        self.assertGreaterEqual(len(_QUEUE_GROUPS), 8)
        first_stream = payload["streams"][0]
        self.assertIn("length", first_stream)
        self.assertIn("lag", first_stream)
        self.assertIn("group_consumers", first_stream)

    async def test_fetch_ops_coverage_summary_groups_rows(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeCoverageConnection(
            [
                {
                    "source_slug": "sofascore",
                    "sport_slug": "football",
                    "surface_name": "season_structure",
                    "freshness_status": "fresh",
                    "tracked_scopes": 325,
                }
            ]
        )
        application._connect = _make_fake_connector(connection)

        payload = await application._fetch_ops_coverage_summary_payload()

        self.assertEqual(
            payload,
            {
                "coverage": [
                    {
                        "source_slug": "sofascore",
                        "sport_slug": "football",
                        "surface_name": "season_structure",
                        "freshness_status": "fresh",
                        "tracked_scopes": 325,
                    }
                ]
            },
        )

    async def test_handle_api_get_computes_sport_zero_event_count_from_database(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        connection = _FakeCoverageConnection(
            [
                {"sport_slug": "football", "live_events": 4, "total_events": 35},
                {"sport_slug": "ice-hockey", "live_events": 0, "total_events": 34},
            ]
        )
        application._connect = _make_fake_connector(connection)

        response = await application.handle_api_get("/api/v1/sport/0/event-count", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.payload,
            {
                "football": {"live": 4, "total": 35},
                "ice-hockey": {"live": 0, "total": 34},
            },
        )
        self.assertTrue(connection.closed)

    async def test_handle_api_get_rebuilds_team_player_statistics_seasons_from_normalized_tables(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeStatisticsSeasonsConnection(
            season_rows=[
                {
                    "unique_tournament_id": 17,
                    "unique_tournament_slug": "premier-league",
                    "unique_tournament_name": "Premier League",
                    "category_id": 1,
                    "category_slug": "england",
                    "category_name": "England",
                    "sport_id": 1,
                    "sport_slug": "football",
                    "sport_name": "Football",
                    "season_id": 76986,
                    "season_name": "Premier League 25/26",
                    "season_year": "25/26",
                    "all_time_season_id": None,
                }
            ],
            type_rows=[
                {
                    "unique_tournament_id": 17,
                    "season_id": 76986,
                    "stat_type": "overall",
                },
                {
                    "unique_tournament_id": 17,
                    "season_id": 76986,
                    "stat_type": "home",
                },
            ],
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get("/api/v1/team/41/player-statistics/seasons", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.payload["typesMap"],
            {"17": {"76986": ["home", "overall"]}},
        )
        self.assertEqual(response.payload["uniqueTournamentSeasons"][0]["uniqueTournament"]["id"], 17)
        self.assertEqual(response.payload["uniqueTournamentSeasons"][0]["seasons"][0]["id"], 76986)
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_event_player_statistics_without_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeNormalizedFallbackConnection(
            event_player_statistics=[
                {
                    "event_id": 44801,
                    "player_id": 292,
                    "team_id": 10,
                    "position": "F",
                    "rating": 7.4,
                    "rating_original": 7.35,
                    "rating_alternative": 7.5,
                    "statistics_type": "overall",
                    "sport_slug": "football",
                    "extra_json": {"shirtNumber": 9},
                    "player_slug": "sample-player",
                    "player_name": "Sample Player",
                    "player_short_name": "S. Player",
                    "team_slug": "sample-team",
                    "team_name": "Sample Team",
                    "team_short_name": "Sample",
                }
            ],
            event_player_stat_values=[
                {
                    "stat_name": "goals",
                    "stat_value_numeric": 2,
                    "stat_value_text": None,
                    "stat_value_json": None,
                },
                {
                    "stat_name": "accuratePass",
                    "stat_value_numeric": None,
                    "stat_value_text": "18/20",
                    "stat_value_json": None,
                },
            ],
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get("/api/v1/event/44801/player/292/statistics", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload["player"], {"id": 292, "slug": "sample-player", "name": "Sample Player", "shortName": "S. Player"})
        self.assertEqual(response.payload["team"]["id"], 10)
        self.assertEqual(response.payload["position"], "F")
        self.assertEqual(response.payload["statistics"]["rating"], 7.4)
        self.assertEqual(response.payload["statistics"]["ratingVersions"], {"original": 7.35, "alternative": 7.5})
        self.assertEqual(response.payload["statistics"]["statisticsType"], {"statisticsType": "overall", "sportSlug": "football"})
        self.assertEqual(response.payload["statistics"]["goals"], 2)
        self.assertEqual(response.payload["statistics"]["accuratePass"], "18/20")
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_rating_breakdown_without_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeNormalizedFallbackConnection(
            rating_breakdown=[
                {
                    "action_group": "passes",
                    "ordinal": 0,
                    "event_action_type": "pass",
                    "is_home": True,
                    "keypass": False,
                    "outcome": True,
                    "start_x": 12.5,
                    "start_y": 44.0,
                    "end_x": 55.0,
                    "end_y": 12.0,
                }
            ]
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get("/api/v1/event/13873923/player/10710/rating-breakdown", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload["passes"][0]["eventActionType"], "pass")
        self.assertEqual(response.payload["passes"][0]["playerCoordinates"], {"x": 12.5, "y": 44.0})
        self.assertEqual(response.payload["passes"][0]["passEndCoordinates"], {"x": 55.0, "y": 12.0})
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_team_heatmap_without_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeNormalizedFallbackConnection(
            heatmap_points=[
                {"point_type": "player", "ordinal": 0, "x": 1.25, "y": 2.5},
                {"point_type": "goalkeeper", "ordinal": 0, "x": 9.5, "y": 8.5},
            ]
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get("/api/v1/event/94957/heatmap/4704", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload, {"playerPoints": [{"x": 1.25, "y": 2.5}], "goalkeeperPoints": [{"x": 9.5, "y": 8.5}]})
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_top_players_without_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeNormalizedFallbackConnection(
            top_player_snapshot={
                "id": 500,
                "statistics_type": {"name": "overall"},
            },
            top_player_entries=[
                {
                    "metric_name": "goals",
                    "ordinal": 0,
                    "player_id": 292,
                    "player_slug": "sample-player",
                    "player_name": "Sample Player",
                    "player_short_name": "S. Player",
                    "team_id": 10,
                    "team_slug": "sample-team",
                    "team_name": "Sample Team",
                    "team_short_name": "Sample",
                    "event_id": None,
                    "played_enough": True,
                    "statistic": 12,
                    "statistics_id": 900,
                    "statistics_payload": {"goals": 12},
                }
            ],
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get("/api/v1/unique-tournament/17/season/76986/top-players/overall", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload["statisticsType"], {"name": "overall"})
        self.assertEqual(response.payload["topPlayers"]["goals"][0]["player"]["id"], 292)
        self.assertEqual(response.payload["topPlayers"]["goals"][0]["team"]["id"], 10)
        self.assertEqual(response.payload["topPlayers"]["goals"][0]["statistics"], {"goals": 12})
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_standings_with_rows_without_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeStandingsConnection(
            standings=[
                {
                    "id": 224907,
                    "season_id": 77559,
                    "tournament_id": 80,
                    "name": "LaLiga",
                    "type": "total",
                    "updated_at_timestamp": 1770000123,
                    "tie_breaking_rule_id": 7,
                    "tie_breaking_rule_text": "Points, goal difference, goals scored",
                    "descriptions": [{"text": "Champions League"}],
                }
            ],
            rows=[
                {
                    "id": 10001,
                    "standing_id": 224907,
                    "team_id": 2817,
                    "team_slug": "barcelona",
                    "team_name": "Barcelona",
                    "team_short_name": "Barcelona",
                    "position": 1,
                    "matches": 33,
                    "wins": 28,
                    "draws": 1,
                    "losses": 4,
                    "points": 85,
                    "scores_for": 87,
                    "scores_against": 30,
                    "score_diff_formatted": "+57",
                    "promotion_id": 1,
                    "promotion_text": "Champions League",
                    "descriptions": [{"text": "Qualified"}],
                },
                {
                    "id": 10002,
                    "standing_id": 224907,
                    "team_id": 2829,
                    "team_slug": "real-madrid",
                    "team_name": "Real Madrid",
                    "team_short_name": "Real Madrid",
                    "position": 2,
                    "matches": 33,
                    "wins": 23,
                    "draws": 5,
                    "losses": 5,
                    "points": 74,
                    "scores_for": 68,
                    "scores_against": 31,
                    "score_diff_formatted": "+37",
                    "promotion_id": None,
                    "promotion_text": None,
                    "descriptions": None,
                },
            ],
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/8/season/77559/standings/total",
            "",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.payload["standings"]), 1)
        standing = response.payload["standings"][0]
        self.assertEqual(standing["id"], 224907)
        self.assertEqual(standing["type"], "total")
        self.assertEqual(standing["tieBreakingRule"], {"id": 7, "text": "Points, goal difference, goals scored"})
        self.assertEqual(len(standing["rows"]), 2)
        self.assertEqual(standing["rows"][0]["team"], {"id": 2817, "slug": "barcelona", "name": "Barcelona", "shortName": "Barcelona"})
        self.assertEqual(standing["rows"][0]["position"], 1)
        self.assertEqual(standing["rows"][0]["matches"], 33)
        self.assertEqual(standing["rows"][0]["wins"], 28)
        self.assertEqual(standing["rows"][0]["draws"], 1)
        self.assertEqual(standing["rows"][0]["losses"], 4)
        self.assertEqual(standing["rows"][0]["scoresFor"], 87)
        self.assertEqual(standing["rows"][0]["scoresAgainst"], 30)
        self.assertEqual(standing["rows"][0]["scoreDiffFormatted"], "+57")
        self.assertEqual(standing["rows"][0]["points"], 85)
        self.assertEqual(standing["rows"][0]["promotion"], {"id": 1, "text": "Champions League"})
        self.assertEqual(standing["rows"][0]["descriptions"], [{"text": "Qualified"}])
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_standings_when_snapshot_is_missing_rows(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection(
            [
                {
                    "source_slug": "sofascore",
                    "source_url": "https://www.sofascore.com/api/v1/unique-tournament/8/season/77559/standings/total",
                    "payload": {"standings": [{"id": 224907, "type": "total"}]},
                }
            ]
        )
        normalized_connection = _FakeStandingsConnection(
            standings=[
                {
                    "id": 224907,
                    "season_id": 77559,
                    "tournament_id": 80,
                    "name": "LaLiga",
                    "type": "total",
                    "updated_at_timestamp": None,
                    "tie_breaking_rule_id": None,
                    "tie_breaking_rule_text": None,
                    "descriptions": None,
                }
            ],
            rows=[
                {
                    "id": 10001,
                    "standing_id": 224907,
                    "team_id": 2817,
                    "team_slug": "barcelona",
                    "team_name": "Barcelona",
                    "team_short_name": "Barcelona",
                    "position": 1,
                    "matches": 33,
                    "wins": 28,
                    "draws": 1,
                    "losses": 4,
                    "points": 85,
                    "scores_for": 87,
                    "scores_against": 30,
                    "score_diff_formatted": "+57",
                    "promotion_id": None,
                    "promotion_text": None,
                    "descriptions": None,
                }
            ],
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/8/season/77559/standings/total",
            "",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload["standings"][0]["rows"][0]["team"]["id"], 2817)
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_uses_generic_normalized_fallback_for_remaining_source_table_routes(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeGenericNormalizedConnection(
            table_columns={"event_graph": {"event_id", "minute", "value"}},
            table_rows={
                "event_graph": [
                    {
                        "event_id": 14083191,
                        "minute": 53,
                        "value": 42,
                    }
                ]
            },
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get("/api/v1/event/14083191/graph", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.payload,
            {
                "graphPoints": [
                    {
                        "eventId": 14083191,
                        "minute": 53,
                        "value": 42,
                    }
                ]
            },
        )
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_transfer_history_with_nested_entities_without_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeReportNormalizedConnection(
            transfer_history=[
                {
                    "id": 2467532,
                    "player_id": 233338,
                    "player_slug": "sample-player",
                    "player_name": "Sample Player",
                    "player_short_name": "S. Player",
                    "player_position": "F",
                    "transfer_from_team_id": 10,
                    "from_team_slug": "old-club",
                    "from_team_entity_name": "Old Club",
                    "from_team_name": "Old Club",
                    "from_team_short_name": "Old",
                    "transfer_to_team_id": 20,
                    "to_team_slug": "new-club",
                    "to_team_entity_name": "New Club",
                    "to_team_name": "New Club",
                    "to_team_short_name": "New",
                    "transfer_date_timestamp": 1711929600,
                    "transfer_fee": 0,
                    "transfer_fee_description": "Free transfer",
                    "transfer_fee_raw": {"value": 0, "currency": "EUR"},
                    "type": 3,
                }
            ],
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get("/api/v1/player/233338/transfer-history", "")

        self.assertEqual(response.status_code, 200)
        transfer = response.payload["transferHistory"][0]
        self.assertEqual(transfer["player"], {"id": 233338, "slug": "sample-player", "name": "Sample Player", "shortName": "S. Player", "position": "F"})
        self.assertEqual(transfer["transferFrom"], {"id": 10, "slug": "old-club", "name": "Old Club", "shortName": "Old"})
        self.assertEqual(transfer["transferTo"], {"id": 20, "slug": "new-club", "name": "New Club", "shortName": "New"})
        self.assertEqual(transfer["fromTeamName"], "Old Club")
        self.assertEqual(transfer["toTeamName"], "New Club")
        self.assertEqual(transfer["transferFeeRaw"], {"value": 0, "currency": "EUR"})
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_comments_with_feed_colors_without_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeReportNormalizedConnection(
            comment_feed={
                "event_id": 14025026,
                "home_player_color": {"primary": "#ffffff"},
                "home_goalkeeper_color": {"primary": "#111111"},
                "away_player_color": {"primary": "#222222"},
                "away_goalkeeper_color": {"primary": "#333333"},
            },
            comments=[
                {
                    "comment_id": 37184719,
                    "sequence": 0,
                    "period_name": "2ND",
                    "is_home": False,
                    "player_id": 829932,
                    "player_slug": "cristian-romero",
                    "player_name": "Cristian Romero",
                    "player_short_name": "C. Romero",
                    "player_position": "D",
                    "text": "Second Half begins.",
                    "match_time": 46,
                    "comment_type": "matchStarted",
                }
            ],
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get("/api/v1/event/14025026/comments", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload["home"]["playerColor"], {"primary": "#ffffff"})
        self.assertEqual(response.payload["away"]["goalkeeperColor"], {"primary": "#333333"})
        self.assertEqual(response.payload["comments"][0]["id"], 37184719)
        self.assertEqual(response.payload["comments"][0]["player"]["id"], 829932)
        self.assertEqual(response.payload["comments"][0]["type"], "matchStarted")
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_h2h_duels_without_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeReportNormalizedConnection(
            duels=[
                {"duel_type": "team", "home_wins": 5, "away_wins": 3, "draws": 1},
                {"duel_type": "manager", "home_wins": 1, "away_wins": 2, "draws": 0},
            ],
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get("/api/v1/event/14025029/h2h", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload["teamDuel"], {"homeWins": 5, "awayWins": 3, "draws": 1})
        self.assertEqual(response.payload["managerDuel"], {"homeWins": 1, "awayWins": 2, "draws": 0})
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_managers_without_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeReportNormalizedConnection(
            managers=[
                {
                    "side": "home",
                    "manager_id": 5878,
                    "manager_slug": "nuno-espirito-santo",
                    "manager_name": "Nuno Espirito Santo",
                    "manager_short_name": "N. E. Santo",
                    "field_translations": {"nameTranslation": {"ar": "nuno"}},
                },
                {
                    "side": "away",
                    "manager_id": 787556,
                    "manager_slug": "rob-edwards",
                    "manager_name": "Rob Edwards",
                    "manager_short_name": "R. Edwards",
                    "field_translations": None,
                },
            ],
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get("/api/v1/event/14025029/managers", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload["homeManager"]["id"], 5878)
        self.assertEqual(response.payload["homeManager"]["fieldTranslations"], {"nameTranslation": {"ar": "nuno"}})
        self.assertEqual(response.payload["awayManager"]["slug"], "rob-edwards")
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_pregame_form_without_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeReportNormalizedConnection(
            pregame_root={"event_id": 14025029, "label": "Pts"},
            pregame_sides=[
                {"side": "home", "avg_rating": "6.73", "position": 18, "value": "29"},
                {"side": "away", "avg_rating": "6.68", "position": 20, "value": "17"},
            ],
            pregame_items=[
                {"side": "home", "ordinal": 0, "form_value": "W"},
                {"side": "home", "ordinal": 1, "form_value": "D"},
                {"side": "away", "ordinal": 0, "form_value": "L"},
            ],
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get("/api/v1/event/14025029/pregame-form", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload["label"], "Pts")
        self.assertEqual(response.payload["homeTeam"], {"avgRating": "6.73", "position": 18, "value": "29", "form": ["W", "D"]})
        self.assertEqual(response.payload["awayTeam"]["form"], ["L"])
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_rounds_without_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeReportNormalizedConnection(
            rounds=[
                {"round_number": 1, "round_name": None, "round_slug": None, "round_prefix": None, "is_current": False},
                {"round_number": 32, "round_name": None, "round_slug": None, "round_prefix": None, "is_current": True},
            ],
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get("/api/v1/unique-tournament/17/season/76986/rounds", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload["currentRound"], {"round": 32})
        self.assertEqual(response.payload["rounds"], [{"round": 1}, {"round": 32}])
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_cup_rounds_with_name_slug_prefix(self) -> None:
        # Cup tournament rounds carry name/slug and optionally prefix
        # (e.g. {"round": 636, "name": "Playoff round",
        #        "slug": "playoff-round", "prefix": "Qualification"}).
        # League rounds remain bare {"round": N} via the test above.
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeReportNormalizedConnection(
            rounds=[
                {
                    "round_number": 1,
                    "round_name": "Qualification Round 1",
                    "round_slug": "qualification-round-1",
                    "round_prefix": None,
                    "is_current": False,
                },
                {
                    "round_number": 636,
                    "round_name": "Playoff round",
                    "round_slug": "playoff-round",
                    "round_prefix": "Qualification",
                    "is_current": False,
                },
                {
                    "round_number": 28,
                    "round_name": "Semifinals",
                    "round_slug": "semifinals",
                    "round_prefix": None,
                    "is_current": True,
                },
            ],
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/7/season/76953/rounds", ""
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.payload["currentRound"],
            {"round": 28, "name": "Semifinals", "slug": "semifinals"},
        )
        # Round 1 has name+slug but no prefix
        self.assertEqual(
            response.payload["rounds"][0],
            {"round": 1, "name": "Qualification Round 1", "slug": "qualification-round-1"},
        )
        # Round 636 includes the prefix field
        self.assertEqual(
            response.payload["rounds"][1],
            {
                "round": 636,
                "name": "Playoff round",
                "slug": "playoff-round",
                "prefix": "Qualification",
            },
        )

    async def test_handle_api_get_rebuilds_season_next_events_with_pagination(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeSeasonEventsConnection(
            rows=[
                {
                    "id": 1001,
                    "slug": "sample-home-sample-away",
                    "custom_id": "abc",
                    "start_timestamp": 1770000000,
                    "status_code": 0,
                    "status_type": "notstarted",
                    "status_description": "Not started",
                    "home_team_id": 10,
                    "home_team_slug": "sample-home",
                    "home_team_name": "Sample Home",
                    "home_team_short_name": "Home",
                    "away_team_id": 11,
                    "away_team_slug": "sample-away",
                    "away_team_name": "Sample Away",
                    "away_team_short_name": "Away",
                    "tournament_id": 20,
                    "tournament_slug": "laliga",
                    "tournament_name": "LaLiga",
                    "unique_tournament_id": 8,
                    "unique_tournament_slug": "laliga",
                    "unique_tournament_name": "LaLiga",
                    "season_id": 77559,
                    "season_name": "LaLiga 25/26",
                    "season_year": "25/26",
                }
            ],
            has_extra=True,
        )
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/8/season/77559/events/next/1",
            "",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload["hasNextPage"], True)
        self.assertEqual(len(response.payload["events"]), 30)
        event = response.payload["events"][0]
        self.assertEqual(event["id"], 1001)
        self.assertEqual(event["status"], {"code": 0, "type": "notstarted", "description": "Not started"})
        # Synthesizer emits the full Sofascore-shape team object; pin id +
        # name + slug fields instead of exact-dict equality so we don't
        # have to enumerate every nested key.
        self.assertEqual(event["homeTeam"]["id"], 10)
        self.assertEqual(event["homeTeam"]["name"], "Sample Home")
        self.assertEqual(event["homeTeam"]["slug"], "sample-home")
        self.assertEqual(event["awayTeam"]["id"], 11)
        self.assertEqual(event["tournament"]["uniqueTournament"]["id"], 8)
        self.assertEqual(event["season"]["id"], 77559)
        # SQL has been promoted to the synthesizer's shared query; the
        # discriminating WHERE clause is "st.type = 'notstarted'" (st is
        # the event_status alias).
        self.assertIn("st.type = 'notstarted'", normalized_connection.fetch_calls[0][0])
        self.assertEqual(normalized_connection.fetch_calls[0][1], (8, 77559, 30, 31))
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_rebuilds_season_last_events_with_pagination(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        normalized_connection = _FakeSeasonEventsConnection(rows=[], has_extra=False)
        application._connect = _make_sequence_connector([snapshot_connection, normalized_connection])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/8/season/77559/events/last/2",
            "",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload, {"events": [], "hasNextPage": False})
        self.assertIn("st.type IN", normalized_connection.fetch_calls[0][0])
        self.assertIn("ORDER BY e.start_timestamp DESC", normalized_connection.fetch_calls[0][0])
        self.assertEqual(normalized_connection.fetch_calls[0][1], (8, 77559, 60, 31))
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(normalized_connection.closed)

    async def test_handle_api_get_serves_season_events_raw_snapshot_with_live_overlay(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        connection = _FakeEventListSnapshotConnection(
            rows=[
                {
                    "source_url": "https://www.sofascore.com/api/v1/unique-tournament/17/season/76986/events/last/0",
                    "source_slug": "sofascore",
                    "payload": {
                        "events": [
                            {
                                "id": 14023940,
                                "customId": "gsJ",
                                "status": {"code": 0, "type": "notstarted", "description": "Not started"},
                                "homeScore": {},
                                "awayScore": {},
                                "eventState": {"statusIndicator": "raw"},
                                "venue": {"id": 2223, "name": "Raw Venue"},
                            }
                        ],
                        "hasNextPage": True,
                    },
                }
            ],
            surface_rows=[
                {
                    "event_id": 14023940,
                    "custom_id": "gsJ",
                    "status_code": 100,
                    "status_type": "finished",
                    "status_description": "Ended",
                    "winner_code": 1,
                    "scores": {
                        "home": {"current": 3, "display": 3, "period1": 1, "period2": 2, "normaltime": 3},
                        "away": {"current": 1, "display": 1, "period1": 0, "period2": 1, "normaltime": 1},
                    },
                    "time_payload": {"currentPeriodStartTimestamp": 1777665748},
                    "changes_payload": {"changes": ["homeScore.current"], "changeTimestamp": 1777667000},
                    "event_filters_payload": {"level": ["top-competitions"]},
                    "has_xg": True,
                }
            ],
        )
        application._connect = _make_fake_connector(connection)

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/17/season/76986/events/last/0",
            "",
        )

        self.assertEqual(response.status_code, 200)
        event = response.payload["events"][0]
        self.assertEqual(event["status"], {"code": 100, "type": "finished", "description": "Ended"})
        self.assertEqual(event["homeScore"]["current"], 3)
        self.assertEqual(event["awayScore"]["current"], 1)
        self.assertEqual(event["time"], {"currentPeriodStartTimestamp": 1777665748})
        self.assertEqual(event["changes"], {"changes": ["homeScore.current"], "changeTimestamp": 1777667000})
        self.assertEqual(event["eventFilters"], {"level": ["top-competitions"]})
        self.assertEqual(event["hasXg"], True)
        self.assertEqual(event["eventState"], {"statusIndicator": "raw"})
        self.assertEqual(event["venue"], {"id": 2223, "name": "Raw Venue"})
        self.assertTrue(connection.closed)

    async def test_handle_api_get_serves_team_events_raw_snapshot_with_live_overlay(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        connection = _FakeEventListSnapshotConnection(
            rows=[
                {
                    "source_url": "https://www.sofascore.com/api/v1/team/2817/events/last/0",
                    "source_slug": "sofascore",
                    "payload": {
                        "events": [
                            {
                                "id": 14083245,
                                "customId": "team-match",
                                "status": {"code": 0, "type": "notstarted", "description": "Not started"},
                                "homeScore": {},
                                "awayScore": {},
                            }
                        ],
                        "hasNextPage": False,
                    },
                }
            ],
            surface_rows=[
                {
                    "event_id": 14083245,
                    "custom_id": "team-match",
                    "status_code": 100,
                    "status_type": "finished",
                    "status_description": "Ended",
                    "scores": {
                        "home": {"current": 0, "display": 0},
                        "away": {"current": 2, "display": 2},
                    },
                }
            ],
        )
        application._connect = _make_fake_connector(connection)

        response = await application.handle_api_get("/api/v1/team/2817/events/last/0", "")

        self.assertEqual(response.status_code, 200)
        event = response.payload["events"][0]
        self.assertEqual(event["status"]["type"], "finished")
        self.assertEqual(event["homeScore"], {"current": 0, "display": 0})
        self.assertEqual(event["awayScore"], {"current": 2, "display": 2})
        self.assertTrue(connection.closed)

    async def test_fetch_ops_health_payload_keeps_drift_coverage_and_alert_summaries_in_payload(self) -> None:
        from schema_inspector.ops.health import (
            CoverageAlert,
            CoverageAlertSummary,
            CoverageSummary,
            DriftFlag,
            DriftSummary,
            HealthReport,
            ReconcilePolicySourceEntry,
            ReconcilePolicySummary,
        )

        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeCoverageConnection([])
        application._connect = _make_fake_connector(connection)
        application.live_state_store = None
        application.redis_backend = None

        async def fake_collect_health_report(*, sql_executor, live_state_store=None, redis_backend=None, stream_queue=None):
            self.assertIs(sql_executor, connection)
            self.assertIsNone(live_state_store)
            self.assertIsNone(redis_backend)
            self.assertIsNone(stream_queue)
            return HealthReport(
                snapshot_count=7,
                capability_rollup_count=3,
                live_hot_count=0,
                live_warm_count=0,
                live_cold_count=0,
                database_ok=True,
                redis_ok=False,
                redis_backend_kind="none",
                drift_summary=DriftSummary(
                    flag_count=1,
                    flags=(
                        DriftFlag(
                            surface="sport_live_events",
                            sport_slug="football",
                            reason="snapshot_older_than_terminal_state",
                        ),
                    ),
                ),
                coverage_summary=CoverageSummary(
                    tracked_scope_count=12,
                    fresh_scope_count=8,
                    stale_scope_count=3,
                    other_scope_count=1,
                    source_count=2,
                    sport_count=3,
                    surface_count=4,
                    avg_completeness_ratio=0.625,
                ),
                coverage_alert_summary=CoverageAlertSummary(
                    flag_count=1,
                    flags=(
                        CoverageAlert(
                            severity="warning",
                            reason="stale_coverage_scopes_present",
                            stale_scope_count=3,
                        ),
                    ),
                ),
                reconcile_policy_summary=ReconcilePolicySummary(
                    policy_enabled=True,
                    primary_source_slug="sofascore",
                    source_count=2,
                    sources=(
                        ReconcilePolicySourceEntry(source_slug="sofascore", priority=100),
                        ReconcilePolicySourceEntry(source_slug="secondary_source", priority=80),
                    ),
                ),
            )

        import schema_inspector.local_api_server as local_api_server

        original = local_api_server.collect_health_report
        local_api_server.collect_health_report = fake_collect_health_report
        try:
            payload = await application._fetch_ops_health_payload()
        finally:
            local_api_server.collect_health_report = original

        self.assertEqual(payload["drift_summary"]["flag_count"], 1)
        self.assertEqual(payload["drift_summary"]["flags"][0]["surface"], "sport_live_events")
        self.assertEqual(payload["drift_summary"]["flags"][0]["sport_slug"], "football")
        self.assertEqual(payload["coverage_summary"]["tracked_scope_count"], 12)
        self.assertEqual(payload["coverage_summary"]["stale_scope_count"], 3)
        self.assertAlmostEqual(payload["coverage_summary"]["avg_completeness_ratio"], 0.625)
        self.assertEqual(payload["coverage_alert_summary"]["flag_count"], 1)
        self.assertEqual(payload["coverage_alert_summary"]["flags"][0]["severity"], "warning")
        self.assertEqual(payload["coverage_alert_summary"]["flags"][0]["stale_scope_count"], 3)
        self.assertTrue(payload["reconcile_policy_summary"]["policy_enabled"])
        self.assertEqual(payload["reconcile_policy_summary"]["primary_source_slug"], "sofascore")
        self.assertEqual(payload["reconcile_policy_summary"]["source_count"], 2)
        self.assertEqual(payload["reconcile_policy_summary"]["sources"][0]["source_slug"], "sofascore")


class LocalApiUniqueTournamentSeasonsTests(unittest.IsolatedAsyncioTestCase):
    """Coverage for ``/api/v1/unique-tournament/{id}/seasons`` reconstruction.

    Regression guard: prior to 2026-05-15 this endpoint fell through to the
    generic normalized fallback which serialized rows from the
    ``unique_tournament_season`` junction table directly, producing
    ``{"seasons": [{"seasonId":..., "uniqueTournamentId":...}]}`` — wrong
    shape, breaks frontend rendering. The dedicated handler now JOINs
    through to ``season`` and produces the proper Sofascore shape:
    ``{"seasons": [{"id":..., "name":..., "year":..., "editor":bool, ...}]}``.
    """

    @staticmethod
    def _season_row(
        *,
        season_id: int,
        name: str,
        year: str,
        editor: bool = False,
        season_coverage_info: dict | None = None,
    ) -> dict[str, object]:
        return {
            "id": season_id,
            "name": name,
            "year": year,
            "editor": editor,
            "season_coverage_info": season_coverage_info,
        }

    async def test_returns_sofascore_shape_with_real_season_data(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        connection = _FakeSeasonsConnection(
            rows=[
                self._season_row(season_id=77559, name="LaLiga 25/26", year="25/26"),
                self._season_row(season_id=61643, name="LaLiga 24/25", year="24/25"),
            ]
        )
        application._connect = _make_fake_connector(connection)

        payload = await application._fetch_unique_tournament_seasons_payload(8)

        self.assertEqual(payload["seasons"][0]["id"], 77559)
        self.assertEqual(payload["seasons"][0]["name"], "LaLiga 25/26")
        self.assertEqual(payload["seasons"][0]["year"], "25/26")
        self.assertFalse(payload["seasons"][0]["editor"])
        # Confirm regression: NO "seasonId" or "uniqueTournamentId" keys
        for entry in payload["seasons"]:
            self.assertNotIn("seasonId", entry)
            self.assertNotIn("uniqueTournamentId", entry)
        self.assertTrue(connection.closed)

    async def test_empty_tournament_returns_empty_seasons_array(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        connection = _FakeSeasonsConnection(rows=[])
        application._connect = _make_fake_connector(connection)

        payload = await application._fetch_unique_tournament_seasons_payload(99999)

        self.assertEqual(payload, {"seasons": []})

    async def test_null_editor_coerced_to_false(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        connection = _FakeSeasonsConnection(
            rows=[
                self._season_row(season_id=12, name="S 03/04", year="03/04", editor=False),
            ]
        )
        application._connect = _make_fake_connector(connection)

        payload = await application._fetch_unique_tournament_seasons_payload(7)

        self.assertFalse(payload["seasons"][0]["editor"])

    async def test_season_coverage_info_passthrough(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        connection = _FakeSeasonsConnection(
            rows=[
                self._season_row(
                    season_id=77559,
                    name="LaLiga 25/26",
                    year="25/26",
                    season_coverage_info={"matches": True, "lineups": True},
                ),
            ]
        )
        application._connect = _make_fake_connector(connection)

        payload = await application._fetch_unique_tournament_seasons_payload(8)

        self.assertEqual(
            payload["seasons"][0]["seasonCoverageInfo"],
            {"matches": True, "lineups": True},
        )


class _FakeSeasonsConnection:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        return self.rows

    async def close(self):
        self.closed = True


class LocalApiUniqueTournamentMediaTests(unittest.IsolatedAsyncioTestCase):
    """Coverage for ``/api/v1/unique-tournament/{id}/media`` aggregator."""

    @staticmethod
    def _media_row(
        *,
        event_id: int,
        snapshot_payload: dict,
        snapshot_fetched_at,
        start_timestamp: int = 1778678400,
        is_soft_error_payload: bool = False,
        http_status: int = 200,
        status_type: str = "finished",
        status_description: str = "Ended",
    ) -> dict[str, object]:
        return {
            "event_id": event_id,
            "snapshot_payload": snapshot_payload,
            "is_soft_error_payload": is_soft_error_payload,
            "http_status": http_status,
            "snapshot_fetched_at": snapshot_fetched_at,
            "event_slug": f"home-vs-away-{event_id}",
            "event_custom_id": f"cid-{event_id}",
            "start_timestamp": start_timestamp,
            "status_code": 100,
            "status_type": status_type,
            "status_description": status_description,
            "home_team_id": 10,
            "home_team_slug": "home",
            "home_team_name": "Home Team",
            "home_team_short_name": "Home",
            "away_team_id": 11,
            "away_team_slug": "away",
            "away_team_name": "Away Team",
            "away_team_short_name": "Away",
            "tournament_id": 20,
            "tournament_slug": "premier-league",
            "tournament_name": "Premier League",
            "unique_tournament_id": 17,
            "unique_tournament_slug": "pl",
            "unique_tournament_name": "Premier League",
            "season_id": 76986,
            "season_name": "Premier League 25/26",
            "season_year": "25/26",
        }

    async def test_returns_empty_media_when_no_highlights_exist(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        media_connection = _FakeUniqueTournamentMediaConnection([])
        application._connect = _make_sequence_connector([snapshot_connection, media_connection])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/17/media",
            "",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload, {"media": []})
        # SQL filter must pin the highlights endpoint pattern and the league id.
        sql, args = media_connection.fetch_calls[0]
        self.assertIn("/api/v1/event/{event_id}/highlights", args)
        self.assertIn(17, args)
        self.assertIn("aps.endpoint_pattern = $1", sql)
        self.assertIn("e.unique_tournament_id = $2", sql)
        self.assertIn("ORDER BY aps.context_entity_id", sql)
        self.assertTrue(snapshot_connection.closed)
        self.assertTrue(media_connection.closed)

    async def test_returns_grouped_event_and_highlights_for_known_tournament(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        from datetime import datetime as _dt, timezone as _tz

        media_connection = _FakeUniqueTournamentMediaConnection(
            [
                self._media_row(
                    event_id=15342059,
                    snapshot_payload={
                        "highlights": [
                            {
                                "id": 7428413,
                                "url": "https://www.youtube.com/watch?v=abc",
                                "title": "Goal!",
                                "subtitle": "Full Highlights",
                                "mediaType": 6,
                                "sourceUrl": "https://src.example/abc",
                                "livestream": False,
                                "keyHighlight": True,
                                "thumbnailUrl": "https://img.example/abc.jpg",
                                "createdAtTimestamp": 1778678404,
                            }
                        ]
                    },
                    snapshot_fetched_at=_dt(2026, 5, 13, 12, 0, tzinfo=_tz.utc),
                ),
            ]
        )
        application._connect = _make_sequence_connector([snapshot_connection, media_connection])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/17/media",
            "",
        )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.payload["media"]), 1)
        media_item = response.payload["media"][0]

        event_payload = media_item["event"]
        self.assertEqual(event_payload["id"], 15342059)
        self.assertEqual(event_payload["slug"], "home-vs-away-15342059")
        self.assertEqual(event_payload["startTimestamp"], 1778678400)
        self.assertEqual(
            event_payload["status"],
            {"code": 100, "type": "finished", "description": "Ended"},
        )
        self.assertEqual(
            event_payload["homeTeam"],
            {"id": 10, "slug": "home", "name": "Home Team", "shortName": "Home"},
        )
        self.assertEqual(event_payload["awayTeam"]["id"], 11)
        self.assertEqual(event_payload["tournament"]["id"], 20)
        self.assertEqual(event_payload["tournament"]["uniqueTournament"]["id"], 17)
        self.assertEqual(event_payload["uniqueTournament"]["id"], 17)
        self.assertEqual(event_payload["uniqueTournament"]["slug"], "pl")
        self.assertEqual(event_payload["season"], {"id": 76986, "name": "Premier League 25/26", "year": "25/26"})

        highlights = media_item["highlights"]
        self.assertEqual(len(highlights), 1)
        self.assertEqual(highlights[0]["id"], 7428413)
        self.assertEqual(highlights[0]["keyHighlight"], True)
        # Sort scaffolding must not leak into the response.
        self.assertNotIn("_fetched_at", media_item)
        self.assertNotIn("_start_timestamp", media_item)
        self.assertNotIn("_event_id", media_item)

    async def test_suppresses_soft_error_and_4xx_snapshots(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        from datetime import datetime as _dt, timezone as _tz

        media_connection = _FakeUniqueTournamentMediaConnection(
            [
                self._media_row(
                    event_id=100,
                    snapshot_payload={"highlights": [{"id": 1}]},
                    snapshot_fetched_at=_dt(2026, 5, 13, 9, 0, tzinfo=_tz.utc),
                    is_soft_error_payload=True,
                ),
                self._media_row(
                    event_id=200,
                    snapshot_payload={"error": {"code": 404}},
                    snapshot_fetched_at=_dt(2026, 5, 13, 10, 0, tzinfo=_tz.utc),
                    http_status=404,
                ),
                self._media_row(
                    event_id=300,
                    snapshot_payload={"highlights": [{"id": 9}]},
                    snapshot_fetched_at=_dt(2026, 5, 13, 11, 0, tzinfo=_tz.utc),
                ),
            ]
        )
        application._connect = _make_sequence_connector([snapshot_connection, media_connection])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/17/media",
            "",
        )

        self.assertEqual(response.status_code, 200)
        event_ids = [item["event"]["id"] for item in response.payload["media"]]
        self.assertEqual(event_ids, [300])

    async def test_drops_snapshots_with_empty_highlights_array(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        from datetime import datetime as _dt, timezone as _tz

        media_connection = _FakeUniqueTournamentMediaConnection(
            [
                self._media_row(
                    event_id=400,
                    snapshot_payload={"highlights": []},
                    snapshot_fetched_at=_dt(2026, 5, 13, 11, 0, tzinfo=_tz.utc),
                ),
                self._media_row(
                    event_id=500,
                    snapshot_payload={"highlights": None},
                    snapshot_fetched_at=_dt(2026, 5, 13, 11, 0, tzinfo=_tz.utc),
                ),
                self._media_row(
                    event_id=600,
                    snapshot_payload={"highlights": [{"id": 1}]},
                    snapshot_fetched_at=_dt(2026, 5, 13, 11, 0, tzinfo=_tz.utc),
                ),
            ]
        )
        application._connect = _make_sequence_connector([snapshot_connection, media_connection])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/17/media",
            "",
        )

        self.assertEqual(response.status_code, 200)
        event_ids = [item["event"]["id"] for item in response.payload["media"]]
        self.assertEqual(event_ids, [600])

    async def test_sorts_by_fetched_at_then_start_timestamp_then_event_id(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        from datetime import datetime as _dt, timezone as _tz

        latest = _dt(2026, 5, 13, 15, 0, tzinfo=_tz.utc)
        older = _dt(2026, 5, 12, 15, 0, tzinfo=_tz.utc)
        media_connection = _FakeUniqueTournamentMediaConnection(
            [
                # Older fetched_at — must be last.
                self._media_row(
                    event_id=1,
                    snapshot_payload={"highlights": [{"id": 1}]},
                    snapshot_fetched_at=older,
                    start_timestamp=1778000000,
                ),
                # Same latest fetched_at as #3, earlier start_timestamp -> after #3.
                self._media_row(
                    event_id=2,
                    snapshot_payload={"highlights": [{"id": 2}]},
                    snapshot_fetched_at=latest,
                    start_timestamp=1778000000,
                ),
                # Latest fetched_at and latest start_timestamp -> first.
                self._media_row(
                    event_id=3,
                    snapshot_payload={"highlights": [{"id": 3}]},
                    snapshot_fetched_at=latest,
                    start_timestamp=1778900000,
                ),
                # Tie-breaker on event_id when fetched_at + start_timestamp tie.
                self._media_row(
                    event_id=4,
                    snapshot_payload={"highlights": [{"id": 4}]},
                    snapshot_fetched_at=latest,
                    start_timestamp=1778000000,
                ),
            ]
        )
        application._connect = _make_sequence_connector([snapshot_connection, media_connection])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/17/media",
            "",
        )

        event_ids = [item["event"]["id"] for item in response.payload["media"]]
        self.assertEqual(event_ids, [3, 4, 2, 1])

    async def test_caps_response_at_200_events(self) -> None:
        from schema_inspector.local_api_server import _UNIQUE_TOURNAMENT_MEDIA_EVENT_LIMIT
        from datetime import datetime as _dt, timezone as _tz

        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        snapshot_connection = _FakeSnapshotConnection([])
        rows = [
            self._media_row(
                event_id=index,
                snapshot_payload={"highlights": [{"id": index}]},
                snapshot_fetched_at=_dt(2026, 5, 13, 12, 0, tzinfo=_tz.utc),
                start_timestamp=1_700_000_000 + index,
            )
            for index in range(1, 251)
        ]
        media_connection = _FakeUniqueTournamentMediaConnection(rows)
        application._connect = _make_sequence_connector([snapshot_connection, media_connection])

        response = await application.handle_api_get(
            "/api/v1/unique-tournament/17/media",
            "",
        )

        self.assertEqual(_UNIQUE_TOURNAMENT_MEDIA_EVENT_LIMIT, 200)
        self.assertEqual(len(response.payload["media"]), 200)
        # Sorted newest-first by start_timestamp (fetched_at equal) so the
        # truncation keeps the highest start_timestamp values.
        event_ids = [item["event"]["id"] for item in response.payload["media"]]
        self.assertEqual(event_ids[0], 250)
        self.assertEqual(event_ids[-1], 51)


class LocalApiConnectionAndCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_connect_uses_pool_lease_and_releases_on_close(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        pool = _FakePoolConnectionBackend()
        application._db_pool = pool

        lease = await application._connect()
        self.assertEqual(pool.acquire_calls, 1)
        self.assertEqual(pool.release_calls, 0)

        await lease.close()

        self.assertEqual(pool.release_calls, 1)

    async def test_handle_api_get_http_response_caches_live_bytes_for_two_seconds(self) -> None:
        from schema_inspector.api_cache import InProcessResponseCache

        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        current_time = [1000.0]
        application._cache_now = lambda: current_time[0]
        # N4 Layer B (2026-05-14): cache backend now pluggable. Use the
        # in-process implementation directly here so the test still
        # measures TTL-driven cache invalidation deterministically.
        application._response_cache_v2 = InProcessResponseCache(clock=lambda: current_time[0])
        calls: list[tuple[str, str]] = []

        async def fake_handle_api_get(path: str, raw_query: str) -> ApiResponse:
            calls.append((path, raw_query))
            return ApiResponse(status_code=200, payload={"events": [{"id": 1, "status": {"type": "inprogress"}}]})

        application.handle_api_get = fake_handle_api_get

        first = await application.handle_api_get_http_response("/api/v1/sport/football/events/live", "")
        second = await application.handle_api_get_http_response("/api/v1/sport/football/events/live", "")
        current_time[0] += 5.1
        third = await application.handle_api_get_http_response("/api/v1/sport/football/events/live", "")

        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.cache_control, "public, max-age=5")
        self.assertEqual(first.body, second.body)
        self.assertEqual(
            calls,
            [
                ("/api/v1/sport/football/events/live", ""),
                ("/api/v1/sport/football/events/live", ""),
            ],
        )
        self.assertEqual(third.cache_control, "public, max-age=5")

    async def test_handle_api_get_http_response_uses_static_ttl_for_non_live_payloads(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        application._response_cache = {}
        application._response_cache_lock = None
        application._cache_now = lambda: 1000.0

        async def fake_handle_api_get(path: str, raw_query: str) -> ApiResponse:
            del path, raw_query
            return ApiResponse(status_code=200, payload={"rounds": [{"round": 1}]})

        application.handle_api_get = fake_handle_api_get

        response = await application.handle_api_get_http_response(
            "/api/v1/unique-tournament/17/season/76986/rounds",
            "",
        )

        self.assertEqual(response.cache_control, "public, max-age=3600")


class LocalApiAsgiTests(unittest.IsolatedAsyncioTestCase):
    def test_asgi_api_route_uses_runtime_serialized_response(self) -> None:
        from fastapi.testclient import TestClient

        application = _FakeAsgiApplication(
            api_response=SerializedApiResponse(
                status_code=200,
                body=b'{"events":[{"id":1}]}',
                cache_control="public, max-age=2",
            )
        )
        app = create_asgi_app(application=application)

        with TestClient(app) as client:
            response = client.get("/api/v1/sport/football/events/live")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"events": [{"id": 1}]})
        self.assertEqual(response.headers["cache-control"], "public, max-age=2")
        self.assertEqual(application.api_calls, [("/api/v1/sport/football/events/live", "")])
        self.assertEqual(application.startup_calls, 1)
        self.assertEqual(application.shutdown_calls, 1)

    def test_asgi_options_route_returns_cors_headers(self) -> None:
        from fastapi.testclient import TestClient

        app = create_asgi_app(application=_FakeAsgiApplication())

        with TestClient(app) as client:
            response = client.options("/api/v1/event/1")

        self.assertEqual(response.status_code, 204)
        self.assertEqual(response.headers["access-control-allow-origin"], "*")

    def test_asgi_collapses_repeated_slashes_in_request_path(self) -> None:
        """A stray ``//`` in the URL must not break route matching.

        Sofascore canonical URLs never contain consecutive slashes, but client
        code that joins path fragments naively can produce ``/api/v1//player/...``
        — the catch-all route would otherwise return 404 because the regex
        compiled from ``path_template`` looks for exactly one ``/``. The local
        API normalizes the path before route lookup so the malformed form is
        served the same as the canonical one.
        """

        from fastapi.testclient import TestClient

        application = _FakeAsgiApplication(
            api_response=SerializedApiResponse(
                status_code=200,
                body=b'{"events":[]}',
                cache_control="public, max-age=2",
            )
        )
        app = create_asgi_app(application=application)

        with TestClient(app) as client:
            response = client.get("/api/v1//player/750/events/last/0")

        self.assertEqual(response.status_code, 200)
        # The application sees only the collapsed canonical path:
        self.assertEqual(
            application.api_calls,
            [("/api/v1/player/750/events/last/0", "")],
        )

    async def test_handle_api_get_http_response_uses_static_ttl_for_finished_event_root(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()
        application._response_cache = {}
        application._response_cache_lock = None
        application._cache_now = lambda: 1000.0

        async def fake_handle_api_get(path: str, raw_query: str) -> ApiResponse:
            del path, raw_query
            return ApiResponse(
                status_code=200,
                payload={"event": {"id": 15868599, "status": {"type": "finished", "description": "Ended"}}},
            )

        application.handle_api_get = fake_handle_api_get

        response = await application.handle_api_get_http_response("/api/v1/event/15868599", "")

        self.assertEqual(response.cache_control, "public, max-age=3600")

    async def test_startup_runs_pool_on_dedicated_runtime_loop(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.database_config = object()
        application._db_pool = None
        application._runtime_loop = None
        application._runtime_thread = None
        application._runtime_ready = threading.Event()
        application._openapi_json = None
        application._openapi_build_future = None
        application._openapi_warmup_future = None
        application._openapi_build_lock = threading.Lock()
        application.openapi_base_urls = ("http://127.0.0.1:8000",)
        application._response_cache = {}
        application._response_cache_lock = None
        application._cache_now = lambda: 1000.0

        pool = _FakePoolConnectionBackend()

        async def fake_create_pool_with_fallback(database_config):
            del database_config
            return pool

        with patch("schema_inspector.local_api_server.create_pool_with_fallback", side_effect=fake_create_pool_with_fallback), patch.object(
            application,
            "_schedule_deferred_openapi_warmup",
            return_value=None,
        ):
            await application.startup()
            try:
                self.assertIsNotNone(application._runtime_loop)
                self.assertIs(application._db_pool, pool)
                self.assertEqual(application.run_async(_return_runtime_value("ok")), "ok")
            finally:
                await application.shutdown()

        self.assertIsNone(application._db_pool)
        self.assertTrue(pool.closed)
        self.assertIsNone(application._runtime_loop)

    def test_constructor_does_not_build_openapi_eagerly(self) -> None:
        with patch(
            "schema_inspector.local_api_server.LocalApiApplication._build_openapi_document",
            side_effect=AssertionError("should not eagerly build openapi"),
        ), patch(
            "schema_inspector.local_api_server.load_cached_openapi_bytes",
            return_value=None,
        ):
            application = LocalApiApplication(
                database_config=object(),
                base_url="http://127.0.0.1:8000",
            )

        self.assertIsNone(application._openapi_json)

    async def test_openapi_json_builds_lazily_and_caches_bytes(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.database_config = object()
        application.base_url = "http://127.0.0.1:8000"
        application.openapi_base_urls = ("http://127.0.0.1:8000",)
        application._db_pool = None
        application._runtime_loop = None
        application._runtime_thread = None
        application._runtime_ready = threading.Event()
        application._openapi_json = None
        application._openapi_build_future = None
        application._openapi_warmup_future = None
        application._openapi_build_lock = threading.Lock()
        application._response_cache = {}
        application._response_cache_lock = None
        application._cache_now = lambda: 1000.0
        application.swagger_html = ""
        application._openapi_json_variants = {}
        build_calls = []

        async def fake_build_openapi_document() -> dict[str, object]:
            build_calls.append("build")
            return {"openapi": "3.1.0", "paths": {}, "components": {"schemas": {}}}

        application._build_openapi_document = fake_build_openapi_document

        with patch(
            "schema_inspector.local_api_server.write_cached_openapi_bytes",
            side_effect=lambda document, **_: _json_dumps_bytes(document),
        ):
            try:
                first = application.openapi_json
                second = application.openapi_json
            finally:
                await application.shutdown()

        self.assertEqual(first, second)
        self.assertEqual(build_calls, ["build"])

    async def test_openapi_json_for_request_builds_https_variant_from_forwarded_headers(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.database_config = SimpleNamespace(dsn="postgresql://example")
        application.base_url = "http://127.0.0.1:8000"
        application.openapi_base_urls = ("http://127.0.0.1:8000",)
        application._db_pool = None
        application._runtime_loop = None
        application._runtime_thread = None
        application._runtime_ready = threading.Event()
        application._openapi_json = b'{"servers":[{"url":"http://127.0.0.1:8000"}]}'
        application._openapi_json_variants = {}
        application._openapi_build_future = None
        application._openapi_warmup_future = None
        application._openapi_build_lock = threading.Lock()
        application._response_cache = {}
        application._response_cache_lock = None
        application._cache_now = lambda: 1000.0
        application.swagger_html = ""

        with patch(
            "schema_inspector.local_api_server.load_cached_openapi_bytes",
            return_value=None,
        ), patch(
            "schema_inspector.local_api_server._load_summary",
            return_value=_swagger_summary("2026-04-23T19:00:00+00:00"),
        ):
            try:
                payload = application.openapi_json_for_request(
                    {
                        "X-Forwarded-Proto": "https",
                        "X-Forwarded-Host": "api.var11.com",
                        "X-Forwarded-Port": "443",
                    }
                )
            finally:
                await application.shutdown()

        self.assertIn(b'"url":"https://api.var11.com"', payload)

    async def test_deferred_openapi_warmup_waits_before_building(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application._openapi_json = None
        application._openapi_warmup_future = None
        application._build_and_cache_openapi_json = AsyncMock(return_value=b"{}")

        delays: list[float] = []

        async def fake_sleep(delay: float) -> None:
            delays.append(delay)

        with patch("schema_inspector.local_api_server.asyncio.sleep", side_effect=fake_sleep):
            await application._deferred_openapi_warmup()

        self.assertEqual(delays, [120.0])
        application._build_and_cache_openapi_json.assert_awaited_once()
        self.assertIsNone(application._openapi_warmup_future)

    async def test_deferred_openapi_warmup_logs_and_swallows_failures(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application._openapi_json = None
        application._openapi_warmup_future = None
        application._build_and_cache_openapi_json = AsyncMock(side_effect=RuntimeError("boom"))

        async def fake_sleep(delay: float) -> None:
            del delay

        with patch("schema_inspector.local_api_server.asyncio.sleep", side_effect=fake_sleep), patch(
            "schema_inspector.local_api_server.logger.exception"
        ) as log_exception:
            await application._deferred_openapi_warmup()

        log_exception.assert_called_once()
        self.assertIsNone(application._openapi_warmup_future)


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

    async def test_handle_api_get_returns_empty_events_for_missing_scheduled_events_date(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()

        async def fake_snapshot(route, path, raw_query, path_params):
            return None

        async def fake_normalized(route, path, raw_query, path_params):
            return None

        application._fetch_snapshot_payload = fake_snapshot
        application._fetch_normalized_payload = fake_normalized

        response = await application.handle_api_get("/api/v1/sport/football/scheduled-events/2030-01-01", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload, {"events": []})

    async def test_fetch_snapshot_payload_pins_to_route_source_slug(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        routes = build_route_specs()
        result = match_route("/api/v1/sport/football/events/live", routes)
        assert result is not None
        route, path_params = result
        route = replace(route, endpoint=replace(route.endpoint, source_slug="sofascore"))
        connection = _FakeSnapshotConnection(
            rows=[
                {
                    "source_slug": "secondary-source",
                    "source_url": "https://mirror.example/api/v1/sport/football/events/live",
                    "payload": {"events": [{"id": 2}]},
                },
                {
                    "source_slug": "sofascore",
                    "source_url": "https://www.sofascore.com/api/v1/sport/football/events/live",
                    "payload": {"events": [{"id": 1}]},
                },
            ]
        )
        application._connect = _make_fake_connector(connection)
        application._reconcile_snapshot_payload = _passthrough_reconcile

        payload = await application._fetch_snapshot_payload(route, "/api/v1/sport/football/events/live", "", path_params)

        self.assertEqual(payload, {"events": [{"id": 1}]})
        self.assertIn("source_slug = $2", connection.fetch_calls[0][0])
        self.assertEqual(connection.fetch_calls[0][1][1], "sofascore")

    async def test_fetch_snapshot_no_entity_scope_filters_by_source_url(self) -> None:
        """N4 Layer D D.2 regression guard.

        Sport-level endpoints without context_entity_type must add a
        ``source_url LIKE '<canonical-prefix>%'`` filter in SQL so the
        composite partial index ``idx_api_payload_snapshot_source_url_lookup``
        can serve the lookup in 1-2 rows instead of returning 500 latest
        rows for the whole pattern (different dates) and JSON-parsing all
        of them in Python.

        See ``docs/PROGRESS.md`` Layer D D.2 entry.
        """

        application = LocalApiApplication.__new__(LocalApiApplication)
        routes = build_route_specs()
        path = "/api/v1/sport/tennis/scheduled-events/2026-05-15"
        result = match_route(path, routes)
        assert result is not None
        route, path_params = result
        connection = _FakeSnapshotConnection(
            rows=[
                {
                    "source_url": f"https://www.sofascore.com{path}",
                    "payload": {"events": [{"id": 7}]},
                    "is_soft_error_payload": False,
                    "http_status": 200,
                }
            ]
        )
        application._connect = _make_fake_connector(connection)
        application._reconcile_snapshot_payload = _passthrough_reconcile

        await application._fetch_snapshot_payload(route, path, "", path_params)

        query, args = connection.fetch_calls[0]
        # Must include the source_url filter
        self.assertIn("source_url LIKE", query)
        # Must NOT add a context_entity_type clause (this is a no-scope route)
        self.assertNotIn("context_entity_type", query)
        # The LIKE argument must be the canonical prefix + '%'
        self.assertIn(f"https://www.sofascore.com{path}%", args)

    async def test_fetch_snapshot_with_entity_scope_skips_source_url_filter(self) -> None:
        """N4 Layer D D.2 regression guard (negative).

        Entity-scoped routes (event/{id}/lineups, team/{id}, ...) must
        keep their existing ``context_entity_type = ... AND
        context_entity_id = ...`` filter and must NOT also add the
        source_url LIKE filter — they already hit a dedicated entity
        index and the LIKE would only slow them down.
        """

        application = LocalApiApplication.__new__(LocalApiApplication)
        routes = build_route_specs()
        path = "/api/v1/event/123456/lineups"
        result = match_route(path, routes)
        assert result is not None
        route, path_params = result
        connection = _FakeSnapshotConnection(
            rows=[
                {
                    "source_url": f"https://www.sofascore.com{path}",
                    "payload": {"home": []},
                    "is_soft_error_payload": False,
                    "http_status": 200,
                }
            ]
        )
        application._connect = _make_fake_connector(connection)
        application._reconcile_snapshot_payload = _passthrough_reconcile

        await application._fetch_snapshot_payload(route, path, "", path_params)

        query, _ = connection.fetch_calls[0]
        self.assertIn("context_entity_type", query)
        self.assertIn("context_entity_id", query)
        # Layer D D.2 filter must NOT apply here:
        self.assertNotIn("source_url LIKE", query)


class LocalApiSnapshotReconciliationTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_events_snapshot_keeps_terminal_events_with_status_override(self) -> None:
        """Regression guard for the under-counting live-list bug.

        Previously ``_reconcile_snapshot_payload`` unconditionally dropped
        every event with any ``event_terminal_state`` row from the live
        list. That caused near-total data loss whenever the upstream feed's
        grace window or the zombie sweeper marked a live-list event as
        terminal. The fix keeps the event in the list and overrides its
        status with the authoritative final status.
        """

        application = LocalApiApplication.__new__(LocalApiApplication)
        routes = build_route_specs()
        result = match_route("/api/v1/sport/football/events/live", routes)
        assert result is not None
        route, _ = result

        payload = {
            "events": [
                {
                    "id": 14109883,
                    "status": {"code": 7, "type": "inprogress", "description": "2nd half"},
                },
                {
                    "id": 14100000,
                    "status": {"code": 7, "type": "inprogress", "description": "2nd half"},
                },
            ]
        }
        executor = _FakeFetchExecutor(
            [
                {
                    "event_id": 14109883,
                    "terminal_status": "finished",
                    "final_payload": {
                        "event": {
                            "id": 14109883,
                            "status": {"code": 100, "type": "finished", "description": "Ended"},
                        }
                    },
                }
            ]
        )

        reconciled = await application._reconcile_snapshot_payload(executor, route, payload)

        # Both events must survive; only the status of the finished one is rewritten.
        self.assertEqual([item["id"] for item in reconciled["events"]], [14109883, 14100000])
        finished_event = reconciled["events"][0]
        self.assertEqual(finished_event["status"]["code"], 100)
        self.assertEqual(finished_event["status"]["type"], "finished")
        live_event = reconciled["events"][1]
        self.assertEqual(live_event["status"]["type"], "inprogress")

    async def test_live_events_snapshot_does_not_undercount_when_every_event_is_terminal(self) -> None:
        """Reproduces the observed 56-events-in-snapshot / 1-event-in-response prod bug.

        Even when every event in the raw snapshot has a terminal-state row
        (which is the default after the zombie sweeper has caught up), the
        response must still contain every event — only with their statuses
        rewritten.
        """

        application = LocalApiApplication.__new__(LocalApiApplication)
        routes = build_route_specs()
        result = match_route("/api/v1/sport/football/events/live", routes)
        assert result is not None
        route, _ = result

        payload = {
            "events": [
                {
                    "id": event_id,
                    "status": {"code": 7, "type": "inprogress", "description": "2nd half"},
                }
                for event_id in (15994150, 14109883, 14109884, 14109885, 14109886)
            ]
        }
        executor = _FakeFetchExecutor(
            [
                {
                    "event_id": event_id,
                    "terminal_status": "finished",
                    "final_payload": {
                        "event": {
                            "id": event_id,
                            "status": {"code": 100, "type": "finished", "description": "Ended"},
                        }
                    },
                }
                for event_id in (14109883, 14109884, 14109885, 14109886)
            ]
        )

        reconciled = await application._reconcile_snapshot_payload(executor, route, payload)

        self.assertEqual(
            [item["id"] for item in reconciled["events"]],
            [15994150, 14109883, 14109884, 14109885, 14109886],
        )
        still_live = reconciled["events"][0]
        self.assertEqual(still_live["status"]["type"], "inprogress")
        for finished in reconciled["events"][1:]:
            self.assertEqual(finished["status"]["type"], "finished")

    async def test_live_events_snapshot_ignores_zombie_terminal_status(self) -> None:
        """The zombie-sweeper stamp must not be treated as authoritative.

        Housekeeping stamps ``zombie_stale`` for events whose live polling
        went quiet past the cutoff, with ``final_snapshot_id = NULL``. If
        upstream later resurrects the event in a list snapshot, the read
        layer must trust the snapshot — not override its status from a
        synthetic sentinel that has no real ``event.status`` attached.
        """

        application = LocalApiApplication.__new__(LocalApiApplication)
        routes = build_route_specs()
        result = match_route("/api/v1/sport/football/events/live", routes)
        assert result is not None
        route, _ = result

        payload = {
            "events": [
                {
                    "id": 14109883,
                    "status": {"code": 7, "type": "inprogress", "description": "2nd half"},
                }
            ]
        }
        executor = _FakeFetchExecutor(
            [
                {
                    "event_id": 14109883,
                    "terminal_status": "zombie_stale",
                    "final_payload": None,
                }
            ]
        )

        reconciled = await application._reconcile_snapshot_payload(executor, route, payload)

        self.assertEqual([item["id"] for item in reconciled["events"]], [14109883])
        self.assertEqual(reconciled["events"][0]["status"]["type"], "inprogress")

    async def test_live_events_snapshot_matches_terminal_status_by_custom_id_when_event_id_changes(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        routes = build_route_specs()
        result = match_route("/api/v1/sport/football/events/live", routes)
        assert result is not None
        route, _ = result

        payload = {
            "events": [
                {
                    "id": 16006762,
                    "customId": "KzcsAoRb",
                    "startTimestamp": 1775779200,
                    "homeTeam": {"id": 44},
                    "awayTeam": {"id": 45},
                    "status": {"code": 7, "type": "inprogress", "description": "2nd half"},
                }
            ]
        }
        executor = _FakeFetchExecutor(
            [
                {
                    "event_id": 15362622,
                    "custom_id": "KzcsAoRb",
                    "start_timestamp": 1775779200,
                    "home_team_id": 44,
                    "away_team_id": 45,
                    "terminal_status": "finished",
                    "finalized_at": "2026-04-21T20:00:00+00:00",
                    "final_payload": {
                        "event": {
                            "id": 15362622,
                            "status": {"code": 100, "type": "finished", "description": "Ended"},
                        }
                    },
                }
            ]
        )

        reconciled = await application._reconcile_snapshot_payload(executor, route, payload)

        self.assertEqual([item["id"] for item in reconciled["events"]], [16006762])
        self.assertEqual(reconciled["events"][0]["status"]["code"], 100)
        self.assertEqual(reconciled["events"][0]["status"]["type"], "finished")

    async def test_scheduled_events_snapshot_overrides_terminal_status_from_final_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        routes = build_route_specs()
        result = match_route("/api/v1/sport/football/scheduled-events/2026-04-19", routes)
        assert result is not None
        route, _ = result

        payload = {
            "events": [
                {
                    "id": 14109883,
                    "status": {"code": 7, "type": "inprogress", "description": "2nd half"},
                }
            ]
        }
        executor = _FakeFetchExecutor(
            [
                {
                    "event_id": 14109883,
                    "terminal_status": "finished",
                    "final_payload": {
                        "event": {
                            "id": 14109883,
                            "status": {"code": 100, "type": "finished", "description": "Ended"},
                        }
                    },
                }
            ]
        )

        reconciled = await application._reconcile_snapshot_payload(executor, route, payload)

        self.assertEqual(reconciled["events"][0]["status"]["code"], 100)
        self.assertEqual(reconciled["events"][0]["status"]["type"], "finished")
        self.assertEqual(reconciled["events"][0]["status"]["description"], "Ended")


class LocalApiEntityRootFallbackTests(unittest.IsolatedAsyncioTestCase):
    """Guarantees that ``/api/v1/{entity}/{id}`` root routes do not 404 when
    ingested data exists in any downstream layer (final snapshot, root
    snapshot, or normalized row)."""

    def test_extract_event_id_from_entity_root_path_matches_all_known_kinds(self) -> None:
        self.assertEqual(_extract_event_id_from_entity_root_path("/api/v1/event/123", "event"), 123)
        self.assertEqual(_extract_event_id_from_entity_root_path("/api/v1/team/42", "team"), 42)
        self.assertEqual(_extract_event_id_from_entity_root_path("/api/v1/player/7", "player"), 7)
        self.assertEqual(_extract_event_id_from_entity_root_path("/api/v1/manager/9", "manager"), 9)
        self.assertEqual(
            _extract_event_id_from_entity_root_path("/api/v1/unique-tournament/17", "unique-tournament"),
            17,
        )
        # Child routes must not match a root pattern.
        self.assertIsNone(
            _extract_event_id_from_entity_root_path("/api/v1/event/123/statistics", "event")
        )
        # Cross-kind paths must not match.
        self.assertIsNone(_extract_event_id_from_entity_root_path("/api/v1/team/42", "event"))

    async def test_event_root_returns_final_snapshot_when_available(self) -> None:
        """When ``event_terminal_state.final_snapshot_id`` points to a row,
        the read layer must return that exact upstream response rather than
        synthesizing from the normalized row."""

        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                ("final_payload", 15994150): {
                    "final_payload": {
                        "event": {
                            "id": 15994150,
                            "status": {"code": 100, "type": "finished"},
                            "homeTeam": {"id": 10},
                            "awayTeam": {"id": 11},
                        }
                    }
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_event_root_payload(15994150)

        self.assertIsNotNone(result)
        self.assertEqual(result["event"]["id"], 15994150)
        self.assertEqual(result["event"]["status"]["type"], "finished")

    async def test_event_root_falls_back_to_normalized_row_when_no_snapshots(self) -> None:
        """When no raw or final-snapshot payload exists but a normalized
        ``event`` row does, the route must answer 200 instead of 404."""

        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                ("normalized_event", 15994150): {
                    "id": 15994150,
                    "slug": "home-away",
                    "tournament_id": 100,
                    "unique_tournament_id": 17,
                    "season_id": 76986,
                    "home_team_id": 10,
                    "away_team_id": 11,
                    "venue_id": 5,
                    "start_timestamp": 1713638400,
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_event_root_payload(15994150)

        self.assertIsNotNone(result)
        self.assertEqual(result["event"]["id"], 15994150)
        self.assertEqual(result["event"]["slug"], "home-away")
        self.assertEqual(result["event"]["homeTeam"]["id"], 10)
        self.assertEqual(result["event"]["awayTeam"]["id"], 11)
        self.assertEqual(result["event"]["season"]["id"], 76986)
        self.assertEqual(result["event"]["tournament"]["id"], 100)
        self.assertEqual(result["event"]["tournament"]["uniqueTournament"]["id"], 17)

    async def test_event_root_overrides_raw_snapshot_with_zombie_terminal_status(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                ("final_payload", 15994150): {
                    "terminal_status": "zombie_stale",
                    "final_payload": None,
                },
                ("raw_event_snapshot", 15994150): {
                    "payload": {
                        "event": {
                            "id": 15994150,
                            "status": {"code": 7, "type": "inprogress"},
                            "homeTeam": {"id": 10},
                            "awayTeam": {"id": 11},
                        }
                    }
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_event_root_payload(15994150)

        self.assertIsNotNone(result)
        self.assertEqual(result["event"]["id"], 15994150)
        self.assertEqual(result["event"]["status"]["code"], 91)
        self.assertEqual(result["event"]["status"]["type"], "stale_live")

    async def test_event_root_returns_none_when_nothing_is_ingested(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(rows={})
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_event_root_payload(99999999)

        self.assertIsNone(result)

    async def test_team_root_falls_back_to_normalized_row(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                ("normalized_team", 42): {
                    "id": 42,
                    "slug": "real-madrid",
                    "name": "Real Madrid",
                    "short_name": "Real",
                    "sport_id": 1,
                    "category_id": 5,
                    "country_alpha2": "ES",
                    "manager_id": 101,
                    "venue_id": 501,
                    "tournament_id": 200,
                    "primary_unique_tournament_id": 8,
                    "parent_team_id": None,
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_team_root_payload(42)

        self.assertIsNotNone(result)
        self.assertEqual(result["team"]["id"], 42)
        self.assertEqual(result["team"]["slug"], "real-madrid")
        self.assertEqual(result["team"]["name"], "Real Madrid")
        self.assertEqual(result["team"]["country"]["alpha2"], "ES")

    async def test_player_root_falls_back_to_normalized_row(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                ("normalized_player", 288205): {
                    "id": 288205,
                    "slug": "k-m",
                    "name": "K. Mbappé",
                    "short_name": "K.M.",
                    "team_id": 42,
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_player_root_payload(288205)

        self.assertIsNotNone(result)
        self.assertEqual(result["player"]["id"], 288205)
        self.assertEqual(result["player"]["team"]["id"], 42)

    async def test_player_root_uses_historic_lineup_team_when_available(self) -> None:
        """Stage 2.1 (2026-05-20 architecture rework, Anomaly A):
        ``/api/v1/player/{id}`` must return the team the player ACTUALLY
        played for on his most recent lineup, not the perpetually-current
        ``player.team_id`` column. Without this, a retired player still
        appears under his very last (possibly transferred) club, and any
        historical query through the synth-path reports the wrong club.

        Scenario: ``player.team_id`` was last written when the player
        moved to club id=999 (e.g. Saudi league transfer). But his most
        recent lineup row is for club id=42 (historic club). The synth
        path must surface the lineup-derived club."""

        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                # player.team_id = 999 (post-transfer current club).
                ("normalized_player", 288205): {
                    "id": 288205,
                    "slug": "k-m",
                    "name": "K. Mbappé",
                    "short_name": "K.M.",
                    "team_id": 999,
                },
                # Lineup-derived team_id = 42 (historic club).
                ("player_team_history", 288205): {
                    "team_id": 42,
                    "side": "home",
                    "home_team_id": 42,
                    "away_team_id": 7,
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_player_root_payload(288205)

        self.assertIsNotNone(result)
        self.assertEqual(result["player"]["id"], 288205)
        self.assertEqual(
            result["player"]["team"]["id"], 42,
            msg=(
                "Stage 2.1: lineup-derived historic team (42) must win "
                "over the current player.team_id (999). Without this, "
                "API lies about retired/transferred players."
            ),
        )

    async def test_manager_root_falls_back_to_normalized_row(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                ("normalized_manager", 9001): {
                    "id": 9001,
                    "slug": "c-a",
                    "name": "C. Ancelotti",
                    "short_name": "C.A.",
                    "team_id": 42,
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_manager_root_payload(9001)

        self.assertIsNotNone(result)
        self.assertEqual(result["manager"]["id"], 9001)
        self.assertEqual(result["manager"]["team"]["id"], 42)

    async def test_unique_tournament_root_falls_back_to_normalized_row(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                ("normalized_unique_tournament", 17): {
                    "id": 17,
                    "slug": "laliga",
                    "name": "LaLiga",
                    "category_id": 5,
                    "country_alpha2": "ES",
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_unique_tournament_root_payload(17)

        self.assertIsNotNone(result)
        self.assertEqual(result["uniqueTournament"]["id"], 17)
        self.assertEqual(result["uniqueTournament"]["slug"], "laliga")
        self.assertEqual(result["uniqueTournament"]["category"]["id"], 5)

    def test_synthesize_event_root_payload_handles_missing_optional_fields(self) -> None:
        row = {
            "id": 1,
            "slug": None,
            "tournament_id": None,
            "unique_tournament_id": None,
            "season_id": None,
            "home_team_id": None,
            "away_team_id": None,
            "venue_id": None,
            "start_timestamp": None,
        }
        payload = _synthesize_event_root_payload(row)
        self.assertEqual(payload, {"event": {"id": 1}})

    def test_synthesize_team_root_payload_handles_missing_optional_fields(self) -> None:
        row = {
            "id": 1,
            "slug": None,
            "name": "Solo",
            "short_name": None,
            "sport_id": None,
            "category_id": None,
            "country_alpha2": None,
            "manager_id": None,
            "venue_id": None,
            "tournament_id": None,
            "primary_unique_tournament_id": None,
            "parent_team_id": None,
        }
        payload = _synthesize_team_root_payload(row)
        self.assertEqual(payload, {"team": {"id": 1, "name": "Solo"}})

    def test_synthesize_player_root_payload_without_team(self) -> None:
        row = {"id": 1, "slug": None, "name": "X", "short_name": None, "team_id": None}
        payload = _synthesize_player_root_payload(row)
        self.assertEqual(payload, {"player": {"id": 1, "name": "X"}})

    def test_synthesize_manager_root_payload_without_team(self) -> None:
        row = {"id": 1, "slug": None, "name": "Y", "short_name": None, "team_id": None}
        payload = _synthesize_manager_root_payload(row)
        self.assertEqual(payload, {"manager": {"id": 1, "name": "Y"}})

    def test_synthesize_unique_tournament_root_payload_minimal(self) -> None:
        row = {"id": 1, "slug": None, "name": "Z", "category_id": None, "country_alpha2": None}
        payload = _synthesize_unique_tournament_root_payload(row)
        self.assertEqual(payload, {"uniqueTournament": {"id": 1, "name": "Z"}})

    # --- stripped snapshot rewrap (PR fixes ingest-side wrapper loss) ---

    def test_wrap_stripped_entity_payload_wraps_dict_with_id(self) -> None:
        result = _wrap_stripped_entity_payload({"id": 17, "name": "LaLiga"}, "uniqueTournament")
        self.assertEqual(result, {"uniqueTournament": {"id": 17, "name": "LaLiga"}})

    def test_wrap_stripped_entity_payload_passes_through_already_wrapped(self) -> None:
        already_wrapped = {"uniqueTournament": {"id": 17}, "extra": "x"}
        result = _wrap_stripped_entity_payload(already_wrapped, "uniqueTournament")
        self.assertIs(result, already_wrapped)

    def test_wrap_stripped_entity_payload_passes_through_error_envelope(self) -> None:
        # Soft-error / status payloads without "id" must not be coerced into a fake entity.
        err = {"error": {"code": 404, "message": "Not Found"}}
        result = _wrap_stripped_entity_payload(err, "uniqueTournament")
        self.assertIs(result, err)

    def test_wrap_stripped_entity_payload_passes_through_non_dict(self) -> None:
        self.assertEqual(_wrap_stripped_entity_payload([], "uniqueTournament"), [])
        self.assertIsNone(_wrap_stripped_entity_payload(None, "uniqueTournament"))
        self.assertEqual(_wrap_stripped_entity_payload("oops", "uniqueTournament"), "oops")

    async def test_unique_tournament_root_rewraps_stripped_snapshot(self) -> None:
        """Historical raw snapshots were stored as the inner uniqueTournament
        body (``{"id": 17, ...}``) instead of the upstream-wrapped envelope.
        The handler must restore the wrapper on read so the API stays 1:1."""

        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                ("raw_unique_tournament_snapshot", 17): {
                    "payload": {"id": 17, "slug": "laliga", "name": "LaLiga"},
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_unique_tournament_root_payload(17)

        self.assertIsNotNone(result)
        self.assertIn("uniqueTournament", result)
        self.assertEqual(result["uniqueTournament"]["id"], 17)
        self.assertEqual(result["uniqueTournament"]["slug"], "laliga")
        # Critically: the handler must NOT also synthesize from the normalized
        # row when a raw snapshot exists, even when stripped.
        self.assertNotIn("id", result)

    async def test_unique_tournament_root_passes_through_already_wrapped_snapshot(self) -> None:
        """Newer ingest paths that correctly preserve the upstream wrapper
        must not be touched twice."""

        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                ("raw_unique_tournament_snapshot", 17): {
                    "payload": {
                        "uniqueTournament": {"id": 17, "name": "LaLiga"},
                        "extra": "preserved",
                    },
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_unique_tournament_root_payload(17)

        self.assertEqual(result["uniqueTournament"]["id"], 17)
        # No double-wrapping.
        self.assertNotIn("uniqueTournament", result["uniqueTournament"])
        self.assertEqual(result.get("extra"), "preserved")

    async def test_event_root_rewraps_stripped_terminal_snapshot(self) -> None:
        """Some finalized snapshots in event_terminal_state.final_snapshot_id
        were stored as the inner event body. Re-wrap on read so the contract
        still surfaces as ``{"event": {...}}``."""

        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                ("final_payload", 15994150): {
                    "terminal_status": "finished",
                    "final_payload": {
                        "id": 15994150,
                        "status": {"code": 100, "type": "finished"},
                        "homeTeam": {"id": 10},
                    },
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_event_root_payload(15994150)

        self.assertIsNotNone(result)
        self.assertIn("event", result)
        self.assertEqual(result["event"]["id"], 15994150)
        self.assertEqual(result["event"]["status"]["type"], "finished")

    async def test_event_root_rewraps_stripped_raw_snapshot_and_applies_terminal_status(self) -> None:
        """A stripped raw event snapshot must be wrapped before
        ``_apply_terminal_status_to_event_payload`` runs, otherwise the
        terminal-status overlay silently misses (it expects ``payload['event']``)."""

        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                ("final_payload", 15994151): {
                    "terminal_status": "finished",
                    "final_payload": None,  # no finalized snapshot row -> waterfall to raw
                },
                ("raw_event_snapshot", 15994151): {
                    "payload": {
                        "id": 15994151,
                        "status": {"code": 7, "type": "inprogress"},
                        "slug": "x-vs-y",
                    },
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_event_root_payload(15994151)

        self.assertIsNotNone(result)
        self.assertIn("event", result)
        self.assertEqual(result["event"]["id"], 15994151)
        # Terminal-status overlay must override the live status from the stripped payload.
        self.assertEqual(result["event"]["status"]["type"], "finished")


class LocalApiPlayerStatisticsTests(unittest.IsolatedAsyncioTestCase):
    """``/api/v1/player/{player_id}/statistics`` must serve the upstream
    ``{"seasons": [...], "typesMap": {...}}`` shape directly from the latest
    raw snapshot, not the flat ``{"playerSeasonStatistics": [...]}`` produced
    by the legacy normalized handler. The normalized fallback only kicks in
    when no raw snapshot exists for the player."""

    async def test_player_statistics_returns_raw_snapshot_when_present(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                ("raw_player_statistics_snapshot", 944656): {
                    "payload": {
                        "seasons": [
                            {
                                "endYear": "2024",
                                "season": {"id": 76986, "name": "23/24"},
                                "startYear": "2023",
                                "statistics": {"goals": 21},
                                "team": {"id": 42, "slug": "team-42"},
                                "uniqueTournament": {"id": 17},
                                "year": "23/24",
                            }
                        ],
                        "typesMap": {"1": "overall"},
                    },
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_player_statistics_payload(944656)

        self.assertIsNotNone(result)
        self.assertEqual(sorted(result.keys()), ["seasons", "typesMap"])
        self.assertEqual(result["seasons"][0]["season"]["id"], 76986)
        self.assertEqual(result["typesMap"], {"1": "overall"})

    async def test_player_statistics_returns_none_when_no_snapshot_to_allow_normalized_fallback(
        self,
    ) -> None:
        """When no raw snapshot exists, the specialized handler must return
        ``None`` so the dispatcher falls through to the generic normalized
        handler over ``player_season_statistics``. This preserves coverage
        for players we have ingested into normalized tables but never
        snapshotted."""

        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(rows={})
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_player_statistics_payload(99999999)

        self.assertIsNone(result)

    async def test_player_statistics_returns_none_when_snapshot_payload_is_not_a_dict(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeFetchRowConnection(
            rows={
                ("raw_player_statistics_snapshot", 944656): {
                    "payload": "not-an-object",
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_player_statistics_payload(944656)

        self.assertIsNone(result)


class _FakeEntityPassthroughConnection:
    """Connection that resolves raw passthrough fetches keyed by either
    ``(endpoint_pattern, context_entity_id)`` (single-entity endpoints) or
    ``(player_id, source_url)`` (paginated player events/{last,next})."""

    def __init__(self, rows: dict):
        self.rows = rows
        self.closed = False

    async def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        # _fetch_latest_entity_passthrough(pattern, type, id) -- 3 args, type is $2 placeholder.
        if "context_entity_type = $2" in normalized and len(args) >= 3:
            return self.rows.get((str(args[0]), int(args[2])))
        # _fetch_player_events_per_page(pattern, player_id, source_url) --
        # 3 args, type is the literal 'player'.
        if (
            "context_entity_type = 'player'" in normalized
            and len(args) >= 3
            and isinstance(args[0], str)
            and "/events/" in str(args[0])
        ):
            return self.rows.get((int(args[1]), str(args[2])))
        return None

    async def close(self):
        self.closed = True


class LocalApiRawPassthroughTests(unittest.IsolatedAsyncioTestCase):
    """D-category endpoints that have no normalized representation: serve the
    latest raw snapshot or 404 (None). No legacy fallback is intentional --
    reconstruction would silently lie about sofascore-derived synthetic data."""

    async def test_team_players_returns_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeEntityPassthroughConnection(
            rows={
                ("/api/v1/team/{team_id}/players", 42): {
                    "payload": {
                        "players": [{"id": 1, "name": "X"}],
                        "foreignPlayers": [],
                        "nationalPlayers": [{"id": 2, "name": "Y"}],
                    }
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_team_players_payload(42)

        self.assertEqual(sorted(result.keys()), ["foreignPlayers", "nationalPlayers", "players"])
        self.assertEqual(result["players"][0]["id"], 1)
        self.assertEqual(result["nationalPlayers"][0]["id"], 2)

    async def test_team_transfers_returns_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeEntityPassthroughConnection(
            rows={
                ("/api/v1/team/{team_id}/transfers", 2817): {
                    "payload": {
                        "transfersIn": [
                            {"id": 100, "player": {"id": 9}, "transferDateTimestamp": 1700000000}
                        ],
                        "transfersOut": [
                            {"id": 101, "player": {"id": 11}, "transferDateTimestamp": 1700000010}
                        ],
                    }
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_team_transfers_payload(2817)

        self.assertEqual(sorted(result.keys()), ["transfersIn", "transfersOut"])
        self.assertEqual(result["transfersIn"][0]["id"], 100)
        self.assertEqual(result["transfersOut"][0]["id"], 101)

    async def test_team_featured_players_returns_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeEntityPassthroughConnection(
            rows={
                ("/api/v1/team/{team_id}/featured-players", 37): {
                    "payload": {
                        "featuredPlayers": {
                            "14065232": {"player": {"id": 978838}, "eventId": 14065232}
                        }
                    }
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_team_featured_players_payload(37)

        self.assertEqual(list(result.keys()), ["featuredPlayers"])
        self.assertEqual(result["featuredPlayers"]["14065232"]["eventId"], 14065232)

    async def test_player_attribute_overviews_returns_raw_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeEntityPassthroughConnection(
            rows={
                ("/api/v1/player/{player_id}/attribute-overviews", 1142562): {
                    "payload": {
                        "playerAttributeOverviews": [
                            {"attacking": 83, "yearShift": 0, "id": 62861}
                        ],
                        "averageAttributeOverviews": [
                            {"attacking": 61, "yearShift": 0, "id": 19812}
                        ],
                    }
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_player_attribute_overviews_payload(1142562)

        self.assertEqual(
            sorted(result.keys()),
            ["averageAttributeOverviews", "playerAttributeOverviews"],
        )
        self.assertEqual(result["playerAttributeOverviews"][0]["attacking"], 83)

    async def test_player_events_last_uses_per_page_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeEntityPassthroughConnection(
            rows={
                (1152, "https://www.sofascore.com/api/v1/player/1152/events/last/0"): {
                    "payload": {
                        "events": [{"id": 100}],
                        "hasNextPage": True,
                        "playedForTeamMap": {"100": 42},
                    }
                },
                (1152, "https://www.sofascore.com/api/v1/player/1152/events/last/1"): {
                    "payload": {
                        "events": [{"id": 200}],
                        "hasNextPage": False,
                    }
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        page0 = await application._fetch_player_events_last_payload(1152, 0)
        page1 = await application._fetch_player_events_last_payload(1152, 1)

        self.assertEqual(page0["events"][0]["id"], 100)
        self.assertEqual(page0["hasNextPage"], True)
        self.assertEqual(page0["playedForTeamMap"], {"100": 42})
        self.assertEqual(page1["events"][0]["id"], 200)
        self.assertEqual(page1["hasNextPage"], False)

    # NOTE: Item 2 (Phase 2.2) flipped ``_fetch_team_players_payload`` from
    # raw-passthrough-only to a hybrid (snapshot primary + synthesizer
    # fallback). The "returns None on bad snapshot" guarantee is now split:
    #   * Bad snapshot (soft-error / 4xx) is still filtered upstream by
    #     ``_fetch_latest_entity_passthrough`` — covered by that helper's tests.
    #   * After filtering, the dispatcher synthesizes a minimal envelope
    #     from ``event_lineup_player`` instead of returning 404. Hybrid
    #     contract tests live in ``test_team_players_hybrid_synth.py``.
    # The former soft_error / 4xx tests are removed because their
    # assertion (``assertIsNone``) is now incorrect — the contract changed.

    async def test_player_events_last_returns_none_for_soft_error_snapshot(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeEntityPassthroughConnection(
            rows={
                (1234567, "https://www.sofascore.com/api/v1/player/1234567/events/last/0"): {
                    "payload": {"error": {"code": 404}},
                    "is_soft_error_payload": True,
                    "http_status": 404,
                },
            }
        )
        application._connect = _make_fake_connector(connection)

        result = await application._fetch_player_events_last_payload(1234567, 0)

        self.assertIsNone(result)

    async def test_passthrough_handlers_return_none_when_no_snapshot(self) -> None:
        """Without a raw snapshot these passthrough-only routes must return
        ``None`` so the dispatcher produces a 404. There is no normalized
        fallback for them; guessing would return wrong data.

        ``team/{id}/players`` is excluded here because Item 2 (Phase 2.2)
        moved it to a hybrid contract — see test_team_players_hybrid_synth.py
        for the new envelope-on-empty behavior.
        """

        application = LocalApiApplication.__new__(LocalApiApplication)
        connection = _FakeEntityPassthroughConnection(rows={})
        application._connect = _make_fake_connector(connection)

        self.assertIsNone(await application._fetch_team_featured_players_payload(99999999))
        self.assertIsNone(await application._fetch_player_attribute_overviews_payload(99999999))
        self.assertIsNone(await application._fetch_player_events_last_payload(99999999, 0))


class LocalApiNormalizedFallbackDispatchTests(unittest.IsolatedAsyncioTestCase):
    """End-to-end dispatch: ensure ``_fetch_normalized_payload`` wires each
    entity-root route to the correct fallback method without touching the
    database."""

    async def test_event_root_handle_get_uses_fast_path_before_snapshot_scan(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()

        async def fail_snapshot_lookup(route, path, raw_query, path_params):
            del route, path, raw_query, path_params
            raise AssertionError("entity root route should not run generic snapshot lookup")

        async def fake_fetch(event_id: int) -> dict[str, dict[str, int]]:
            return {"event": {"id": event_id}}

        application._fetch_snapshot_payload = fail_snapshot_lookup
        application._fetch_event_root_payload = fake_fetch

        response = await application.handle_api_get("/api/v1/event/15994150", "")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.payload, {"event": {"id": 15994150}})

    async def test_event_root_path_dispatches_to_event_root_fetcher(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()

        calls: list[tuple[str, int]] = []

        async def fake_fetch(event_id: int) -> dict[str, int]:
            calls.append(("event", event_id))
            return {"event": {"id": event_id}}

        application._fetch_event_root_payload = fake_fetch

        result = match_route("/api/v1/event/15994150", application.routes)
        assert result is not None
        route, path_params = result
        payload = await application._fetch_normalized_payload(
            route,
            "/api/v1/event/15994150",
            "",
            path_params,
        )

        self.assertEqual(payload, {"event": {"id": 15994150}})
        self.assertEqual(calls, [("event", 15994150)])

    async def test_team_root_path_dispatches_to_team_root_fetcher(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()

        async def fake_fetch(team_id: int) -> dict[str, int]:
            return {"team": {"id": team_id}}

        application._fetch_team_root_payload = fake_fetch

        result = match_route("/api/v1/team/42", application.routes)
        assert result is not None
        route, path_params = result
        payload = await application._fetch_normalized_payload(
            route,
            "/api/v1/team/42",
            "",
            path_params,
        )

        self.assertEqual(payload, {"team": {"id": 42}})

    async def test_player_root_path_dispatches_to_player_root_fetcher(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()

        async def fake_fetch(player_id: int) -> dict[str, int]:
            return {"player": {"id": player_id}}

        application._fetch_player_root_payload = fake_fetch

        result = match_route("/api/v1/player/288205", application.routes)
        assert result is not None
        route, path_params = result
        payload = await application._fetch_normalized_payload(
            route,
            "/api/v1/player/288205",
            "",
            path_params,
        )

        self.assertEqual(payload, {"player": {"id": 288205}})

    async def test_unique_tournament_root_path_dispatches_to_ut_fetcher(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        application.routes = build_route_specs()

        async def fake_fetch(unique_tournament_id: int) -> dict[str, int]:
            return {"uniqueTournament": {"id": unique_tournament_id}}

        application._fetch_unique_tournament_root_payload = fake_fetch

        result = match_route("/api/v1/unique-tournament/17", application.routes)
        assert result is not None
        route, path_params = result
        payload = await application._fetch_normalized_payload(
            route,
            "/api/v1/unique-tournament/17",
            "",
            path_params,
        )

        self.assertEqual(payload, {"uniqueTournament": {"id": 17}})


class LocalApiRedisLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_shutdown_async_closes_redis_backend(self) -> None:
        application = LocalApiApplication.__new__(LocalApiApplication)
        pool = _FakeClosablePool()
        redis_backend = _FakeClosableRedis()
        application._db_pool = pool
        application.redis_backend = redis_backend

        await application._shutdown_async()

        self.assertTrue(pool.closed)
        self.assertTrue(redis_backend.closed)

    def test_load_optional_redis_backend_closes_backend_when_ping_fails(self) -> None:
        import schema_inspector.local_api_server as local_api_server

        fake_backend = _FakeFailingRedis()
        fake_redis_module = SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *args, **kwargs: fake_backend),
        )

        with mock.patch.object(local_api_server, "_load_project_env", return_value={"REDIS_URL": "redis://example"}):
            with mock.patch.dict("sys.modules", {"redis": fake_redis_module}):
                backend = local_api_server._load_optional_redis_backend(None)

        self.assertIsNone(backend)
        self.assertTrue(fake_backend.closed)


if __name__ == "__main__":
    unittest.main()


def _make_fake_connector(connection):
    async def _connect():
        return connection

    return _connect


def _make_sequence_connector(connections):
    pending = list(connections)

    async def _connect():
        if not pending:
            raise AssertionError("No fake connection left")
        return pending.pop(0)

    return _connect


class _FakeClosablePool:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _FakeClosableRedis:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeFailingRedis(_FakeClosableRedis):
    def ping(self) -> None:
        raise RuntimeError("redis unavailable")


class _FakeFetchRowConnection:
    """Stand-in for an asyncpg connection that maps ``fetchrow`` queries to
    prepared rows by ``(kind, id)`` keys derived from the SQL shape.

    This keeps the tests hermetic (no DB) while still exercising the real
    ``_fetch_*_root_payload`` method bodies end-to-end.
    """

    def __init__(self, rows: dict):
        self.rows = rows
        self.closed = False

    async def fetchrow(self, query: str, *args):
        kind = self._classify_query(query)
        if kind is None or not args:
            return None
        key = (kind, int(args[0]))
        row = self.rows.get(key)
        if row is None:
            return None
        return row

    async def close(self):
        self.closed = True

    @staticmethod
    def _classify_query(query: str) -> str | None:
        normalized = " ".join(query.split())
        if "FROM event_terminal_state" in normalized:
            return "final_payload"
        if "FROM api_payload_snapshot" in normalized and "/api/v1/player/{player_id}/statistics" in normalized:
            return "raw_player_statistics_snapshot"
        if "FROM api_payload_snapshot" in normalized and "event_id" in normalized:
            return "raw_event_snapshot"
        if "FROM api_payload_snapshot" in normalized and "team_id" in normalized:
            return "raw_team_snapshot"
        if "FROM api_payload_snapshot" in normalized and "player_id" in normalized:
            return "raw_player_snapshot"
        if "FROM api_payload_snapshot" in normalized and "manager_id" in normalized:
            return "raw_manager_snapshot"
        if "FROM api_payload_snapshot" in normalized and "unique_tournament_id" in normalized:
            return "raw_unique_tournament_snapshot"
        if "FROM event " in (normalized + " ") or normalized.endswith("FROM event WHERE id = $1"):
            return "normalized_event"
        if "FROM team" in normalized:
            return "normalized_team"
        if "FROM event_lineup_player" in normalized:
            return "player_team_history"
        if "FROM player" in normalized:
            return "normalized_player"
        if "FROM manager" in normalized:
            return "normalized_manager"
        if "FROM unique_tournament" in normalized:
            return "normalized_unique_tournament"
        return None


class _FakeCoverageConnection:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.closed = False

    async def fetch(self, query: str, *args):
        del query, args
        return self.rows

    async def close(self):
        self.closed = True


class _FakeSnapshotConnection:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        if "source_slug =" not in query or len(args) < 2:
            return self.rows
        expected_source_slug = args[1]
        return [row for row in self.rows if row.get("source_slug") == expected_source_slug]

    async def close(self):
        self.closed = True


class _FakeEventListSnapshotConnection(_FakeSnapshotConnection):
    def __init__(
        self,
        rows: list[dict[str, object]],
        *,
        surface_rows: list[dict[str, object]],
    ) -> None:
        super().__init__(rows)
        self.surface_rows = surface_rows

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM event_terminal_state" in normalized:
            return []
        if "FROM event AS e" in normalized and "event_score" in normalized:
            return self.surface_rows
        return await super().fetch(query, *args)


class _FakeStatisticsSeasonsConnection:
    def __init__(self, *, season_rows: list[dict[str, object]], type_rows: list[dict[str, object]]) -> None:
        self.season_rows = season_rows
        self.type_rows = type_rows
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        if "stat_type" in query:
            return self.type_rows
        return self.season_rows

    async def close(self):
        self.closed = True


class _FakeNormalizedFallbackConnection:
    def __init__(
        self,
        *,
        event_player_statistics: list[dict[str, object]] | None = None,
        event_player_stat_values: list[dict[str, object]] | None = None,
        rating_breakdown: list[dict[str, object]] | None = None,
        heatmap_points: list[dict[str, object]] | None = None,
        top_player_snapshot: dict[str, object] | None = None,
        top_player_entries: list[dict[str, object]] | None = None,
    ) -> None:
        self.event_player_statistics = event_player_statistics or []
        self.event_player_stat_values = event_player_stat_values or []
        self.rating_breakdown = rating_breakdown or []
        self.heatmap_points = heatmap_points or []
        self.top_player_snapshot = top_player_snapshot
        self.top_player_entries = top_player_entries or []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def fetchrow(self, query: str, *args):
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM event_player_statistics" in normalized:
            return self.event_player_statistics[0] if self.event_player_statistics else None
        if "FROM top_player_snapshot" in normalized:
            return self.top_player_snapshot
        return None

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM event_player_stat_value" in normalized:
            return self.event_player_stat_values
        if "FROM event_player_rating_breakdown_action" in normalized:
            return self.rating_breakdown
        if "FROM event_team_heatmap_point" in normalized:
            return self.heatmap_points
        if "FROM top_player_entry" in normalized:
            return self.top_player_entries
        return []

    async def close(self):
        self.closed = True


class _FakeStandingsConnection:
    def __init__(
        self,
        *,
        standings: list[dict[str, object]],
        rows: list[dict[str, object]],
    ) -> None:
        self.standings = standings
        self.rows = rows
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        normalized = " ".join(query.split())
        if "information_schema.columns" in normalized:
            return [{"column_name": column} for column in sorted({"id", "season_id", "tournament_id", "name", "type"})]
        if "FROM standing_row" in normalized or "JOIN standing_row" in normalized:
            return self.rows
        if "FROM standing" in normalized:
            return self.standings
        return []

    async def close(self):
        self.closed = True


class _FakeReportNormalizedConnection:
    def __init__(
        self,
        *,
        transfer_history: list[dict[str, object]] | None = None,
        comment_feed: dict[str, object] | None = None,
        comments: list[dict[str, object]] | None = None,
        duels: list[dict[str, object]] | None = None,
        managers: list[dict[str, object]] | None = None,
        pregame_root: dict[str, object] | None = None,
        pregame_sides: list[dict[str, object]] | None = None,
        pregame_items: list[dict[str, object]] | None = None,
        rounds: list[dict[str, object]] | None = None,
    ) -> None:
        self.transfer_history = transfer_history or []
        self.comment_feed = comment_feed
        self.comments = comments or []
        self.duels = duels or []
        self.managers = managers or []
        self.pregame_root = pregame_root
        self.pregame_sides = pregame_sides or []
        self.pregame_items = pregame_items or []
        self.rounds = rounds or []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def fetchrow(self, query: str, *args):
        self.fetchrow_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM event_comment_feed" in normalized:
            return self.comment_feed
        if "FROM event_pregame_form" in normalized:
            return self.pregame_root
        return None

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        normalized = " ".join(query.split())
        if "FROM player_transfer_history" in normalized:
            return self.transfer_history
        if "FROM event_comment AS" in normalized:
            return self.comments
        if "FROM event_duel" in normalized:
            return self.duels
        if "FROM event_manager_assignment" in normalized:
            return self.managers
        if "FROM event_pregame_form_side" in normalized:
            return self.pregame_sides
        if "FROM event_pregame_form_item" in normalized:
            return self.pregame_items
        if "FROM season_round" in normalized:
            return self.rounds
        return []

    async def close(self):
        self.closed = True


class _FakeGenericNormalizedConnection:
    def __init__(
        self,
        *,
        table_columns: dict[str, set[str]],
        table_rows: dict[str, list[dict[str, object]]],
        jsonb_rows_as_text: bool = False,
    ) -> None:
        self.table_columns = table_columns
        self.table_rows = table_rows
        self.jsonb_rows_as_text = jsonb_rows_as_text
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        if "information_schema.columns" in query:
            table_name = str(args[0])
            return [{"column_name": column_name} for column_name in sorted(self.table_columns.get(table_name, set()))]
        for table_name, rows in self.table_rows.items():
            if f"FROM {table_name}" in query:
                if self.jsonb_rows_as_text:
                    return [{"row": orjson.dumps(row).decode()} for row in rows]
                return [{"row": row} for row in rows]
        return []

    async def close(self):
        self.closed = True


class _FakeSeasonEventsConnection:
    def __init__(self, *, rows: list[dict[str, object]], has_extra: bool) -> None:
        self.rows = rows
        self.has_extra = has_extra
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        if self.has_extra:
            extras = []
            template = self.rows[-1] if self.rows else {"id": 9999}
            for index in range(30):
                extra = dict(template)
                extra["id"] = 9000 + index
                extras.append(extra)
            return [*self.rows, *extras]
        return self.rows

    async def close(self):
        self.closed = True


class _FakeUniqueTournamentMediaConnection:
    """Stand-in connection for the synthetic
    ``/api/v1/unique-tournament/{id}/media`` handler.

    Returns the canned rows as-is so the test can shape exactly what the SQL
    layer would have produced (already DISTINCT ON-ed per event). Captures the
    query text and parameters so tests can assert filter / pattern correctness.
    """

    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.closed = False

    async def fetch(self, query: str, *args):
        self.fetch_calls.append((query, args))
        return self.rows

    async def close(self):
        self.closed = True


class _FakePoolConnectionBackend:
    def __init__(self) -> None:
        self.connection = _FakeCoverageConnection([])
        self.acquire_calls = 0
        self.release_calls = 0
        self.closed = False

    async def acquire(self):
        self.acquire_calls += 1
        return self.connection

    async def release(self, connection):
        self.asserted_connection = connection
        self.release_calls += 1

    async def close(self):
        self.closed = True


class _FakeAsgiApplication:
    def __init__(
        self,
        *,
        api_response: SerializedApiResponse | None = None,
        ops_response: ApiResponse | None = None,
    ) -> None:
        self.api_response = api_response or SerializedApiResponse(
            status_code=200,
            body=b"{}",
            cache_control="no-cache",
        )
        self.ops_response = ops_response or ApiResponse(status_code=200, payload={"ok": True})
        self.swagger_html = "<html>docs</html>"
        self.startup_calls = 0
        self.shutdown_calls = 0
        self.api_calls: list[tuple[str, str]] = []
        self.ops_calls: list[tuple[str, str]] = []
        self.openapi_headers: list[dict[str, str]] = []

    async def startup(self) -> None:
        self.startup_calls += 1

    async def shutdown(self) -> None:
        self.shutdown_calls += 1

    async def run_in_runtime(self, awaitable):
        return await awaitable

    async def handle_api_get_http_response(self, path: str, raw_query: str) -> SerializedApiResponse:
        self.api_calls.append((path, raw_query))
        return self.api_response

    async def handle_ops_get(self, path: str, raw_query: str) -> ApiResponse:
        self.ops_calls.append((path, raw_query))
        return self.ops_response

    def openapi_json_for_request(self, request_headers: dict[str, str] | None = None) -> bytes:
        self.openapi_headers.append(dict(request_headers or {}))
        return b'{"openapi":"3.1.0"}'


async def _return_runtime_value(value):
    return value


async def _passthrough_reconcile(executor, route, payload):
    del executor, route
    return payload


class _FakePendingQueue:
    def pending_summary(self, stream: str, group: str):
        del stream, group
        from schema_inspector.queue.streams import PendingSummary

        return PendingSummary(total=0, smallest_id=None, largest_id=None, consumers={})

    def stream_length(self, stream: str) -> int:
        del stream
        return 12

    def group_info(self, stream: str, group: str):
        del stream, group
        from schema_inspector.queue.streams import ConsumerGroupInfo

        return ConsumerGroupInfo(
            consumers=2,
            pending=0,
            last_delivered_id="1-99",
            entries_read=42,
            lag=7,
        )


class _FakeFetchExecutor:
    def __init__(self, rows):
        self.rows = rows

    async def fetch(self, query: str, *args):
        del query, args
        return self.rows
