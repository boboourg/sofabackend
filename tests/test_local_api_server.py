from __future__ import annotations

from dataclasses import replace
import unittest

from schema_inspector.local_api_server import (
    ApiResponse,
    LocalApiApplication,
    _QUEUE_GROUPS,
    _compile_path_template,
    _decode_snapshot_payload,
    _extract_event_id_from_entity_root_path,
    _normalized_query_map,
    _parse_context_value,
    _synthesize_event_root_payload,
    _synthesize_manager_root_payload,
    _synthesize_player_root_payload,
    _synthesize_team_root_payload,
    _synthesize_unique_tournament_root_payload,
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
        self.assertEqual(snapshots, ApiResponse(status_code=200, payload={"raw_snapshots": 12}))
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


class LocalApiNormalizedFallbackDispatchTests(unittest.IsolatedAsyncioTestCase):
    """End-to-end dispatch: ensure ``_fetch_normalized_payload`` wires each
    entity-root route to the correct fallback method without touching the
    database."""

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


if __name__ == "__main__":
    unittest.main()


def _make_fake_connector(connection):
    async def _connect():
        return connection

    return _connect


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
