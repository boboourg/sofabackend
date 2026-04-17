from __future__ import annotations

import unittest

from schema_inspector.endpoints import LOCAL_API_SUPPORTED_SPORTS
from schema_inspector.local_swagger_builder import (
    SwaggerDataSummary,
    _build_viewer_html,
    build_openapi_document,
    resolve_openapi_base_urls,
)


class LocalSwaggerBuilderTests(unittest.TestCase):
    def test_document_contains_core_football_paths(self) -> None:
        summary = SwaggerDataSummary(
            generated_at="2026-04-11T18:00:00+00:00",
            table_counts={"event": 10, "player_season_statistics": 2},
            snapshot_counts={"/api/v1/player/{player_id}/statistics": 1},
        )

        document = build_openapi_document(summary)

        self.assertIsInstance(document["paths"], dict)
        self.assertIn("schemas", document["components"])
        self.assertIn("/api/v1/player/{player_id}/statistics", document["paths"])
        self.assertIn("/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics", document["paths"])
        self.assertIn("PlayerStatisticsEnvelope", document["components"]["schemas"])
        self.assertIn("SeasonStatisticsEnvelope", document["components"]["schemas"])

    def test_viewer_html_points_to_openapi_file(self) -> None:
        html = _build_viewer_html("football.openapi.json")
        self.assertIn("./football.openapi.json", html)
        self.assertIn("SwaggerUIBundle", html)
        self.assertIn("operationsSorter", html)
        self.assertIn("tagsSorter", html)
        self.assertIn("filter: true", html)

    def test_document_contains_operations_paths_and_schemas(self) -> None:
        summary = SwaggerDataSummary(
            generated_at="2026-04-17T18:00:00+00:00",
            table_counts={
                "api_payload_snapshot": 42,
                "etl_job_run": 7,
                "event_live_state_history": 3,
            },
            snapshot_counts={"/api/v1/event/{event_id}/lineups": 5},
        )

        document = build_openapi_document(summary)

        self.assertIn("/ops/health", document["paths"])
        self.assertIn("/ops/snapshots/summary", document["paths"])
        self.assertIn("/ops/queues/summary", document["paths"])
        self.assertIn("/ops/jobs/runs", document["paths"])
        self.assertIn("OperationalHealth", document["components"]["schemas"])
        self.assertIn("QueueSummary", document["components"]["schemas"])
        self.assertIn("JobRunEntry", document["components"]["schemas"])

    def test_document_contains_all_supported_sports_and_special_routes(self) -> None:
        summary = SwaggerDataSummary(
            generated_at="2026-04-17T18:00:00+00:00",
            table_counts={"api_payload_snapshot": 42},
            snapshot_counts={},
        )

        document = build_openapi_document(summary)
        paths = document["paths"]

        for sport_slug in LOCAL_API_SUPPORTED_SPORTS:
            with self.subTest(sport_slug=sport_slug):
                self.assertIn(f"/api/v1/sport/{sport_slug}/scheduled-events/{{date}}", paths)
                self.assertIn(f"/api/v1/sport/{sport_slug}/events/live", paths)

        tag_names = [tag["name"] for tag in document["tags"]]
        self.assertIn("Special Routes", tag_names)
        self.assertIn("/api/v1/event/{event_id}/point-by-point", paths)
        self.assertIn("/api/v1/event/{event_id}/tennis-power", paths)
        self.assertIn("/api/v1/event/{event_id}/innings", paths)
        self.assertIn("/api/v1/event/{event_id}/atbat/{at_bat_id}/pitches", paths)
        self.assertIn("/api/v1/event/{event_id}/shotmap", paths)
        self.assertIn("/api/v1/event/{event_id}/esports-games", paths)
        self.assertEqual(paths["/api/v1/event/{event_id}/innings"]["get"]["tags"], ["Special Routes"])
        self.assertEqual(paths["/api/v1/event/{event_id}/shotmap"]["get"]["tags"], ["Special Routes"])
        self.assertEqual(paths["/api/v1/event/{event_id}/esports-games"]["get"]["tags"], ["Special Routes"])

    def test_document_includes_local_and_public_servers(self) -> None:
        summary = SwaggerDataSummary(
            generated_at="2026-04-17T18:00:00+00:00",
            table_counts={},
            snapshot_counts={},
        )

        document = build_openapi_document(
            summary,
            base_urls=(
                "http://127.0.0.1:8000",
                "http://localhost:8000",
                "https://api.example.com",
            ),
        )

        server_urls = [entry["url"] for entry in document["servers"]]
        self.assertEqual(
            server_urls,
            [
                "https://api.example.com",
                "http://127.0.0.1:8000",
                "http://localhost:8000",
            ],
        )

    def test_resolve_openapi_base_urls_prefers_public_then_local_fallbacks(self) -> None:
        self.assertEqual(
            resolve_openapi_base_urls(
                primary_base_url="http://0.0.0.0:8000",
                public_base_urls=("https://api.example.com",),
            ),
            (
                "https://api.example.com",
                "http://127.0.0.1:8000",
                "http://localhost:8000",
            ),
        )


if __name__ == "__main__":
    unittest.main()
