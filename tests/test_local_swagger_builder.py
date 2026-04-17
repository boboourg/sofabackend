from __future__ import annotations

import unittest

from schema_inspector.local_swagger_builder import (
    SwaggerDataSummary,
    _build_viewer_html,
    build_openapi_document,
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


if __name__ == "__main__":
    unittest.main()
