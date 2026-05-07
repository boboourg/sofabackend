from __future__ import annotations

import unittest

from schema_inspector.endpoints import (
    EVENT_DETAIL_BASEBALL_ENDPOINTS,
    EVENT_DETAIL_CRICKET_ENDPOINTS,
    EVENT_INNINGS_ENDPOINT,
    EVENT_BASEBALL_INNINGS_ENDPOINT,
    event_detail_endpoints,
)
from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.classifier import classify_snapshot
from schema_inspector.parsers.sports import resolve_sport_adapter


class InningsCricketMigrationTests(unittest.TestCase):
    def test_legacy_alias_points_to_new_endpoint(self) -> None:
        # Backwards-compat: old import path should resolve to the
        # renamed endpoint object so dependent tooling doesn't break.
        self.assertIs(EVENT_BASEBALL_INNINGS_ENDPOINT, EVENT_INNINGS_ENDPOINT)

    def test_innings_endpoint_target_is_raw_passthrough(self) -> None:
        # No CricketInningsParser yet — store as raw, parser can be added later.
        self.assertEqual(EVENT_INNINGS_ENDPOINT.target_table, "api_payload_snapshot")

    def test_innings_endpoint_uses_head_probe_gating(self) -> None:
        # P0.2 — gate body GET via HEAD; baseball events HEAD=404 → cheap skip.
        self.assertTrue(EVENT_INNINGS_ENDPOINT.prefer_head_probe)

    def test_baseball_detail_no_longer_includes_innings(self) -> None:
        baseball_patterns = {ep.path_template for ep in EVENT_DETAIL_BASEBALL_ENDPOINTS}
        self.assertNotIn("/api/v1/event/{event_id}/innings", baseball_patterns)
        # /at-bats remains.
        self.assertIn("/api/v1/event/{event_id}/at-bats", baseball_patterns)

    def test_cricket_detail_includes_innings(self) -> None:
        cricket_patterns = {ep.path_template for ep in EVENT_DETAIL_CRICKET_ENDPOINTS}
        self.assertIn("/api/v1/event/{event_id}/innings", cricket_patterns)

    def test_event_detail_endpoints_for_cricket(self) -> None:
        patterns = {ep.path_template for ep in event_detail_endpoints(sport_slug="cricket")}
        self.assertIn("/api/v1/event/{event_id}/innings", patterns)

    def test_event_detail_endpoints_for_baseball(self) -> None:
        patterns = {ep.path_template for ep in event_detail_endpoints(sport_slug="baseball")}
        self.assertNotIn("/api/v1/event/{event_id}/innings", patterns)
        self.assertIn("/api/v1/event/{event_id}/at-bats", patterns)

    def test_classifier_routes_cricket_innings_to_distinct_family(self) -> None:
        snapshot = RawSnapshot(
            snapshot_id=1,
            endpoint_pattern="/api/v1/event/{event_id}/innings",
            sport_slug="cricket",
            source_url="https://example/x",
            resolved_url=None,
            envelope_key="innings",
            http_status=200,
            payload={"innings": [{"number": 1, "battingTeam": {}, "bowlingTeam": {}}]},
            fetched_at="2026-05-08T00:00:00Z",
        )
        self.assertEqual(classify_snapshot(snapshot), "cricket_innings")

    def test_classifier_keeps_baseball_innings_legacy_for_baseball(self) -> None:
        snapshot = RawSnapshot(
            snapshot_id=1,
            endpoint_pattern="/api/v1/event/{event_id}/innings",
            sport_slug="baseball",
            source_url="https://example/x",
            resolved_url=None,
            envelope_key="innings",
            http_status=200,
            payload={"innings": [{"inning": 1, "homeScore": 0, "awayScore": 1}]},
            fetched_at="2026-05-08T00:00:00Z",
        )
        # Legacy baseball snapshots in the DB can still be replayed.
        self.assertEqual(classify_snapshot(snapshot), "baseball_innings")

    def test_cricket_adapter_declares_innings_special(self) -> None:
        adapter = resolve_sport_adapter("cricket")
        self.assertIn("cricket_innings", adapter.special_families)

    def test_baseball_adapter_no_longer_declares_innings_special(self) -> None:
        adapter = resolve_sport_adapter("baseball")
        self.assertNotIn("baseball_innings", adapter.special_families)


if __name__ == "__main__":
    unittest.main()
