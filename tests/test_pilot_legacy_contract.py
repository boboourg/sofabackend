from __future__ import annotations

import unittest

from schema_inspector.endpoints import (
    ENTITIES_ENDPOINTS,
    EVENT_STATISTICS_ENDPOINT,
    LOCAL_API_SUPPORTED_SPORTS,
    event_detail_registry_entries,
    leaderboards_registry_entries_for_sport,
)


class PilotLegacyContractTests(unittest.TestCase):
    def test_pilot_supported_sports_match_local_api_pilot_trio(self) -> None:
        self.assertEqual(LOCAL_API_SUPPORTED_SPORTS, ("football", "basketball", "tennis"))

    def test_football_pilot_patterns_are_covered_by_legacy_registry_layers(self) -> None:
        patterns = {entry.pattern for entry in event_detail_registry_entries(sport_slug="football")}
        patterns.update(endpoint.pattern for endpoint in ENTITIES_ENDPOINTS)
        patterns.update(entry.pattern for entry in leaderboards_registry_entries_for_sport("football"))

        required = {
            "/api/v1/event/{event_id}",
            "/api/v1/event/{event_id}/statistics",
            "/api/v1/event/{event_id}/lineups",
            "/api/v1/event/{event_id}/incidents",
            "/api/v1/team/{team_id}",
            "/api/v1/player/{player_id}",
            "/api/v1/manager/{manager_id}",
        }
        self.assertTrue(required.issubset(patterns))

    def test_tennis_pilot_patterns_include_special_routes_from_event_detail_family(self) -> None:
        patterns = {entry.pattern for entry in event_detail_registry_entries(sport_slug="tennis")}
        required = {
            "/api/v1/event/{event_id}/point-by-point",
            "/api/v1/event/{event_id}/tennis-power",
            "/api/v1/event/{event_id}/statistics",
        }
        self.assertTrue(required.issubset(patterns))

    def test_statistics_endpoint_is_now_part_of_event_detail_registry_family(self) -> None:
        football_patterns = {entry.pattern for entry in event_detail_registry_entries(sport_slug="football")}
        self.assertIn(EVENT_STATISTICS_ENDPOINT.pattern, football_patterns)


if __name__ == "__main__":
    unittest.main()
