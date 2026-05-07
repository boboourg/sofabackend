from __future__ import annotations

import unittest

from schema_inspector.parsers.sports import resolve_sport_adapter


class SportAdapterTests(unittest.TestCase):
    def test_football_like_adapter_family(self) -> None:
        adapter = resolve_sport_adapter("handball")

        self.assertEqual(adapter.archetype, "football_like")
        self.assertIn("incidents", adapter.core_event_edges)
        self.assertTrue(adapter.hydrate_entity_profiles)

    def test_regular_season_team_adapter_family(self) -> None:
        adapter = resolve_sport_adapter("baseball")

        self.assertEqual(adapter.archetype, "regular_season_team")
        # P0.2: baseball_innings removed from baseball special_families —
        # /innings endpoint is cricket-only on prod (live probe confirmed
        # 100% soft-error 404 for baseball events). Baseball still gets
        # /at-bats and /atbat/{id}/pitches via EVENT_DETAIL_BASEBALL_ENDPOINTS.
        self.assertNotIn("baseball_innings", adapter.special_families)
        self.assertIn("incidents", adapter.core_event_edges)
        self.assertTrue(adapter.hydrate_entity_profiles)

    def test_cricket_adapter_has_innings_family(self) -> None:
        # P0.2: /innings moved to cricket adapter.
        adapter = resolve_sport_adapter("cricket")
        self.assertIn("cricket_innings", adapter.special_families)

    def test_thin_special_adapter_family(self) -> None:
        adapter = resolve_sport_adapter("esports")

        self.assertEqual(adapter.archetype, "thin_special")
        self.assertIn("esports_games", adapter.special_families)
        self.assertFalse(adapter.hydrate_entity_profiles)


if __name__ == "__main__":
    unittest.main()
