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
        self.assertIn("baseball_innings", adapter.special_families)
        self.assertIn("incidents", adapter.core_event_edges)
        self.assertTrue(adapter.hydrate_entity_profiles)

    def test_thin_special_adapter_family(self) -> None:
        adapter = resolve_sport_adapter("esports")

        self.assertEqual(adapter.archetype, "thin_special")
        self.assertIn("esports_games", adapter.special_families)
        self.assertFalse(adapter.hydrate_entity_profiles)


if __name__ == "__main__":
    unittest.main()
