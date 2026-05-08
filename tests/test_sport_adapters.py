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

    def test_football_final_only_edges_cover_post_match_endpoints(self) -> None:
        # F-3 (2026-05-08): finished football matches must end up with
        # canonical post-match /comments, /best-players/summary, /h2h,
        # /pregame-form even though those endpoints are NOT in live polling.
        adapter = resolve_sport_adapter("football")
        self.assertIn("comments", adapter.final_only_edges)
        self.assertIn("best_players_summary", adapter.final_only_edges)
        self.assertIn("h2h", adapter.final_only_edges)
        self.assertIn("pregame_form", adapter.final_only_edges)
        # And those edges MUST NOT leak into core_event_edges (would change
        # live polling), nor into live_optional_edges.
        for edge in adapter.final_only_edges:
            self.assertNotIn(edge, adapter.core_event_edges)
            self.assertNotIn(edge, adapter.live_optional_edges)

    def test_non_football_adapters_have_empty_final_only_edges(self) -> None:
        # Phase 1: only football opted in. Other sports must keep prior
        # finalize behaviour (core_event_edges only).
        for slug in ("basketball", "tennis", "ice-hockey", "baseball",
                     "cricket", "handball", "rugby", "volleyball",
                     "esports", "futsal", "table-tennis", "american-football"):
            adapter = resolve_sport_adapter(slug)
            self.assertEqual(adapter.final_only_edges, (),
                f"sport={slug!r} unexpectedly has final_only_edges={adapter.final_only_edges!r}")


if __name__ == "__main__":
    unittest.main()
