from __future__ import annotations

import unittest

from schema_inspector.endpoints import (
    category_live_events_count_endpoint,
    calendar_months_with_events_endpoint,
    event_detail_endpoints,
    leaderboards_registry_entries_for_sport,
    season_last_events_endpoint,
    season_next_events_endpoint,
    season_rounds_endpoint,
    sport_finished_upcoming_tournaments_endpoint,
    sport_live_categories_endpoint,
    sport_live_tournaments_endpoint,
    sport_trending_top_players_endpoint,
)


class EndpointRegistryTests(unittest.TestCase):
    def test_live_discovery_endpoints_normalize_sport_slug(self) -> None:
        self.assertEqual(
            sport_live_categories_endpoint("handball").path_template,
            "/api/v1/sport/handball/live-categories",
        )
        self.assertEqual(
            sport_live_tournaments_endpoint("ice-hockey").path_template,
            "/api/v1/sport/ice-hockey/live-tournaments",
        )
        self.assertEqual(
            sport_finished_upcoming_tournaments_endpoint("esports").path_template,
            "/api/v1/sport/esports/finished-upcoming-tournaments",
        )

    def test_season_support_endpoints_cover_rounds_and_adjacent_events(self) -> None:
        self.assertEqual(
            season_rounds_endpoint().path_template,
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/rounds",
        )
        self.assertEqual(
            season_last_events_endpoint().path_template,
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/last/{page}",
        )
        self.assertEqual(
            season_next_events_endpoint().path_template,
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/next/{page}",
        )

    def test_calendar_and_category_count_endpoints_are_available(self) -> None:
        self.assertEqual(
            calendar_months_with_events_endpoint().path_template,
            "/api/v1/calendar/unique-tournament/{unique_tournament_id}/season/{season_id}/months-with-events",
        )
        self.assertEqual(
            category_live_events_count_endpoint().path_template,
            "/api/v1/category/{category_id}/events/live-count",
        )

    def test_leaderboards_registry_uses_profile_specific_suffixes(self) -> None:
        basketball_patterns = {entry.pattern for entry in leaderboards_registry_entries_for_sport("basketball")}
        baseball_patterns = {entry.pattern for entry in leaderboards_registry_entries_for_sport("baseball")}
        tennis_patterns = {entry.pattern for entry in leaderboards_registry_entries_for_sport("tennis")}
        handball_patterns = {entry.pattern for entry in leaderboards_registry_entries_for_sport("handball")}

        self.assertIn(
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/regularSeason",
            basketball_patterns,
        )
        self.assertIn(
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/regularSeason",
            baseball_patterns,
        )
        self.assertIn(
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall",
            handball_patterns,
        )
        self.assertEqual(tennis_patterns, set())

    def test_trending_top_players_endpoint_is_still_factory_based(self) -> None:
        endpoint = sport_trending_top_players_endpoint("basketball")
        self.assertEqual(endpoint.path_template, "/api/v1/sport/basketball/trending-top-players")

    def test_event_detail_endpoints_include_sport_specific_special_routes(self) -> None:
        baseball_patterns = {endpoint.pattern for endpoint in event_detail_endpoints(sport_slug="baseball")}
        ice_hockey_patterns = {endpoint.pattern for endpoint in event_detail_endpoints(sport_slug="ice-hockey")}
        esports_patterns = {endpoint.pattern for endpoint in event_detail_endpoints(sport_slug="esports")}

        self.assertIn("/api/v1/event/{event_id}/innings", baseball_patterns)
        self.assertIn("/api/v1/event/{event_id}/shotmap", ice_hockey_patterns)
        self.assertIn("/api/v1/event/{event_id}/esports-games", esports_patterns)


if __name__ == "__main__":
    unittest.main()
