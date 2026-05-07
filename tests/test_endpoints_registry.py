from __future__ import annotations

import unittest

from schema_inspector.endpoints import (
    category_live_events_count_endpoint,
    calendar_months_with_events_endpoint,
    event_detail_endpoints,
    leaderboards_registry_entries_for_sport,
    season_cuptrees_endpoint,
    season_last_events_endpoint,
    season_next_events_endpoint,
    season_rounds_endpoint,
    sport_finished_upcoming_tournaments_endpoint,
    sport_live_categories_endpoint,
    sport_live_tournaments_endpoint,
    sport_trending_top_players_endpoint,
    team_last_events_endpoint,
    team_next_events_endpoint,
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
            season_cuptrees_endpoint().path_template,
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/cuptrees",
        )
        self.assertEqual(
            season_last_events_endpoint().path_template,
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/last/{page}",
        )
        self.assertEqual(
            season_next_events_endpoint().path_template,
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/next/{page}",
        )
        self.assertEqual(
            team_last_events_endpoint().path_template,
            "/api/v1/team/{team_id}/events/last/{page}",
        )
        self.assertEqual(
            team_next_events_endpoint().path_template,
            "/api/v1/team/{team_id}/events/next/{page}",
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
        football_patterns = {endpoint.pattern for endpoint in event_detail_endpoints(sport_slug="football")}
        baseball_patterns = {endpoint.pattern for endpoint in event_detail_endpoints(sport_slug="baseball")}
        ice_hockey_patterns = {endpoint.pattern for endpoint in event_detail_endpoints(sport_slug="ice-hockey")}
        esports_patterns = {endpoint.pattern for endpoint in event_detail_endpoints(sport_slug="esports")}

        self.assertIn("/api/v1/event/{event_id}/statistics", football_patterns)
        self.assertIn("/api/v1/event/{event_id}/innings", baseball_patterns)
        self.assertIn("/api/v1/event/{event_id}/shotmap", ice_hockey_patterns)
        self.assertIn("/api/v1/event/{event_id}/esports-games", esports_patterns)

    def test_d1_new_endpoints_are_registered_in_local_api_endpoints(self) -> None:
        """D1: new endpoint constants must reach local_api_endpoints() and route specs.

        ``_ALL_ENDPOINTS`` (built from ``local_api_endpoints()``) is the source
        of truth for both ``build_route_specs()`` and the Swagger/OpenAPI doc,
        so a single membership check covers both surfaces.
        """

        from schema_inspector.endpoints import (
            EVENT_BASEBALL_AT_BATS_ENDPOINT,
            PLAYER_LAST_YEAR_SUMMARY_ENDPOINT,
            PLAYER_NATIONAL_TEAM_STATISTICS_ENDPOINT,
            PLAYER_STATISTICS_MATCH_TYPE_OVERALL_ENDPOINT,
            TEAM_SEASON_GOAL_DISTRIBUTIONS_ENDPOINT,
            local_api_endpoints,
        )
        from schema_inspector.local_api_server import build_route_specs

        all_patterns = {endpoint.pattern for endpoint in local_api_endpoints()}
        for endpoint in (
            EVENT_BASEBALL_AT_BATS_ENDPOINT,
            TEAM_SEASON_GOAL_DISTRIBUTIONS_ENDPOINT,
            PLAYER_STATISTICS_MATCH_TYPE_OVERALL_ENDPOINT,
            PLAYER_NATIONAL_TEAM_STATISTICS_ENDPOINT,
            PLAYER_LAST_YEAR_SUMMARY_ENDPOINT,
        ):
            self.assertIn(endpoint.pattern, all_patterns, msg=f"missing in local_api_endpoints: {endpoint.pattern}")

        route_patterns = {route.endpoint.pattern for route in build_route_specs()}
        for endpoint in (
            EVENT_BASEBALL_AT_BATS_ENDPOINT,
            TEAM_SEASON_GOAL_DISTRIBUTIONS_ENDPOINT,
            PLAYER_STATISTICS_MATCH_TYPE_OVERALL_ENDPOINT,
            PLAYER_NATIONAL_TEAM_STATISTICS_ENDPOINT,
            PLAYER_LAST_YEAR_SUMMARY_ENDPOINT,
        ):
            self.assertIn(endpoint.pattern, route_patterns, msg=f"missing in build_route_specs: {endpoint.pattern}")

    def test_d1_new_endpoints_envelope_keys_match_upstream(self) -> None:
        """Pre-D1 upstream probe verified these envelope_key shapes."""

        from schema_inspector.endpoints import (
            EVENT_BASEBALL_AT_BATS_ENDPOINT,
            PLAYER_LAST_YEAR_SUMMARY_ENDPOINT,
            PLAYER_NATIONAL_TEAM_STATISTICS_ENDPOINT,
            PLAYER_STATISTICS_MATCH_TYPE_OVERALL_ENDPOINT,
            TEAM_SEASON_GOAL_DISTRIBUTIONS_ENDPOINT,
        )

        self.assertEqual(EVENT_BASEBALL_AT_BATS_ENDPOINT.envelope_key, "atBats")
        self.assertEqual(TEAM_SEASON_GOAL_DISTRIBUTIONS_ENDPOINT.envelope_key, "goalDistributions")
        self.assertEqual(PLAYER_STATISTICS_MATCH_TYPE_OVERALL_ENDPOINT.envelope_key, "seasons,typesMap")
        self.assertEqual(PLAYER_NATIONAL_TEAM_STATISTICS_ENDPOINT.envelope_key, "statistics")
        self.assertEqual(PLAYER_LAST_YEAR_SUMMARY_ENDPOINT.envelope_key, "summary,uniqueTournamentsMap")

    def test_d2_refresh_metadata_set_on_new_endpoints(self) -> None:
        """D2 wires each new endpoint into the Resource Refresh Loop with a
        scope_kind, refresh interval and freshness window. ``freshness_ttl``
        must always sit just under ``refresh_interval`` so a duplicate
        publish is deduped at the Freshness store before hitting upstream.
        """

        from schema_inspector.endpoints import (
            EVENT_BASEBALL_AT_BATS_ENDPOINT,
            PLAYER_LAST_YEAR_SUMMARY_ENDPOINT,
            PLAYER_NATIONAL_TEAM_STATISTICS_ENDPOINT,
            PLAYER_STATISTICS_MATCH_TYPE_OVERALL_ENDPOINT,
            TEAM_SEASON_GOAL_DISTRIBUTIONS_ENDPOINT,
        )

        cases = [
            (EVENT_BASEBALL_AT_BATS_ENDPOINT, "event-of-active-baseball", 10 * 60),
            (TEAM_SEASON_GOAL_DISTRIBUTIONS_ENDPOINT, "team-of-active-ut-season", 24 * 3600),
            (PLAYER_STATISTICS_MATCH_TYPE_OVERALL_ENDPOINT, "player-of-active-squad", 24 * 3600),
            (PLAYER_NATIONAL_TEAM_STATISTICS_ENDPOINT, "player-of-active-squad", 7 * 24 * 3600),
            (PLAYER_LAST_YEAR_SUMMARY_ENDPOINT, "player-of-active-squad", 24 * 3600),
        ]
        for endpoint, expected_kind, expected_interval in cases:
            with self.subTest(endpoint=endpoint.path_template):
                self.assertEqual(endpoint.scope_kind, expected_kind)
                self.assertEqual(endpoint.refresh_interval_seconds, expected_interval)
                self.assertIsNotNone(endpoint.freshness_ttl_seconds)
                self.assertLess(endpoint.freshness_ttl_seconds, endpoint.refresh_interval_seconds)
                self.assertGreater(endpoint.refresh_priority, 0)

    def test_d2_new_scope_kinds_are_distinct_strings(self) -> None:
        """Scope kinds must be unique (the resolver registry is keyed by it)."""

        from schema_inspector.services.resource_scope import (
            EventOfFinishedBaseballResolver,
            PlayerOfNationalTeamHistoryResolver,
            TeamOfActiveUTSeasonResolver,
        )

        kinds = {
            TeamOfActiveUTSeasonResolver.kind,
            EventOfFinishedBaseballResolver.kind,
            PlayerOfNationalTeamHistoryResolver.kind,
        }
        # All three must be present and distinct.
        self.assertEqual(len(kinds), 3)
        self.assertIn("team-of-active-ut-season", kinds)
        self.assertIn("event-of-active-baseball", kinds)
        self.assertIn("player-of-national-team-history", kinds)


if __name__ == "__main__":
    unittest.main()
