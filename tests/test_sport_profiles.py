from __future__ import annotations

import unittest

from schema_inspector.sport_profiles import (
    BASKETBALL_PROFILE,
    BASEBALL_PROFILE,
    CRICKET_PROFILE,
    FOOTBALL_PROFILE,
    FUTSAL_PROFILE,
    HANDBALL_PROFILE,
    ICE_HOCKEY_PROFILE,
    AMERICAN_FOOTBALL_PROFILE,
    ESPORTS_PROFILE,
    RUGBY_PROFILE,
    SUPPORTED_SPORT_SLUGS,
    TENNIS_PROFILE,
    TABLE_TENNIS_PROFILE,
    VOLLEYBALL_PROFILE,
    resolve_sport_profile,
)


class SportProfileTests(unittest.TestCase):
    def test_resolve_known_profiles(self) -> None:
        self.assertEqual(resolve_sport_profile("football"), FOOTBALL_PROFILE)
        self.assertEqual(resolve_sport_profile("basketball"), BASKETBALL_PROFILE)
        self.assertEqual(resolve_sport_profile("tennis"), TENNIS_PROFILE)
        self.assertEqual(resolve_sport_profile("cricket"), CRICKET_PROFILE)
        self.assertEqual(resolve_sport_profile("volleyball"), VOLLEYBALL_PROFILE)
        self.assertEqual(resolve_sport_profile("baseball"), BASEBALL_PROFILE)
        self.assertEqual(resolve_sport_profile("american-football"), AMERICAN_FOOTBALL_PROFILE)
        self.assertEqual(resolve_sport_profile("handball"), HANDBALL_PROFILE)
        self.assertEqual(resolve_sport_profile("table-tennis"), TABLE_TENNIS_PROFILE)
        self.assertEqual(resolve_sport_profile("ice-hockey"), ICE_HOCKEY_PROFILE)
        self.assertEqual(resolve_sport_profile("rugby"), RUGBY_PROFILE)
        self.assertEqual(resolve_sport_profile("futsal"), FUTSAL_PROFILE)
        self.assertEqual(resolve_sport_profile("esports"), ESPORTS_PROFILE)

    def test_supported_sport_slugs_cover_approved_program(self) -> None:
        self.assertEqual(
            SUPPORTED_SPORT_SLUGS,
            (
                "football",
                "tennis",
                "basketball",
                "volleyball",
                "baseball",
                "american-football",
                "handball",
                "table-tennis",
                "ice-hockey",
                "rugby",
                "cricket",
                "futsal",
                "esports",
            ),
        )

    def test_tennis_profile_is_conservative_by_default(self) -> None:
        profile = resolve_sport_profile("tennis")

        self.assertEqual(profile.sport_slug, "tennis")
        self.assertEqual(profile.standings_scopes, ())
        self.assertEqual(profile.team_event_scopes, ())
        self.assertIsNone(profile.top_players_suffix)
        self.assertFalse(profile.include_team_events)
        self.assertFalse(profile.include_statistics_types)
        self.assertFalse(profile.include_player_of_the_season)
        self.assertFalse(profile.include_trending_top_players)

    def test_baseball_profile_uses_regular_season_suffixes(self) -> None:
        profile = resolve_sport_profile("baseball")

        self.assertEqual(profile.sport_slug, "baseball")
        self.assertEqual(profile.top_players_suffix, "regularSeason")
        self.assertEqual(profile.top_players_per_game_suffix, "all/regularSeason")
        self.assertEqual(profile.top_teams_suffix, "regularSeason")
        self.assertTrue(profile.use_scheduled_tournaments)

    def test_handball_profile_stays_generic_but_explicit(self) -> None:
        profile = resolve_sport_profile("handball")

        self.assertEqual(profile.sport_slug, "handball")
        self.assertEqual(profile.standings_scopes, ("total",))
        self.assertEqual(profile.top_players_suffix, "overall")
        self.assertFalse(profile.include_top_ratings)

    def test_unknown_profile_still_uses_generic_defaults(self) -> None:
        profile = resolve_sport_profile("waterpolo")

        self.assertEqual(profile.sport_slug, "waterpolo")
        self.assertEqual(profile.standings_scopes, ("total",))
        self.assertEqual(profile.top_players_suffix, "overall")
        self.assertFalse(profile.include_top_ratings)


if __name__ == "__main__":
    unittest.main()
