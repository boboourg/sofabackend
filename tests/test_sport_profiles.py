from __future__ import annotations

import unittest

from schema_inspector.sport_profiles import (
    BASKETBALL_PROFILE,
    CRICKET_PROFILE,
    FOOTBALL_PROFILE,
    TENNIS_PROFILE,
    resolve_sport_profile,
)


class SportProfileTests(unittest.TestCase):
    def test_resolve_known_profiles(self) -> None:
        self.assertEqual(resolve_sport_profile("football"), FOOTBALL_PROFILE)
        self.assertEqual(resolve_sport_profile("basketball"), BASKETBALL_PROFILE)
        self.assertEqual(resolve_sport_profile("tennis"), TENNIS_PROFILE)
        self.assertEqual(resolve_sport_profile("cricket"), CRICKET_PROFILE)

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

    def test_resolve_unknown_profile_uses_generic_defaults(self) -> None:
        profile = resolve_sport_profile("baseball")

        self.assertEqual(profile.sport_slug, "baseball")
        self.assertEqual(profile.standings_scopes, ("total",))
        self.assertEqual(profile.top_players_suffix, "overall")
        self.assertFalse(profile.include_top_ratings)


if __name__ == "__main__":
    unittest.main()
