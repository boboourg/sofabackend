from __future__ import annotations

import unittest

from schema_inspector.services.historical_planner import (
    choose_recent_history_window,
    choose_saturation_budget,
)


class RecentHistoryPolicyTests(unittest.TestCase):
    def test_priority_sports_get_deeper_recent_window(self) -> None:
        self.assertEqual(choose_recent_history_window("football"), 730)

    def test_other_sports_use_default_recent_window(self) -> None:
        self.assertEqual(choose_recent_history_window("table-tennis"), 180)

    def test_priority_sports_get_deeper_saturation_budget_than_other_sports(self) -> None:
        football = choose_saturation_budget("football")
        table_tennis = choose_saturation_budget("table-tennis")

        self.assertGreater(football.player_limit, table_tennis.player_limit)
        self.assertGreater(football.team_limit, table_tennis.team_limit)
        self.assertGreater(football.player_request_limit, table_tennis.player_request_limit)
        self.assertGreater(football.team_request_limit, table_tennis.team_request_limit)


if __name__ == "__main__":
    unittest.main()
