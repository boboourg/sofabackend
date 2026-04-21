from __future__ import annotations

import unittest

from schema_inspector.services.historical_planner import choose_recent_history_window


class RecentHistoryPolicyTests(unittest.TestCase):
    def test_priority_sports_get_deeper_recent_window(self) -> None:
        self.assertEqual(choose_recent_history_window("football"), 730)

    def test_other_sports_use_default_recent_window(self) -> None:
        self.assertEqual(choose_recent_history_window("table-tennis"), 180)


if __name__ == "__main__":
    unittest.main()
