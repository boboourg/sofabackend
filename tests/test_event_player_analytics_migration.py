from __future__ import annotations

import unittest
from pathlib import Path


class EventPlayerAnalyticsMigrationTests(unittest.TestCase):
    def test_event_player_analytics_migration_exists(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-17_event_player_analytics.sql"
        self.assertTrue(path.exists(), str(path))

    def test_event_player_analytics_migration_declares_required_tables(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-17_event_player_analytics.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE IF NOT EXISTS event_best_player_entry", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS event_player_statistics", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS event_player_stat_value", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS event_player_rating_breakdown_action", sql)


if __name__ == "__main__":
    unittest.main()
