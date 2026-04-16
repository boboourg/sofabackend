from __future__ import annotations

import unittest
from pathlib import Path


class HybridEventMetricSinksMigrationTests(unittest.TestCase):
    def test_hybrid_event_metric_sinks_migration_exists(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-17_hybrid_event_metric_sinks.sql"
        self.assertTrue(path.exists(), str(path))

    def test_hybrid_event_metric_sinks_migration_declares_required_tables(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-17_hybrid_event_metric_sinks.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE IF NOT EXISTS event_statistic", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS event_incident", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS tennis_point_by_point", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS tennis_power", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS baseball_inning", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS shotmap_point", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS esports_game", sql)


if __name__ == "__main__":
    unittest.main()
