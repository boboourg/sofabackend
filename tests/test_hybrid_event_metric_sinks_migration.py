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

    def test_original_metric_sink_migration_keeps_legacy_tennis_power_shape(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-17_hybrid_event_metric_sinks.sql"
        sql = path.read_text(encoding="utf-8")
        start = sql.index("CREATE TABLE IF NOT EXISTS tennis_power (")
        end = sql.index(");", start)
        tennis_power_block = sql[start:end]

        self.assertIn("side TEXT NOT NULL", tennis_power_block)
        self.assertIn("current_value_numeric NUMERIC", tennis_power_block)
        self.assertIn("delta_value_numeric NUMERIC", tennis_power_block)
        self.assertIn("PRIMARY KEY (event_id, side)", tennis_power_block)
        self.assertNotIn("set_number INTEGER", tennis_power_block)
        self.assertNotIn("break_occurred BOOLEAN", tennis_power_block)

    def test_tennis_power_series_shape_lives_in_followup_migration(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-17_tennis_power_series_shape.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("DROP TABLE IF EXISTS tennis_power", sql)
        self.assertIn("ordinal INTEGER NOT NULL", sql)
        self.assertIn("set_number INTEGER", sql)
        self.assertIn("game_number INTEGER", sql)
        self.assertIn("value_numeric NUMERIC", sql)
        self.assertIn("break_occurred BOOLEAN", sql)
        self.assertIn("PRIMARY KEY (event_id, ordinal)", sql)


if __name__ == "__main__":
    unittest.main()
