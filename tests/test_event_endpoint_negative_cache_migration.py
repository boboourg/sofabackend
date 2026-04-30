from __future__ import annotations

import unittest
from pathlib import Path


class EventEndpointNegativeCacheMigrationTests(unittest.TestCase):
    def test_event_endpoint_negative_cache_migration_exists(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-28_event_endpoint_negative_cache.sql"
        self.assertTrue(path.exists(), str(path))

    def test_event_endpoint_negative_cache_migration_declares_state_and_log_tables(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-28_event_endpoint_negative_cache.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE IF NOT EXISTS event_endpoint_negative_cache_state", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS event_endpoint_availability_log", sql)
        self.assertIn("status_phase TEXT NOT NULL", sql)
        self.assertIn("last_outcome_classification TEXT NULL", sql)
        self.assertIn("'shadow_suppress'", sql)
        self.assertIn("CREATE UNIQUE INDEX IF NOT EXISTS uq_event_endpoint_negative_cache_state", sql)

    def test_event_endpoint_negative_cache_migration_is_wrapped_in_transaction(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-28_event_endpoint_negative_cache.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("BEGIN;", sql)
        self.assertIn("COMMIT;", sql)


if __name__ == "__main__":
    unittest.main()
