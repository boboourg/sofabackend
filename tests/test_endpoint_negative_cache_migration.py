from __future__ import annotations

import unittest
from pathlib import Path


class EndpointNegativeCacheMigrationTests(unittest.TestCase):
    def test_endpoint_negative_cache_migration_exists(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-24_endpoint_negative_cache.sql"
        self.assertTrue(path.exists(), str(path))

    def test_endpoint_negative_cache_migration_declares_state_and_log_tables(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-24_endpoint_negative_cache.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE IF NOT EXISTS endpoint_negative_cache_state", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS endpoint_availability_log", sql)
        self.assertIn("seen_404_season_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb", sql)
        self.assertIn("seen_200_season_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb", sql)
        self.assertIn("probe_lease_until TIMESTAMPTZ", sql)
        self.assertIn("'shadow_suppress'", sql)
        self.assertIn("CREATE UNIQUE INDEX IF NOT EXISTS uq_endpoint_negative_cache_tournament", sql)
        self.assertIn("CREATE UNIQUE INDEX IF NOT EXISTS uq_endpoint_negative_cache_season", sql)

    def test_endpoint_negative_cache_migration_is_wrapped_in_transaction(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-24_endpoint_negative_cache.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("BEGIN;", sql)
        self.assertIn("COMMIT;", sql)


if __name__ == "__main__":
    unittest.main()
