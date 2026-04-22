from __future__ import annotations

import unittest
from pathlib import Path


class EventDetailBackfillIndexesMigrationTests(unittest.TestCase):
    def test_api_payload_snapshot_lookup_index_migration_exists(self) -> None:
        path = (
            Path(__file__).resolve().parent.parent
            / "migrations"
            / "2026-04-22_api_payload_snapshot_event_detail_lookup_idx.sql"
        )
        self.assertTrue(path.exists(), str(path))

    def test_api_payload_snapshot_lookup_index_migration_declares_partial_index(self) -> None:
        path = (
            Path(__file__).resolve().parent.parent
            / "migrations"
            / "2026-04-22_api_payload_snapshot_event_detail_lookup_idx.sql"
        )
        sql = path.read_text(encoding="utf-8")

        self.assertIn("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_payload_snapshot_event_detail_lookup", sql)
        self.assertIn("ON api_payload_snapshot (context_entity_id)", sql)
        self.assertIn("endpoint_pattern = '/api/v1/event/{event_id}'", sql)
        self.assertIn("context_entity_type = 'event'", sql)
        self.assertNotIn("BEGIN;", sql)
        self.assertNotIn("COMMIT;", sql)

    def test_event_historical_backfill_lookup_index_migration_exists(self) -> None:
        path = (
            Path(__file__).resolve().parent.parent
            / "migrations"
            / "2026-04-22_event_historical_backfill_lookup_idx.sql"
        )
        self.assertTrue(path.exists(), str(path))

    def test_event_historical_backfill_lookup_index_migration_declares_composite_index(self) -> None:
        path = (
            Path(__file__).resolve().parent.parent
            / "migrations"
            / "2026-04-22_event_historical_backfill_lookup_idx.sql"
        )
        sql = path.read_text(encoding="utf-8")

        self.assertIn("CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_historical_backfill_lookup", sql)
        self.assertIn("ON event (unique_tournament_id, season_id, start_timestamp DESC, id DESC)", sql)
        self.assertNotIn("BEGIN;", sql)
        self.assertNotIn("COMMIT;", sql)


if __name__ == "__main__":
    unittest.main()
