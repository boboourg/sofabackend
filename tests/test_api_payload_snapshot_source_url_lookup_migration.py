"""Smoke test for the source-url composite partial index migration.

Migration: 2026-05-15_api_payload_snapshot_source_url_lookup.sql
Plan: docs/PROGRESS.md - "Layer D D.2" entry

Test verifies the migration file exists and is well-formed:
- CONCURRENTLY (no downtime)
- not wrapped in transaction (CONCURRENTLY requirement)
- composite key matches the new _fetch_snapshot_payload SQL shape
- partial WHERE context_entity_id IS NULL keeps the index small
"""

from __future__ import annotations

import unittest
from pathlib import Path


class ApiPayloadSnapshotSourceUrlLookupMigrationTests(unittest.TestCase):
    MIGRATION_FILENAME = "2026-05-15_api_payload_snapshot_source_url_lookup.sql"

    def _migration_path(self) -> Path:
        return (
            Path(__file__).resolve().parent.parent
            / "migrations"
            / self.MIGRATION_FILENAME
        )

    def test_migration_file_exists(self) -> None:
        path = self._migration_path()
        self.assertTrue(path.exists(), f"missing migration: {path}")

    def test_declares_concurrent_composite_partial_index(self) -> None:
        sql = self._migration_path().read_text(encoding="utf-8")
        self.assertIn(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_payload_snapshot_source_url_lookup",
            sql,
            "must use CONCURRENTLY IF NOT EXISTS for safe rollout",
        )
        self.assertIn(
            "endpoint_pattern text_pattern_ops",
            sql,
            "first column must use text_pattern_ops for LIKE-prefix index scans",
        )
        self.assertIn(
            "source_url text_pattern_ops",
            sql,
            "source_url must use text_pattern_ops or Postgres falls back to heap Filter",
        )
        self.assertIn(
            "id DESC",
            sql,
            "third column id DESC supports ORDER BY without sort step",
        )
        self.assertIn(
            "WHERE context_entity_id IS NULL",
            sql,
            "partial WHERE keeps index small — entity-scoped lookups use other indexes",
        )

    def test_does_not_wrap_in_transaction(self) -> None:
        """CONCURRENTLY index creation cannot run inside a transaction block."""

        sql = self._migration_path().read_text(encoding="utf-8")
        self.assertNotIn("BEGIN;", sql)
        self.assertNotIn("COMMIT;", sql)

    def test_migration_includes_rationale_comment(self) -> None:
        sql = self._migration_path().read_text(encoding="utf-8")
        self.assertIn("N4 Layer D D.2", sql)
        self.assertIn("source_url", sql)


if __name__ == "__main__":
    unittest.main()
