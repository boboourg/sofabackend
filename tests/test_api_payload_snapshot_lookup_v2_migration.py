"""Smoke test for the api_payload_snapshot composite lookup index.

Migration: 2026-05-14_api_payload_snapshot_lookup_v2.sql
Plan: docs/N4_API_PERFORMANCE_PLAN.md Layer A

Test verifies the migration file exists and is well-formed:
- uses CREATE INDEX CONCURRENTLY (no downtime)
- not wrapped in transaction (CONCURRENTLY requirement)
- covers the canonical local_api_server query shape
- partial index gates on context_entity_id IS NOT NULL
"""

from __future__ import annotations

import unittest
from pathlib import Path


class ApiPayloadSnapshotLookupV2MigrationTests(unittest.TestCase):
    MIGRATION_FILENAME = "2026-05-14_api_payload_snapshot_lookup_v2.sql"

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
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_payload_snapshot_lookup_v2",
            sql,
            "must use CONCURRENTLY IF NOT EXISTS for safe rollout",
        )
        self.assertIn(
            "ON api_payload_snapshot (endpoint_pattern, context_entity_type, context_entity_id, id DESC)",
            sql,
            "composite key must match _fetch_snapshot_payload query shape",
        )
        self.assertIn(
            "WHERE context_entity_id IS NOT NULL",
            sql,
            "partial index must exclude rows without entity scope",
        )

    def test_does_not_wrap_in_transaction(self) -> None:
        """CONCURRENTLY index creation cannot run inside a transaction block."""

        sql = self._migration_path().read_text(encoding="utf-8")
        self.assertNotIn("BEGIN;", sql)
        self.assertNotIn("COMMIT;", sql)

    def test_migration_includes_rationale_comment(self) -> None:
        sql = self._migration_path().read_text(encoding="utf-8")
        self.assertIn("N4 Layer A", sql)
        self.assertIn("docs/N4_API_PERFORMANCE_PLAN.md", sql)


if __name__ == "__main__":
    unittest.main()
