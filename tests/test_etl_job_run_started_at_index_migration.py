"""Smoke test for the etl_job_run started_at BRIN index migration.

Migration purpose: docs/ARCHITECTURE_AUDIT.md D.3 noted a 30s full table
scan on every started_at-windowed probe. N1 monitoring Phase 3 needs
fast time-window queries on etl_job_run; the BRIN index keeps the
storage cost negligible while collapsing probe latency.

Test only verifies the migration file exists and is well-formed (no
transaction wrapper, CONCURRENTLY, IF NOT EXISTS, BRIN). Application
to a real Postgres is handled by ops, not by the test suite.
"""

from __future__ import annotations

import unittest
from pathlib import Path


class EtlJobRunStartedAtIndexMigrationTests(unittest.TestCase):
    MIGRATION_FILENAME = "2026-05-14_etl_job_run_started_at_index.sql"

    def _migration_path(self) -> Path:
        return (
            Path(__file__).resolve().parent.parent
            / "migrations"
            / self.MIGRATION_FILENAME
        )

    def test_migration_file_exists(self) -> None:
        path = self._migration_path()
        self.assertTrue(path.exists(), f"missing migration: {path}")

    def test_migration_declares_concurrent_brin_index(self) -> None:
        sql = self._migration_path().read_text(encoding="utf-8")
        self.assertIn(
            "CREATE INDEX CONCURRENTLY IF NOT EXISTS",
            sql,
            "must use CONCURRENTLY IF NOT EXISTS for safe rollout",
        )
        self.assertIn("idx_etl_job_run_started_at_brin", sql)
        self.assertIn("ON etl_job_run", sql)
        self.assertIn("USING BRIN (started_at)", sql)

    def test_migration_does_not_wrap_in_transaction(self) -> None:
        """CONCURRENTLY index creation cannot run inside a transaction block."""

        sql = self._migration_path().read_text(encoding="utf-8")
        self.assertNotIn("BEGIN;", sql)
        self.assertNotIn("COMMIT;", sql)

    def test_migration_includes_rationale_comment(self) -> None:
        sql = self._migration_path().read_text(encoding="utf-8")
        self.assertIn("ARCHITECTURE_AUDIT.md", sql)
        self.assertIn("N1 monitoring", sql)


if __name__ == "__main__":
    unittest.main()
