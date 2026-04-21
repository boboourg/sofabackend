from __future__ import annotations

import unittest
from pathlib import Path


class JobStageObservabilityMigrationTests(unittest.TestCase):
    def test_job_stage_migration_exists(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-21_job_stage_observability.sql"
        self.assertTrue(path.exists(), str(path))

    def test_job_stage_migration_declares_stage_table(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-21_job_stage_observability.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE IF NOT EXISTS etl_job_stage_run", sql)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_etl_job_stage_run_job", sql)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_etl_job_stage_run_stage", sql)


if __name__ == "__main__":
    unittest.main()
