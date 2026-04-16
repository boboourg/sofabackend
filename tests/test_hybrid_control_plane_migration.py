from __future__ import annotations

import unittest
from pathlib import Path


class HybridControlPlaneMigrationTests(unittest.TestCase):
    def test_control_plane_migration_exists(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-16_hybrid_control_plane.sql"
        self.assertTrue(path.exists(), str(path))

    def test_control_plane_migration_declares_required_tables(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-16_hybrid_control_plane.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE IF NOT EXISTS api_request_log", sql)
        self.assertIn("ALTER TABLE api_payload_snapshot", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS api_snapshot_head", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS etl_job_run", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS endpoint_capability_observation", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS endpoint_capability_rollup", sql)


if __name__ == "__main__":
    unittest.main()
