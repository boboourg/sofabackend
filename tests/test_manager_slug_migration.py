from __future__ import annotations

import unittest
from pathlib import Path


class ManagerSlugMigrationTests(unittest.TestCase):
    def test_manager_slug_migration_exists(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-17_drop_manager_slug_unique.sql"
        self.assertTrue(path.exists(), str(path))

    def test_manager_slug_migration_drops_unique_constraint(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-17_drop_manager_slug_unique.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("ALTER TABLE manager DROP CONSTRAINT IF EXISTS manager_slug_key", sql)
        self.assertIn("DROP INDEX IF EXISTS manager_slug_key", sql)


if __name__ == "__main__":
    unittest.main()
