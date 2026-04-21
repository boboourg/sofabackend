from __future__ import annotations

import unittest
from pathlib import Path


class TournamentRegistryMigrationTests(unittest.TestCase):
    def test_tournament_registry_migration_exists(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-23_tournament_registry.sql"
        self.assertTrue(path.exists(), str(path))

    def test_tournament_registry_migration_declares_registry_table(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-23_tournament_registry.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE IF NOT EXISTS tournament_registry", sql)
        self.assertIn("REFERENCES provider_source(source_slug)", sql)
        self.assertIn("PRIMARY KEY (source_slug, sport_slug, unique_tournament_id)", sql)

    def test_tournament_registry_migration_is_wrapped_in_transaction(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-23_tournament_registry.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("BEGIN;", sql)
        self.assertIn("COMMIT;", sql)


if __name__ == "__main__":
    unittest.main()
