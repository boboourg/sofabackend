from __future__ import annotations

import unittest
from pathlib import Path


class SeasonStructureMigrationTests(unittest.TestCase):
    def test_season_structure_migration_exists(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-23_season_structure.sql"
        self.assertTrue(path.exists(), str(path))

    def test_season_structure_migration_declares_rounds_and_cuptrees_tables(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-23_season_structure.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE IF NOT EXISTS season_round", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS season_cup_tree", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS season_cup_tree_round", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS season_cup_tree_block", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS season_cup_tree_participant", sql)

    def test_season_structure_migration_is_wrapped_in_transaction(self) -> None:
        path = Path(__file__).resolve().parent.parent / "migrations" / "2026-04-23_season_structure.sql"
        sql = path.read_text(encoding="utf-8")

        self.assertIn("BEGIN;", sql)
        self.assertIn("COMMIT;", sql)


if __name__ == "__main__":
    unittest.main()
