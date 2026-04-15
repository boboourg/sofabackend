from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from schema_inspector.db_setup_cli import (
    _database_name_from_dsn,
    _discover_migration_files,
    _quote_identifier,
    _replace_database_in_dsn,
)


class DbSetupCliTests(unittest.TestCase):
    def test_database_name_is_extracted_from_dsn(self) -> None:
        dsn = "postgresql://user:pass@localhost:5433/sofascore_schema_inspector?sslmode=disable"
        self.assertEqual(_database_name_from_dsn(dsn), "sofascore_schema_inspector")

    def test_replace_database_preserves_other_dsn_parts(self) -> None:
        dsn = "postgresql://user:pass@localhost:5433/sofascore_schema_inspector?sslmode=disable"
        replaced = _replace_database_in_dsn(dsn, "postgres")
        self.assertEqual(replaced, "postgresql://user:pass@localhost:5433/postgres?sslmode=disable")

    def test_quote_identifier_escapes_double_quotes(self) -> None:
        self.assertEqual(_quote_identifier('name"with"quotes'), '"name""with""quotes"')

    def test_discover_migration_files_returns_sorted_sql_only(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "2026-04-12_z.sql").write_text("-- z", encoding="utf-8")
            (root / "2026-04-11_a.sql").write_text("-- a", encoding="utf-8")
            (root / "notes.txt").write_text("skip", encoding="utf-8")

            files = _discover_migration_files(root)

        self.assertEqual([path.name for path in files], ["2026-04-11_a.sql", "2026-04-12_z.sql"])


if __name__ == "__main__":
    unittest.main()
