"""Stage 3.1 (2026-05-20 historical layer): champion-per-season DDL.

The existing ``unique_tournament.title_holder_team_id`` column is overwritten
on every upsert (``competition_repository.py:354-407``), so the history of
champions per season is not persisted anywhere — see audit B finding
``unique_tournament champion overwrite``.

This migration introduces ``unique_tournament_season_champion`` keyed by
``(unique_tournament_id, season_id)``, giving us a natural per-season
historicity through the composite primary key. UPSERT with Stage 1.2
``IS DISTINCT FROM`` guard will live in ``normalize_repository``.
"""

from __future__ import annotations

import unittest
from pathlib import Path


_MIGRATION_PATH = (
    Path(__file__).resolve().parent.parent
    / "migrations"
    / "2026-05-20_unique_tournament_season_champion.sql"
)
_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "postgres_schema.sql"


class UniqueTournamentSeasonChampionMigrationTests(unittest.TestCase):
    def test_migration_file_exists(self) -> None:
        self.assertTrue(
            _MIGRATION_PATH.exists(),
            msg=f"Migration file missing: {_MIGRATION_PATH}",
        )

    def test_migration_creates_table_idempotently(self) -> None:
        sql = _MIGRATION_PATH.read_text(encoding="utf-8")
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS unique_tournament_season_champion",
            sql,
            msg="Migration must use IF NOT EXISTS for idempotent re-runs",
        )

    def test_migration_is_wrapped_in_transaction(self) -> None:
        sql = _MIGRATION_PATH.read_text(encoding="utf-8")
        self.assertIn("BEGIN;", sql)
        self.assertIn("COMMIT;", sql)

    def test_table_has_composite_primary_key(self) -> None:
        """PK (unique_tournament_id, season_id) is what makes this table
        naturally historic per season — each (UT, season) maps to exactly
        one champion, but different seasons keep separate rows."""
        sql = _MIGRATION_PATH.read_text(encoding="utf-8")
        self.assertIn(
            "PRIMARY KEY (unique_tournament_id, season_id)",
            sql,
            msg=(
                "Composite PK (unique_tournament_id, season_id) is the "
                "central invariant — without it the table would inherit "
                "the same 'overwrite-on-upsert' problem as the source "
                "column."
            ),
        )

    def test_table_columns_match_expected_schema(self) -> None:
        sql = _MIGRATION_PATH.read_text(encoding="utf-8")
        for column in (
            "unique_tournament_id BIGINT",
            "season_id BIGINT",
            "team_id BIGINT",
            "ordinal INTEGER",
            "source TEXT",
            "observed_at TIMESTAMPTZ",
        ):
            self.assertIn(
                column,
                sql,
                msg=f"Migration must declare column: {column}",
            )

    def test_foreign_keys_to_existing_entities(self) -> None:
        sql = _MIGRATION_PATH.read_text(encoding="utf-8")
        self.assertIn(
            "REFERENCES unique_tournament(id)",
            sql,
            msg="FK on unique_tournament_id must exist",
        )
        self.assertIn(
            "REFERENCES season(id)",
            sql,
            msg="FK on season_id must exist",
        )
        self.assertIn(
            "REFERENCES team(id)",
            sql,
            msg="FK on team_id must exist",
        )

    def test_index_on_team_for_career_titles_lookup(self) -> None:
        """Without an index on team_id, queries like 'how many championships
        has Real Madrid won across all seasons' would force a seq scan."""
        sql = _MIGRATION_PATH.read_text(encoding="utf-8")
        self.assertIn(
            "CREATE INDEX IF NOT EXISTS idx_unique_tournament_season_champion_team",
            sql,
            msg=(
                "Index on team_id for the 'titles per team' reverse lookup"
            ),
        )

    def test_postgres_schema_snapshot_includes_new_table(self) -> None:
        """postgres_schema.sql is the canonical schema snapshot — it must
        also gain the new table so any from-scratch DB build picks it up
        without applying every migration in sequence."""
        sql = _SCHEMA_PATH.read_text(encoding="utf-8")
        self.assertIn(
            "CREATE TABLE unique_tournament_season_champion",
            sql,
            msg=(
                "postgres_schema.sql must include the new table — "
                "otherwise a fresh DB built from the schema snapshot "
                "(not migrations) would lack the champion history."
            ),
        )


if __name__ == "__main__":
    unittest.main()
