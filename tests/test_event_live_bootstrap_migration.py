from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_event_live_bootstrap_migration_adds_nullable_timestamp() -> None:
    sql = (ROOT / "migrations" / "2026-04-24_event_live_bootstrap.sql").read_text(encoding="utf-8")

    assert "ALTER TABLE event" in sql
    assert "ADD COLUMN IF NOT EXISTS live_bootstrap_done_at TIMESTAMPTZ" in sql


def test_canonical_schema_contains_live_bootstrap_column() -> None:
    sql = (ROOT / "postgres_schema.sql").read_text(encoding="utf-8")
    event_block = sql[sql.index("CREATE TABLE event (") : sql.index("CREATE OR REPLACE FUNCTION set_event_updated_at()")]

    assert "live_bootstrap_done_at TIMESTAMPTZ" in event_block
