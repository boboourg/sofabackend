BEGIN;

ALTER TABLE tournament_registry
    ADD COLUMN IF NOT EXISTS structure_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS current_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS live_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS historical_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS enrichment_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS refresh_interval_seconds INTEGER NULL,
    ADD COLUMN IF NOT EXISTS historical_backfill_start_date DATE NULL,
    ADD COLUMN IF NOT EXISTS historical_backfill_end_date DATE NULL,
    ADD COLUMN IF NOT EXISTS recent_refresh_days INTEGER NULL,
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS idx_tournament_registry_structure_targets
    ON tournament_registry (sport_slug, priority_rank, unique_tournament_id)
    WHERE is_active = TRUE AND structure_enabled = TRUE;

CREATE INDEX IF NOT EXISTS idx_tournament_registry_historical_targets
    ON tournament_registry (sport_slug, priority_rank, unique_tournament_id)
    WHERE is_active = TRUE AND historical_enabled = TRUE;

COMMIT;
