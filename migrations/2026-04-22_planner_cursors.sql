BEGIN;

CREATE TABLE IF NOT EXISTS etl_planner_cursor (
    planner_name TEXT NOT NULL,
    source_slug TEXT NOT NULL,
    sport_slug TEXT NOT NULL,
    scope_type TEXT NOT NULL,
    scope_id BIGINT NOT NULL DEFAULT 0,
    cursor_date DATE NULL,
    cursor_id BIGINT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (planner_name, source_slug, sport_slug, scope_type, scope_id)
);

CREATE INDEX IF NOT EXISTS idx_etl_planner_cursor_updated_at
    ON etl_planner_cursor (updated_at DESC);

COMMIT;
