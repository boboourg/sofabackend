BEGIN;

CREATE TABLE IF NOT EXISTS tournament_registry (
    source_slug TEXT NOT NULL REFERENCES provider_source(source_slug),
    sport_slug TEXT NOT NULL,
    category_id BIGINT NOT NULL,
    unique_tournament_id BIGINT NOT NULL,
    discovery_surface TEXT NOT NULL,
    priority_rank INTEGER NOT NULL DEFAULT 0,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_slug, sport_slug, unique_tournament_id)
);

CREATE INDEX IF NOT EXISTS idx_tournament_registry_active_sport
    ON tournament_registry (sport_slug, is_active, priority_rank, unique_tournament_id);

COMMIT;
