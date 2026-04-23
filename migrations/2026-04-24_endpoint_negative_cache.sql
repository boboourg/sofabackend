BEGIN;

CREATE TABLE IF NOT EXISTS endpoint_negative_cache_state (
    cache_key TEXT PRIMARY KEY,
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('tournament', 'season')),
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NULL REFERENCES season(id),
    endpoint_pattern TEXT NOT NULL REFERENCES endpoint_registry(pattern),
    classification TEXT NOT NULL CHECK (
        classification IN ('c_probation', 'b_structural', 'mixed_by_season', 'supported_season')
    ),
    first_404_at TIMESTAMPTZ NULL,
    last_404_at TIMESTAMPTZ NULL,
    first_200_at TIMESTAMPTZ NULL,
    last_200_at TIMESTAMPTZ NULL,
    seen_404_season_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    seen_200_season_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    suppressed_hits_total BIGINT NOT NULL DEFAULT 0,
    actual_probe_total BIGINT NOT NULL DEFAULT 0,
    recheck_iteration INTEGER NOT NULL DEFAULT 0,
    next_probe_after TIMESTAMPTZ NULL,
    probe_lease_until TIMESTAMPTZ NULL,
    probe_lease_owner TEXT NULL,
    last_http_status INTEGER NULL,
    last_job_type TEXT NULL,
    last_trace_id TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_endpoint_negative_cache_tournament
    ON endpoint_negative_cache_state (unique_tournament_id, endpoint_pattern)
    WHERE scope_kind = 'tournament' AND season_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_endpoint_negative_cache_season
    ON endpoint_negative_cache_state (unique_tournament_id, season_id, endpoint_pattern)
    WHERE scope_kind = 'season' AND season_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_endpoint_negative_cache_next_probe
    ON endpoint_negative_cache_state (next_probe_after, classification);

CREATE TABLE IF NOT EXISTS endpoint_availability_log (
    id BIGSERIAL PRIMARY KEY,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NULL REFERENCES season(id),
    endpoint_pattern TEXT NOT NULL REFERENCES endpoint_registry(pattern),
    job_type TEXT NOT NULL,
    trace_id TEXT NULL,
    worker_id TEXT NULL,
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('tournament', 'season')),
    decision TEXT NOT NULL CHECK (decision IN ('probe', 'bypass_probe', 'state_transition', 'lease_blocked', 'shadow_suppress')),
    http_status INTEGER NULL,
    probe_latency_ms INTEGER NULL,
    classification_before TEXT NULL,
    classification_after TEXT NULL,
    next_probe_after TIMESTAMPTZ NULL,
    proxy_id TEXT NULL,
    source_url TEXT NULL,
    note TEXT NULL
);

CREATE INDEX IF NOT EXISTS ix_endpoint_availability_log_time
    ON endpoint_availability_log (observed_at DESC);

CREATE INDEX IF NOT EXISTS ix_endpoint_availability_log_key
    ON endpoint_availability_log (unique_tournament_id, endpoint_pattern, season_id);

COMMIT;
