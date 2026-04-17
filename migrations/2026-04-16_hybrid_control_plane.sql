BEGIN;

CREATE TABLE IF NOT EXISTS api_request_log (
    id BIGSERIAL PRIMARY KEY,
    trace_id TEXT,
    job_id TEXT,
    job_type TEXT,
    sport_slug TEXT,
    method TEXT NOT NULL,
    source_url TEXT NOT NULL,
    endpoint_pattern TEXT NOT NULL,
    request_headers_redacted JSONB,
    query_params JSONB,
    proxy_id TEXT,
    transport_attempt INTEGER,
    http_status INTEGER,
    challenge_reason TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    latency_ms INTEGER
);

ALTER TABLE api_payload_snapshot
    ADD COLUMN IF NOT EXISTS resolved_url TEXT,
    ADD COLUMN IF NOT EXISTS trace_id TEXT,
    ADD COLUMN IF NOT EXISTS job_id TEXT,
    ADD COLUMN IF NOT EXISTS sport_slug TEXT,
    ADD COLUMN IF NOT EXISTS context_unique_tournament_id BIGINT,
    ADD COLUMN IF NOT EXISTS context_season_id BIGINT,
    ADD COLUMN IF NOT EXISTS context_event_id BIGINT,
    ADD COLUMN IF NOT EXISTS http_status INTEGER,
    ADD COLUMN IF NOT EXISTS payload_hash TEXT,
    ADD COLUMN IF NOT EXISTS payload_size_bytes INTEGER,
    ADD COLUMN IF NOT EXISTS content_type TEXT,
    ADD COLUMN IF NOT EXISTS is_valid_json BOOLEAN,
    ADD COLUMN IF NOT EXISTS is_soft_error_payload BOOLEAN;

CREATE TABLE IF NOT EXISTS api_snapshot_head (
    scope_key TEXT PRIMARY KEY,
    endpoint_pattern TEXT NOT NULL REFERENCES endpoint_registry(pattern),
    context_entity_type TEXT,
    context_entity_id BIGINT,
    latest_snapshot_id BIGINT NOT NULL REFERENCES api_payload_snapshot(id),
    latest_payload_hash TEXT,
    latest_fetched_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS etl_job_run (
    job_run_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    job_type TEXT NOT NULL,
    parent_job_id TEXT,
    trace_id TEXT,
    sport_slug TEXT,
    entity_type TEXT,
    entity_id BIGINT,
    scope TEXT,
    priority INTEGER,
    attempt INTEGER NOT NULL DEFAULT 1,
    worker_id TEXT,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    duration_ms INTEGER,
    error_class TEXT,
    error_message TEXT,
    retry_scheduled_for TIMESTAMPTZ,
    parser_version TEXT,
    normalizer_version TEXT,
    schema_version TEXT
);

CREATE TABLE IF NOT EXISTS etl_job_effect (
    job_run_id TEXT PRIMARY KEY REFERENCES etl_job_run(job_run_id) ON DELETE CASCADE,
    created_job_count INTEGER NOT NULL DEFAULT 0,
    created_snapshot_count INTEGER NOT NULL DEFAULT 0,
    created_normalized_rows INTEGER NOT NULL DEFAULT 0,
    capability_updates INTEGER NOT NULL DEFAULT 0,
    live_state_transition TEXT
);

CREATE TABLE IF NOT EXISTS etl_replay_log (
    replay_id TEXT PRIMARY KEY,
    source_snapshot_id BIGINT NOT NULL REFERENCES api_payload_snapshot(id) ON DELETE CASCADE,
    replay_reason TEXT NOT NULL,
    parser_version TEXT,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS endpoint_capability_observation (
    id BIGSERIAL PRIMARY KEY,
    sport_slug TEXT NOT NULL,
    endpoint_pattern TEXT NOT NULL,
    entity_scope TEXT,
    context_type TEXT,
    http_status INTEGER,
    payload_validity TEXT,
    payload_root_keys JSONB,
    is_empty_payload BOOLEAN NOT NULL DEFAULT FALSE,
    is_soft_error_payload BOOLEAN NOT NULL DEFAULT FALSE,
    observed_at TIMESTAMPTZ NOT NULL,
    sample_snapshot_id BIGINT REFERENCES api_payload_snapshot(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS endpoint_capability_rollup (
    sport_slug TEXT NOT NULL,
    endpoint_pattern TEXT NOT NULL,
    support_level TEXT NOT NULL,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0,
    last_success_at TIMESTAMPTZ,
    last_404_at TIMESTAMPTZ,
    last_soft_error_at TIMESTAMPTZ,
    success_count INTEGER NOT NULL DEFAULT 0,
    not_found_count INTEGER NOT NULL DEFAULT 0,
    soft_error_count INTEGER NOT NULL DEFAULT 0,
    empty_count INTEGER NOT NULL DEFAULT 0,
    notes TEXT,
    PRIMARY KEY (sport_slug, endpoint_pattern)
);

CREATE TABLE IF NOT EXISTS event_live_state_history (
    id BIGSERIAL PRIMARY KEY,
    event_id BIGINT NOT NULL,
    observed_status_type TEXT,
    poll_profile TEXT,
    home_score INTEGER,
    away_score INTEGER,
    period_label TEXT,
    observed_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS event_terminal_state (
    event_id BIGINT PRIMARY KEY,
    terminal_status TEXT NOT NULL,
    finalized_at TIMESTAMPTZ NOT NULL,
    final_snapshot_id BIGINT REFERENCES api_payload_snapshot(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_api_request_log_endpoint_pattern ON api_request_log (endpoint_pattern);
CREATE INDEX IF NOT EXISTS idx_api_request_log_trace_id ON api_request_log (trace_id);
CREATE INDEX IF NOT EXISTS idx_api_payload_snapshot_trace_id ON api_payload_snapshot (trace_id);
CREATE INDEX IF NOT EXISTS idx_api_payload_snapshot_context_event_id ON api_payload_snapshot (context_event_id);
CREATE INDEX IF NOT EXISTS idx_api_payload_snapshot_endpoint_pattern ON api_payload_snapshot (endpoint_pattern);
CREATE INDEX IF NOT EXISTS idx_etl_job_run_job_id ON etl_job_run (job_id);
CREATE INDEX IF NOT EXISTS idx_etl_job_run_status ON etl_job_run (status);
CREATE INDEX IF NOT EXISTS idx_endpoint_capability_observation_lookup
    ON endpoint_capability_observation (sport_slug, endpoint_pattern, observed_at DESC);

COMMIT;
