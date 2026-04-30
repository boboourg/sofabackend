BEGIN;

CREATE TABLE IF NOT EXISTS event_endpoint_negative_cache_state (
    cache_key TEXT PRIMARY KEY,
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    status_phase TEXT NOT NULL CHECK (status_phase IN ('notstarted', 'inprogress', 'finished', 'terminated', 'unknown')),
    endpoint_pattern TEXT NOT NULL REFERENCES endpoint_registry(pattern),
    classification TEXT NOT NULL CHECK (classification IN ('c_probation', 'supported')),
    first_negative_at TIMESTAMPTZ NULL,
    last_negative_at TIMESTAMPTZ NULL,
    first_success_at TIMESTAMPTZ NULL,
    last_success_at TIMESTAMPTZ NULL,
    suppressed_hits_total BIGINT NOT NULL DEFAULT 0,
    actual_probe_total BIGINT NOT NULL DEFAULT 0,
    recheck_iteration INTEGER NOT NULL DEFAULT 0,
    next_probe_after TIMESTAMPTZ NULL,
    probe_lease_until TIMESTAMPTZ NULL,
    probe_lease_owner TEXT NULL,
    last_http_status INTEGER NULL,
    last_outcome_classification TEXT NULL,
    last_job_type TEXT NULL,
    last_trace_id TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_event_endpoint_negative_cache_state
    ON event_endpoint_negative_cache_state (event_id, status_phase, endpoint_pattern);

CREATE INDEX IF NOT EXISTS ix_event_endpoint_negative_cache_next_probe
    ON event_endpoint_negative_cache_state (next_probe_after, status_phase);

CREATE TABLE IF NOT EXISTS event_endpoint_availability_log (
    id BIGSERIAL PRIMARY KEY,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    event_id BIGINT NOT NULL REFERENCES event(id) ON DELETE CASCADE,
    status_phase TEXT NOT NULL CHECK (status_phase IN ('notstarted', 'inprogress', 'finished', 'terminated', 'unknown')),
    endpoint_pattern TEXT NOT NULL REFERENCES endpoint_registry(pattern),
    job_type TEXT NOT NULL,
    trace_id TEXT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('probe', 'suppressed', 'lease_blocked', 'shadow_suppress')),
    http_status INTEGER NULL,
    outcome_classification TEXT NULL,
    classification_before TEXT NULL,
    classification_after TEXT NULL,
    next_probe_after TIMESTAMPTZ NULL,
    proxy_id TEXT NULL,
    source_url TEXT NULL,
    note TEXT NULL
);

CREATE INDEX IF NOT EXISTS ix_event_endpoint_availability_log_time
    ON event_endpoint_availability_log (observed_at DESC);

CREATE INDEX IF NOT EXISTS ix_event_endpoint_availability_log_key
    ON event_endpoint_availability_log (event_id, status_phase, endpoint_pattern);

COMMIT;
