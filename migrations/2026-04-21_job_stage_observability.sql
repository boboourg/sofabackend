BEGIN;

ALTER TABLE api_request_log
    ADD COLUMN IF NOT EXISTS attempts_json JSONB,
    ADD COLUMN IF NOT EXISTS payload_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS error_message TEXT;

CREATE TABLE IF NOT EXISTS etl_job_stage_run (
    id BIGSERIAL PRIMARY KEY,
    job_run_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    trace_id TEXT,
    worker_id TEXT NOT NULL,
    stage_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('started', 'succeeded', 'failed')),
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    duration_ms BIGINT,
    endpoint_pattern TEXT,
    proxy_name TEXT,
    http_status INTEGER,
    payload_bytes BIGINT,
    rows_written INTEGER,
    rows_deleted INTEGER,
    lock_wait_ms BIGINT,
    db_time_ms BIGINT,
    error_class TEXT,
    error_message TEXT,
    meta JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_etl_job_stage_run_job
    ON etl_job_stage_run (job_id, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_etl_job_stage_run_stage
    ON etl_job_stage_run (stage_name, started_at DESC);

COMMIT;
