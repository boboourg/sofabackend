-- Add BRIN index on etl_job_run.started_at to support time-windowed monitoring
-- queries (N1 monitoring daemon Phase 3, see docs/N1_MONITORING_PLAN.md).
--
-- ARCHITECTURE_AUDIT.md D.3 noted that filtering etl_job_run by
-- ``started_at >= now() - interval '5 minutes'`` does a full table scan
-- (etl_job_run has no usable index on started_at). On prod (2026-05-14
-- evening) a single such probe took ~30 seconds and starved /ops/health
-- which shares the asyncpg connection — see commit 329acca for the workaround
-- (the success-rate metric was zeroed out until this index lands).
--
-- BRIN is intentional: started_at is append-mostly monotonic-ish (jobs are
-- recorded in time order modulo small clock skew), so block ranges align
-- with timestamp ranges. BRIN gives 100× smaller index than B-tree for the
-- same probe latency on time-windowed scans.
--
-- IMPORTANT: CREATE INDEX CONCURRENTLY cannot run inside a transaction
-- block. Apply this file directly with psql, not through a BEGIN/COMMIT
-- wrapper.

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_etl_job_run_started_at_brin
ON etl_job_run
USING BRIN (started_at)
WITH (pages_per_range = 32);
