-- Add index on api_snapshot_head.latest_snapshot_id.
--
-- Resolves DELETE timeout on api_payload_snapshot retention.
-- Without this index, the NOT EXISTS clause in the retention query
-- requires Seq Scan + Sort on full api_snapshot_head (1.32M rows)
-- on every batch, taking ~520ms even for LIMIT=100.
--
-- Production cardinality on 2026-05-01: latest_snapshot_id is 100% NOT NULL,
-- so a full btree index is simpler and equivalent in size to a partial index.

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_api_snapshot_head_latest_snapshot_id
ON api_snapshot_head (latest_snapshot_id);
