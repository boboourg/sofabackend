-- Add FK index on endpoint_capability_observation.sample_snapshot_id.
--
-- Resolves housekeeping TimeoutError caused by FK ON DELETE SET NULL
-- doing seq-scan on endpoint_capability_observation for each parent DELETE
-- from api_payload_snapshot.
--
-- IMPORTANT: CREATE INDEX CONCURRENTLY cannot run inside a transaction block.
-- Apply this file directly with psql, not through a BEGIN/COMMIT wrapper.

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_endpoint_capability_observation_sample_snapshot_id
ON endpoint_capability_observation (sample_snapshot_id)
WHERE sample_snapshot_id IS NOT NULL;
