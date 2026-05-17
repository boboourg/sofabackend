-- Retention indexes for housekeeping (2026-05-17).
--
-- The housekeeping loop's batched-DELETE retention for two large tables
-- (api_payload_snapshot legacy rows and endpoint_capability_observation)
-- was timing out every tick: each batch had to walk the pkey b-tree
-- filtering rows on the fly because no index covered the actual
-- predicate.
--
-- EXPLAIN evidence (104 GB / 6.2M rows api_payload_snapshot):
--
--   Limit  (cost=38..375437 rows=5000)
--     -> Merge Anti Join  (cost=38..19820293 rows=263989)
--           -> Index Scan using api_payload_snapshot_pkey
--                 Filter: ((scope_key IS NULL) AND (fetched_at < $cutoff))
--
-- Cost 19M to find 5K victims out of 1.1M legacy rows ≈ 60s asyncpg
-- command_timeout → retention step fails → zombie sweep never reaches
-- _run_zombie_sweep → terminal-zset zombies pile up → BackfillGovernor
-- pauses historical-tournament-planner → backfill stalls.
--
-- Same pattern for endpoint_capability_observation: 12 GB / 31.9M rows,
-- existing composite index begins with sport_slug so it can't satisfy a
-- retention scan keyed purely by observed_at.
--
-- Both indexes use CONCURRENTLY so prod writes are not blocked.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_aps_legacy_retention
    ON api_payload_snapshot (fetched_at)
    WHERE scope_key IS NULL;

COMMENT ON INDEX idx_aps_legacy_retention IS
    'Partial b-tree over (fetched_at) for rows with scope_key IS NULL. '
    'Powers housekeeping legacy-snapshot retention (delete_legacy_snapshot_batch). '
    'Without it the batched DELETE walks 18M rows via pkey to find 5K victims '
    'and times out at the asyncpg 60s command_timeout. Bobur ACK 2026-05-17.';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_endpoint_capability_observation_observed_at
    ON endpoint_capability_observation (observed_at);

COMMENT ON INDEX idx_endpoint_capability_observation_observed_at IS
    'B-tree over observed_at — powers housekeeping capability-observation '
    'retention. The existing composite index '
    'idx_endpoint_capability_observation_lookup begins with sport_slug so '
    'PG cannot use it for a retention scan keyed purely on observed_at. '
    'Bobur ACK 2026-05-17.';
