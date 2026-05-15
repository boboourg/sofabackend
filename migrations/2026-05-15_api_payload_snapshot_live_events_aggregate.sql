-- N4 Layer D D.4 / Hotfix (2026-05-15): partial covering index for the
-- live-events aggregator query used by ops/health.py:fetch_live_snapshot_repair_reasons
-- and live_discovery_planner.tick().
--
-- Background: live_discovery_planner crashed in a restart loop (counter
-- 891) because asyncpg client-side timeout fired on a 4.4s Parallel Seq
-- Scan over api_payload_snapshot (4.8M rows). Sweeper never ran ->
-- oldest_hot_score_age_seconds climbed to 5347s (CRIT 1800s). /ops/health
-- also returned 500 on the same SQL.
--
-- EXPLAIN ANALYZE before the index (just 2 sports):
--   Parallel Seq Scan on api_payload_snapshot aps
--     Filter: ((endpoint_pattern ~~ '/api/v1/sport/%/events/live')
--             OR (source_url ~~ '%/api/v1/sport/%/events/live%'))
--     Rows Removed by Filter: 1629552 (per worker, 3 workers)
--     Buffers: shared hit=713781 read=1147737
--   Execution Time: 4389.111 ms
--   JIT compilation: 847 ms
--
-- Recon on prod showed:
--   119520 rows match endpoint_pattern LIKE '/api/v1/sport/%/events/live'
--   100% of those ALSO match source_url LIKE '%/api/v1/sport/%/events/live%'
-- so a partial index keyed on endpoint_pattern covers the full result set.
--
-- The index:
--   - Partial WHERE clause matches the query predicate exactly, so the
--     planner can scan the entire index instead of seq-scanning the heap.
--   - fetched_at DESC included so MAX(fetched_at) GROUP BY sport can be
--     served as an Index Only Scan (with VACUUM keeping visibility map
--     up-to-date).
--   - sport_slug included so the COALESCE(NULLIF(sport_slug, ''), ...)
--     expression has source data without a heap fetch.
--
-- Expected impact: ~4400ms -> <50ms for the live aggregator. Resolves
-- live_discovery_planner crash loop and /ops/health 500.
--
-- IMPORTANT: CREATE INDEX CONCURRENTLY cannot run inside a transaction
-- block. Apply with `psql -f` directly, not via BEGIN/COMMIT wrapper.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_aps_endpoint_live_aggregate
    ON api_payload_snapshot
      (endpoint_pattern, fetched_at DESC, sport_slug)
    WHERE endpoint_pattern LIKE '/api/v1/sport/%/events/live';
