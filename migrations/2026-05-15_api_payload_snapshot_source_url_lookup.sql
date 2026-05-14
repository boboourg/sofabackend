-- N4 Layer D D.2 (2026-05-15): composite partial index for sport-level
-- (no-entity-scope) snapshot lookups.
--
-- Background: docs/N4_API_PERFORMANCE_PLAN.md + PROGRESS.md "Layer D D.1
-- diagnosis" entry. Phase 0 audit showed ~20 sport-level endpoints
-- (sport/{slug}/scheduled-events/{date}, sport/{slug}/events/live,
-- sport/{slug}/scheduled-tournaments/{date}/page/{page}) taking 5-30s,
-- often timing out at uvicorn 30s.
--
-- EXPLAIN ANALYZE on prod revealed the SQL itself is fast (~146ms via
-- idx_api_payload_snapshot_endpoint_pattern). The bottleneck is Python:
-- _fetch_snapshot_payload pulls 500 latest rows for the pattern across
-- ALL dates, then parses each ~50-100 KB JSONB payload before filtering
-- by source_url. Out of 500 rows, only 1-2 match the target date.
-- Effective Python cost: 5-10 seconds, frequently timing out.
--
-- Fix companion to local_api_server.py: _fetch_snapshot_payload now
-- adds AND source_url LIKE '<canonical-prefix>%' when the route has no
-- entity scope. This index makes that query a fast range scan.
--
-- Expected impact: sport-level scheduled-events first-hit 30s -> 5-10ms.
-- All Phase 0 audit catastrophic + timeout endpoints move to FAST band.
--
-- Partial WHERE context_entity_id IS NULL keeps the index small —
-- entity-scoped lookups have their own dedicated indexes (event_root
-- partial idx, lookup_v2 composite) and don't need this one.
--
-- IMPORTANT: CREATE INDEX CONCURRENTLY cannot run inside a transaction
-- block. Apply with psql -f directly, not via BEGIN/COMMIT wrapper.

-- text_pattern_ops on both text columns: required so the index supports
-- prefix LIKE on source_url as a range scan (not just as a heap Filter).
-- Default collation (en_US / glibc) does NOT allow LIKE prefix index
-- scans; the operator class text_pattern_ops gives Postgres the right
-- C-locale ordering for that range. Without this, the planner falls
-- back to bitmap heap scan + Filter (10K Rows Removed) and Layer D D.2
-- gives no SQL-side savings.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_payload_snapshot_source_url_lookup
    ON api_payload_snapshot
      (endpoint_pattern text_pattern_ops, source_url text_pattern_ops, id DESC)
    WHERE context_entity_id IS NULL;
