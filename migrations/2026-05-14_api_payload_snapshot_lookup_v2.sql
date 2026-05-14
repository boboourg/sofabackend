-- N4 Layer A: composite snapshot lookup index for local API read path
-- (2026-05-14, see docs/N4_API_PERFORMANCE_PLAN.md).
--
-- Phase 0 audit (scripts/audit_api_latency.py) showed that ~70% of
-- entity-scoped endpoints (event/{id}/lineups, event/{id}/statistics,
-- player/{id}, team/{id}, manager/{id}, ...) take 200ms–10s on cold path.
-- Root cause: only one partial index exists for snapshot lookups
-- (idx_api_payload_snapshot_event_root_local_api), covering exactly one
-- pattern (/api/v1/event/{event_id}). All other patterns route through
-- the generic idx_api_payload_snapshot_endpoint_pattern btree, which
-- yields ~16K rows per pattern and then filter-scans by
-- context_entity_id — 100–500ms on 4M-row table.
--
-- This index covers the canonical local_api_server._fetch_snapshot_payload
-- query shape:
--   WHERE endpoint_pattern = $1
--     AND context_entity_type = $2
--     AND context_entity_id = $3
--   ORDER BY id DESC LIMIT N
--
-- /api/v1/event/{event_id} stays on its existing dedicated partial index
-- (idx_api_payload_snapshot_event_root_local_api) — it's already
-- 4ms in audit so no change needed there.
--
-- IMPORTANT: CREATE INDEX CONCURRENTLY cannot run inside a transaction
-- block. Apply this file directly with psql, not through BEGIN/COMMIT.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_payload_snapshot_lookup_v2
    ON api_payload_snapshot (endpoint_pattern, context_entity_type, context_entity_id, id DESC)
    WHERE context_entity_id IS NOT NULL;
