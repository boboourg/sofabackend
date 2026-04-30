-- Add a targeted partial index for local API event-root cold reads.
--
-- Purpose: accelerate /api/v1/event/{event_id} fallback lookup in
-- LocalApiApplication._fetch_event_root_payload. Without this index Postgres
-- can scan api_payload_snapshot by primary key backwards and discard millions
-- of rows when an event has no root snapshot.
--
-- IMPORTANT: CREATE INDEX CONCURRENTLY cannot run inside a transaction block.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_payload_snapshot_event_root_local_api
    ON api_payload_snapshot (context_entity_id, id DESC)
    WHERE endpoint_pattern = '/api/v1/event/{event_id}'
      AND context_entity_type = 'event'
      AND context_entity_id IS NOT NULL;
