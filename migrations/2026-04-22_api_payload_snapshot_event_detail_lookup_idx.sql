-- Add a targeted partial index for historical event-detail backfill lookups.
--
-- Purpose: accelerate the anti-join in EventDetailBackfillJob that checks
-- whether the base event payload snapshot already exists for a given event_id.
--
-- IMPORTANT: CREATE INDEX CONCURRENTLY cannot run inside a transaction block.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_api_payload_snapshot_event_detail_lookup
    ON api_payload_snapshot (context_entity_id)
    WHERE endpoint_pattern = '/api/v1/event/{event_id}'
      AND context_entity_type = 'event'
      AND context_entity_id IS NOT NULL;
