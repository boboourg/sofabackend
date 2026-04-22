-- Drop the obsolete event-detail lookup index on api_payload_snapshot.
--
-- EventDetailBackfillJob no longer uses api_payload_snapshot for missing-detail
-- detection, so the partial lookup index is dead weight. On production we also
-- observed an invalid copy of this index, which still adds write overhead while
-- providing no read benefit.
--
-- IMPORTANT: DROP INDEX CONCURRENTLY cannot run inside a transaction block.

DROP INDEX CONCURRENTLY IF EXISTS idx_api_payload_snapshot_event_detail_lookup;
