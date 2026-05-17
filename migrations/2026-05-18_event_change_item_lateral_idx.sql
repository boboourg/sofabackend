-- Index for the synthesizer's LATERAL changes aggregation (2026-05-18).
--
-- The scheduled-events / live-events / single-event synthesizer joins a
-- LATERAL subquery against event_change_item to assemble the Sofascore
-- ``changes`` block:
--
--   SELECT jsonb_build_object('changes', array_agg(change_value ORDER BY ordinal),
--                             'changeTimestamp', change_timestamp)
--   FROM event_change_item
--   WHERE event_id = e.id
--     AND change_timestamp = (SELECT MAX(change_timestamp)
--                             FROM event_change_item WHERE event_id = e.id)
--
-- event_change_item is 25 M rows. The pkey is (event_id, ordinal); it
-- supports the seek-by-event_id half but the inner MAX(change_timestamp)
-- and the equality filter still need to scan all the event's rows. A
-- composite index on (event_id, change_timestamp DESC) lets PG resolve
-- the MAX with an index-only one-row peek and walk the matching
-- timestamp group with a tight range scan.
--
-- Estimated impact on the live-events synthesizer query:
--   before:   7 s warm (with LATERAL agg)
--   after:    ~1-2 s warm (LATERAL agg becomes index-only)
--
-- CONCURRENTLY so prod writes are not blocked.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_change_item_event_id_ts_desc
    ON event_change_item (event_id, change_timestamp DESC);

COMMENT ON INDEX idx_event_change_item_event_id_ts_desc IS
    'Composite (event_id, change_timestamp DESC) — powers the synthesizer''s '
    'LATERAL changes-block aggregation. Bobur ACK 2026-05-18.';
