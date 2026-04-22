-- Add a composite event lookup index for historical event-detail backfill.
--
-- Purpose: support filtering by unique_tournament_id / season_id plus recent
-- time-window scans while preserving the requested DESC ordering.
--
-- IMPORTANT: CREATE INDEX CONCURRENTLY cannot run inside a transaction block.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_historical_backfill_lookup
    ON event (unique_tournament_id, season_id, start_timestamp DESC, id DESC);
