-- Phase 1 backfill ordering: per-(UT, season) cursor in tournament_registry.
--
-- Bobur ACK 2026-05-16 (docs/backfill-ordering-roadmap.md).
--
-- Semantics:
--   next_season_backfill_id IS NULL
--     → cursor not initialised. Bootstrap CLI seeds it to the UT's most
--       recent season; planner picks it up on the next tick.
--   next_season_backfill_id > 0
--     → next season to process. Worker picks this season, fetches it,
--       advances cursor to the next-older season_id by DESC walk.
--   next_season_backfill_id = 0
--     → backfill exhausted for this UT (all seasons processed). Stop
--       publishing new jobs. backfill_completed_at gets stamped.
--
-- The cursor only moves forward on a `succeeded` worker outcome; a
-- failed/retry_scheduled job leaves the cursor in place so the next
-- planner tick re-tries the same season (with the usual delayed-retry
-- machinery).

ALTER TABLE tournament_registry
    ADD COLUMN IF NOT EXISTS next_season_backfill_id BIGINT NULL,
    ADD COLUMN IF NOT EXISTS backfill_started_at  TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS backfill_completed_at TIMESTAMPTZ NULL,
    ADD COLUMN IF NOT EXISTS backfill_last_advance_at TIMESTAMPTZ NULL;

COMMENT ON COLUMN tournament_registry.next_season_backfill_id IS
    'Phase 1 backfill cursor: next season_id to process (NULL=uninitialised, '
    '0=exhausted, >0=pending). See docs/backfill-ordering-roadmap.md.';

COMMENT ON COLUMN tournament_registry.backfill_started_at IS
    'When the cursor was first seeded by the bootstrap CLI.';

COMMENT ON COLUMN tournament_registry.backfill_completed_at IS
    'When the cursor reached the oldest season (= backfill done for this UT).';

COMMENT ON COLUMN tournament_registry.backfill_last_advance_at IS
    'When the cursor was last advanced (after a successful season fetch). '
    'Useful for stuck-cursor detection in /ops/backfill-cursor.';

-- Index for planner ordering: it wants "UTs with a pending cursor, ordered
-- by priority_rank then ut_id". Excluding NULL/0 rows keeps the index lean.
CREATE INDEX IF NOT EXISTS idx_tournament_registry_backfill_pending
    ON tournament_registry (sport_slug, priority_rank, unique_tournament_id)
    WHERE is_active = true
      AND historical_enabled = true
      AND next_season_backfill_id IS NOT NULL
      AND next_season_backfill_id > 0;
