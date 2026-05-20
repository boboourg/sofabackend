-- Task 2 Phase A (2026-05-20): final-sync lifecycle column.
--
-- Adds ``locked_at`` to ``event_terminal_state`` so the system can
-- distinguish three lifecycle phases for a finished event:
--
--   1. HOT  — finalized_at IS NOT NULL,  locked_at IS NULL,
--             finalized_at + FINAL_SYNC_DELAY > now()
--     → Live path stopped; hydrate / scheduled / resource refreshes
--       may still touch the event (statistics late-arrival correction).
--
--   2. PENDING FINAL SYNC — finalized_at IS NOT NULL, locked_at IS NULL,
--             finalized_at + FINAL_SYNC_DELAY <= now()
--     → FinalSyncPlannerDaemon will publish ONE more hydrate job with
--       scope="final_sync"; the orchestrator stamps locked_at on success.
--
--   3. FROZEN — locked_at IS NOT NULL
--     → All non-historical workers skip; read-path serves the captured
--       final snapshot 1:1; retention pins ``final_snapshot_id`` forever.
--
-- The CLI command ``unlock-event`` (operator escape hatch) resets
-- ``locked_at = NULL`` so a frozen event can re-enter the normal flow.

ALTER TABLE event_terminal_state
    ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ;

-- Partial index — FinalSyncPlanner scans this thousands of times per
-- minute. The predicate keeps the index tiny (only events still in
-- HOT or PENDING phase appear).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_terminal_state_pending_lock
    ON event_terminal_state (finalized_at)
    WHERE locked_at IS NULL;

-- Lookup index for the worker / read-path defensive gate
-- (``WHERE event_id = $1 AND locked_at IS NOT NULL``).
-- PK already covers event_id; this just makes the locked-only filter
-- a partial-index scan.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_terminal_state_locked
    ON event_terminal_state (event_id)
    WHERE locked_at IS NOT NULL;
