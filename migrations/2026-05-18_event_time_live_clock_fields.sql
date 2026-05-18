-- Add live-clock columns to event_time so the WS consumer (Stage 1)
-- can store the few delta fields that today have no normalized home:
--
--   time.played                    → played            (seconds into the period)
--   time.playedLastUpdated         → played_last_updated (server timestamp of the tick)
--   time.clockRunning              → clock_running     (bool: is the clock active)
--   time.clockRunningLastUpdated   → clock_running_last_updated
--
-- These come from wss://ws.sofascore.com:9222 (subjects sport.*) and
-- account for ~80K of the 315K event-type deltas observed across a
-- 6-day archive — too high a volume to drop on the floor.
--
-- Also add ``first_to_serve`` on event for tennis: a single-shot per
-- service game indicator (1=home, 2=away) used by the match-center UI
-- to render the serve marker.

ALTER TABLE event_time
    ADD COLUMN IF NOT EXISTS played                       integer,
    ADD COLUMN IF NOT EXISTS played_last_updated          timestamp with time zone,
    ADD COLUMN IF NOT EXISTS clock_running                boolean,
    ADD COLUMN IF NOT EXISTS clock_running_last_updated   timestamp with time zone;

ALTER TABLE event
    ADD COLUMN IF NOT EXISTS first_to_serve               smallint;

COMMENT ON COLUMN event_time.played IS
    'Seconds into the current period. Pushed by ws.sofascore.com '
    '(subject sport.*) — written by the WS consumer (2026-05-18). '
    'Polling-derived snapshots also contain this; whoever writes last '
    'wins (UPDATE is unconditional).';

COMMENT ON COLUMN event_time.clock_running IS
    'Whether the period clock is currently advancing. Important for '
    'tennis/basketball where pauses are explicit, less so for football '
    '(running while in-period).';

COMMENT ON COLUMN event.first_to_serve IS
    'Tennis: which side serves first in this match — 1=home, 2=away. '
    'Pushed by ws.sofascore.com on a tennis-event subscription.';
