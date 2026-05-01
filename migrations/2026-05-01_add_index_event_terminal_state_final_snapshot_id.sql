-- Add index on event_terminal_state.final_snapshot_id
-- Resolves last bottleneck in api_payload_snapshot retention DELETE.
-- Without this index, ON DELETE SET NULL FK trigger does seq-scan
-- on 548K rows for each parent DELETE, causing 44ms per row overhead.
-- After this index, retention DELETE for 20K batch should fit in 60s timeout.

CREATE INDEX CONCURRENTLY IF NOT EXISTS
    idx_event_terminal_state_final_snapshot_id
ON event_terminal_state (final_snapshot_id);
