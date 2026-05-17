-- Index for the synthesizer's player events fetcher (2026-05-18).
--
-- /player/{player_id}/events/last/{page} joins event with
-- event_lineup_player (6.2 M rows) filtered by player_id. The pkey is
-- (event_id, side, player_id) — useful for "who played in event X" but
-- a forward scan / full-table scan for "which events did player X
-- play in".
--
-- This composite (player_id, event_id) lets the synthesizer's player
-- events query do an index range scan keyed by player_id and project
-- distinct event_id values, then nested-loop into event via pkey.
--
-- CONCURRENTLY so prod writes are not blocked.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_lineup_player_player_id_event_id
    ON event_lineup_player (player_id, event_id);

COMMENT ON INDEX idx_event_lineup_player_player_id_event_id IS
    'Composite (player_id, event_id) — powers fetch_player_events_rows '
    'in the synthesizer. Bobur ACK 2026-05-18.';
