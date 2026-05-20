-- B3 (2026-05-20): add team_of_the_week_player.event_id to enable
-- 1:1 reconstruction of the upstream Sofascore ToTW wire format
-- which carries a nested ``event`` object per player (the match
-- where the player earned the rating that landed them in the
-- team of the week).
--
-- The column is nullable: existing rows backfill from snapshot
-- payloads in a follow-up DML and new ingests populate it via
-- the updated parser. FK is ON DELETE SET NULL so an event purge
-- does not cascade-delete ToTW history.

ALTER TABLE team_of_the_week_player
    ADD COLUMN IF NOT EXISTS event_id BIGINT;

ALTER TABLE team_of_the_week_player
    DROP CONSTRAINT IF EXISTS team_of_the_week_player_event_id_fkey;

ALTER TABLE team_of_the_week_player
    ADD CONSTRAINT team_of_the_week_player_event_id_fkey
    FOREIGN KEY (event_id) REFERENCES event(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_team_of_the_week_player_event
    ON team_of_the_week_player (event_id)
    WHERE event_id IS NOT NULL;
