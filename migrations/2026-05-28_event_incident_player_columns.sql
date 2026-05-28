-- 2026-05-28: enrich event_incident with player references so the
-- /api/v1/player/{pid}/events/last/{page} endpoint can serve the
-- ``incidentsMap`` envelope (goals / assists / yellowCards / redCards)
-- that the Sofascore web client emits.
--
-- The original 4-column schema (event_id, ordinal, incident_id,
-- incident_type, minute, scores, text_value) was sufficient for the
-- match-center incident timeline but didn't carry the player-scoped
-- pointers the per-player aggregations need.  We add 5 columns:
--
--   * ``player_id``           — primary actor for the incident
--                               (goalscorer, card recipient, subbed-in
--                               player for substitution events).
--   * ``assist1_player_id``   — secondary actor (assister on goals,
--                               subbed-out player on substitutions).
--                               Sofascore also has ``assist2`` but only
--                               ~1% of goals have a second assister and
--                               the web client never surfaces it.
--   * ``incident_class``      — ``"regular" | "yellow" | "red" |
--                               "yellowRed" | "penalty" | "ownGoal" |
--                               "headed" | ...``  Disambiguates goal
--                               types and card colours.
--   * ``is_home``             — boolean: was the incident on the home
--                               side.  Lets us slice incidents per
--                               team without rejoining ``event`` for
--                               home_team_id / away_team_id.
--   * ``length``              — only set for ``incidentType='injuryTime'``
--                               rows; the added-minutes count.
--
-- Backfill is intentionally **NOT** done in this migration.  The
-- event_incidents parser (next commit in this PR) will populate the
-- new columns on every fresh fetch.  Existing rows stay NULL until
-- the next ``hydrate`` / ``live-tier`` refresh — natural drain in
-- 24–48h for current-season events; older events stay NULL
-- (acceptable, mobile only renders incidents on recent matches).

ALTER TABLE event_incident
    ADD COLUMN IF NOT EXISTS player_id          BIGINT REFERENCES player(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS assist1_player_id  BIGINT REFERENCES player(id) ON DELETE SET NULL,
    ADD COLUMN IF NOT EXISTS incident_class     TEXT,
    ADD COLUMN IF NOT EXISTS is_home            BOOLEAN,
    ADD COLUMN IF NOT EXISTS length             INTEGER;

-- The ``incidentsMap`` aggregator queries by (player_id, event_id IN
-- (...)) — same access pattern as the existing
-- idx_event_lineup_player_player_id_event_id, just on a smaller fact
-- table (~10–20 incidents per event).  Partial index keeps it small:
-- the vast majority of incidents (substitutions, periods, injury time)
-- don't carry a player_id and stay out of the index.
CREATE INDEX IF NOT EXISTS idx_event_incident_player_event
    ON event_incident (player_id, event_id)
    WHERE player_id IS NOT NULL;

-- Same shape for the assister lookup — far rarer but still useful when
-- a player's events page slices their assist contributions.
CREATE INDEX IF NOT EXISTS idx_event_incident_assist1_event
    ON event_incident (assist1_player_id, event_id)
    WHERE assist1_player_id IS NOT NULL;
