-- Task D / Coverage view v3 (2026-05-15 post-review-2).
--
-- Migration v2 (earlier today) had three pattern-string bugs that
-- generated FALSE-zero coverage rows. The actual endpoint_pattern
-- shape on prod is:
--
--   v2 looked for                              real pattern
--   ----------------------------------------   -------------------------
--   .../standings/total                        .../standings/{scope}
--   .../events/round/{round}                   .../events/round/{round_number}
--   .../team-events/last/{page}   (split)      .../team-events/{scope}  (unified)
--   .../team-events/next/{page}   (split)      (same)
--
-- Each of those lookups matched zero rows in api_payload_snapshot,
-- which is why /ops/coverage/season-leaves reported
--   standings_total = 0%
--   events_round    = 0%
--   team_events_*   = 0%
-- when the true numbers are ~36% / ~? / ~0.01%.
--
-- This v3 migration drops and recreates the materialised view with
-- the corrected pattern strings. The /ops/coverage/season-leaves
-- handler is updated in the same commit so the JSON keys stay
-- meaningful (standings_total -> standings, events_round stays).

DROP MATERIALIZED VIEW IF EXISTS mv_season_coverage CASCADE;

CREATE MATERIALIZED VIEW mv_season_coverage AS
WITH leaf_snapshot_counts AS (
    SELECT
        endpoint_pattern,
        context_entity_id AS season_id,
        COUNT(*) AS snapshot_count
    FROM api_payload_snapshot
    WHERE context_entity_type = 'season'
      AND context_entity_id IS NOT NULL
      AND endpoint_pattern IN (
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/info',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/rounds',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/{scope}',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/cuptrees',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/overall',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/last/{page}',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/next/{page}',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round_number}',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/{scope}',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/brackets',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/groups',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/venues',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/info'
      )
    GROUP BY 1, 2
),
event_existence AS (
    SELECT
        unique_tournament_id,
        season_id,
        COUNT(*) AS event_count
    FROM event
    WHERE unique_tournament_id IS NOT NULL
      AND season_id IS NOT NULL
    GROUP BY 1, 2
),
round_existence AS (
    SELECT unique_tournament_id, season_id, COUNT(*) AS round_count
    FROM season_round
    GROUP BY 1, 2
),
cup_tree_existence AS (
    SELECT unique_tournament_id, season_id, COUNT(*) AS cup_tree_count
    FROM season_cup_tree
    GROUP BY 1, 2
),
season_stats_existence AS (
    SELECT
        unique_tournament_id,
        season_id,
        COUNT(*) AS season_stats_count
    FROM season_statistics_snapshot
    GROUP BY 1, 2
)
SELECT
    uts.unique_tournament_id,
    uts.season_id,
    s.slug AS sport_slug,
    c.id AS category_id,
    -- snapshot-based leaves (FIXED patterns)
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/info'
    ) AS has_info_snapshot,
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/rounds'
    ) AS has_rounds_snapshot,
    -- FIX: was standings/total, real pattern uses {scope} template
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/{scope}'
    ) AS has_standings_snapshot,
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/cuptrees'
    ) AS has_cuptrees_snapshot,
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall'
    ) AS has_top_players_snapshot,
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/overall'
    ) AS has_top_teams_snapshot,
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/last/{page}'
    ) AS has_events_last_snapshot,
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/next/{page}'
    ) AS has_events_next_snapshot,
    -- FIX: was events/round/{round}, real pattern uses {round_number}
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round_number}'
    ) AS has_events_round_snapshot,
    -- FIX: was team-events/last/{page} + team-events/next/{page} (two
    -- separate flags). Real registry uses a single team-events/{scope}.
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/{scope}'
    ) AS has_team_events_snapshot,
    -- New leaves added in v3 (reviewer's spirit: cover the actual
    -- registry, not what I imagined).
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/brackets'
    ) AS has_brackets_snapshot,
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/groups'
    ) AS has_groups_snapshot,
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/venues'
    ) AS has_venues_snapshot,
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/player-of-the-season'
    ) AS has_player_of_the_season_snapshot,
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/statistics/info'
    ) AS has_statistics_info_snapshot,
    -- normalised-table leaves
    COALESCE(rd.round_count, 0) > 0 AS has_rounds_rows,
    COALESCE(ct.cup_tree_count, 0) > 0 AS has_cup_tree_rows,
    COALESCE(ss.season_stats_count, 0) > 0 AS has_season_stats_rows,
    -- event presence
    COALESCE(ev.event_count, 0) AS event_count_in_db,
    now() AS refreshed_at
FROM unique_tournament_season uts
JOIN unique_tournament ut ON ut.id = uts.unique_tournament_id
JOIN category c ON c.id = ut.category_id
JOIN sport s ON s.id = c.sport_id
LEFT JOIN event_existence ev
    ON ev.unique_tournament_id = uts.unique_tournament_id
    AND ev.season_id = uts.season_id
LEFT JOIN round_existence rd
    ON rd.unique_tournament_id = uts.unique_tournament_id
    AND rd.season_id = uts.season_id
LEFT JOIN cup_tree_existence ct
    ON ct.unique_tournament_id = uts.unique_tournament_id
    AND ct.season_id = uts.season_id
LEFT JOIN season_stats_existence ss
    ON ss.unique_tournament_id = uts.unique_tournament_id
    AND ss.season_id = uts.season_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_season_coverage_pk
    ON mv_season_coverage (unique_tournament_id, season_id);

CREATE INDEX IF NOT EXISTS idx_mv_season_coverage_sport
    ON mv_season_coverage (sport_slug, unique_tournament_id);

CREATE INDEX IF NOT EXISTS idx_mv_season_coverage_missing_rounds
    ON mv_season_coverage (sport_slug, unique_tournament_id, season_id)
    WHERE NOT has_rounds_rows;

CREATE INDEX IF NOT EXISTS idx_mv_season_coverage_missing_events_last
    ON mv_season_coverage (sport_slug, unique_tournament_id, season_id)
    WHERE NOT has_events_last_snapshot;

CREATE INDEX IF NOT EXISTS idx_mv_season_coverage_missing_standings
    ON mv_season_coverage (sport_slug, unique_tournament_id, season_id)
    WHERE NOT has_standings_snapshot;
