-- Task 4 v2 (2026-05-15 post-review): extend mv_season_coverage with the
-- four leaves the reviewer flagged as missing:
--
--   - events/last/{page}    — schedule history
--   - events/next/{page}    — schedule future
--   - events/round/{round}  — per-round event listing
--   - team-events/last/{page} — team-side history pagination
--
-- These are paginated endpoints, so a "covered" flag means "at least
-- one snapshot exists for (ut, season) with the given pattern", not
-- "every page is filled". Operators consume the rollup to find
-- (ut, season) pairs that have ZERO pages — the actionable subset.
--
-- We DROP and CREATE again (not ALTER + add columns) because the view
-- body changes shape and PostgreSQL materialised views do not support
-- column-additive ALTER. This is fine: REFRESH is operator-driven via
-- the ``coverage-refresh`` CLI so a moment of "no data" between the
-- drop and the next REFRESH is acceptable. The old indexes are
-- recreated explicitly.

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
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/total',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/home',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/away',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/cuptrees',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/overall',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/overall',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/last/{page}',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/next/{page}',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round}',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/last/{page}',
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/next/{page}'
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
    -- existing snapshot-based leaves
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
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/standings/total'
    ) AS has_standings_total_snapshot,
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
    -- v2: paginated/event-listing leaves (reviewer flag)
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
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/events/round/{round}'
    ) AS has_events_round_snapshot,
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/last/{page}'
    ) AS has_team_events_last_snapshot,
    EXISTS (
        SELECT 1 FROM leaf_snapshot_counts lsc
        WHERE lsc.season_id = uts.season_id
          AND lsc.endpoint_pattern =
            '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-events/next/{page}'
    ) AS has_team_events_next_snapshot,
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
