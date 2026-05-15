-- Task 4 (2026-05-15): coverage ledger for per-(ut_id, season_id) leaves.
--
-- Surfaces what fraction of every registered (unique_tournament_id,
-- season_id) pair has each leaf endpoint actually ingested. Drives
-- /ops/coverage/season-leaves and the CLI ``coverage-report`` rollup.
--
-- Design choices:
--
-- 1. MATERIALIZED VIEW, not a regular view, because the underlying
--    EXISTS-checks against api_payload_snapshot (~5M rows) would be
--    too slow on every request. Refresh cadence is operator-controlled
--    (see scripts/refresh_season_coverage.sh or the CLI subcommand).
--
-- 2. Boolean flags per leaf (not counts) — we care about coverage
--    ratio, not snapshot volume. The rollup query in coverage-report
--    aggregates per-sport / per-tournament.
--
-- 3. Indexed by primary (ut_id, season_id) so per-tournament drill-down
--    is fast. Additional partial indexes target the "not yet covered"
--    sets which are the actionable rollup queries.
--
-- 4. Pre-aggregated leaf_snapshots CTE collapses the 5M-row scan to
--    one pass per endpoint pattern, then JOIN-style lookups instead
--    of N independent EXISTS subqueries.

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_season_coverage AS
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
        '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-teams/overall'
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
    -- snapshot-based leaves
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
    -- normalised-table leaves
    COALESCE(rd.round_count, 0) > 0 AS has_rounds_rows,
    COALESCE(ct.cup_tree_count, 0) > 0 AS has_cup_tree_rows,
    COALESCE(ss.season_stats_count, 0) > 0 AS has_season_stats_rows,
    -- event presence
    COALESCE(ev.event_count, 0) AS event_count_in_db,
    -- timestamp
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

-- Per-sport rollup uses this composite index for fast GROUP BY sport.
CREATE INDEX IF NOT EXISTS idx_mv_season_coverage_sport
    ON mv_season_coverage (sport_slug, unique_tournament_id);

-- Partial index — the actionable "missing rounds" rollup driver. The
-- coverage-report CLI uses this to list seasons that still need a
-- structure-sync pass after Task 3.
CREATE INDEX IF NOT EXISTS idx_mv_season_coverage_missing_rounds
    ON mv_season_coverage (sport_slug, unique_tournament_id, season_id)
    WHERE NOT has_rounds_rows;
