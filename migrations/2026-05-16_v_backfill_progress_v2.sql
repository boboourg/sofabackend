-- v_backfill_progress v2 (2026-05-16).
--
-- v1 used median across ALL of a UT's seasons for the "expected" baseline.
-- That dragged the baseline down for UTs with many short historical seasons
-- (LaLiga 1929-1980 had partial-only data → median was 38 vs recent 380).
--
-- v2 picks "expected" from the median of the UT's most recent 5 *non-trivial*
-- seasons (events > 100). Rationale: a season with > 100 events is a real
-- league campaign, not an isolated cup tie or back-filled stub. The most
-- recent 5 reflect the current competition shape; older formats (different
-- number of teams, group-stage formats) don't bias the number.

DROP VIEW IF EXISTS v_backfill_progress;

CREATE VIEW v_backfill_progress AS
WITH per_season_events AS (
    SELECT
        uts.unique_tournament_id,
        s.id            AS season_id,
        s.year          AS season_year,
        s.name          AS season_name,
        COUNT(e.id)     AS events,
        COUNT(e.id) FILTER (
            WHERE e.status_code IN (100, 110, 120, 130, 140, 150)
        )               AS finished
    FROM unique_tournament_season uts
    JOIN season s ON s.id = uts.season_id
    LEFT JOIN event e
        ON e.season_id = s.id
       AND e.unique_tournament_id = uts.unique_tournament_id
    GROUP BY uts.unique_tournament_id, s.id, s.year, s.name
),
-- Pick the last 5 substantial seasons per UT (events > 100, ordered by
-- season_id DESC which approximates "recent first" for Sofascore — newer
-- seasons get higher ids).
recent_seasons AS (
    SELECT
        unique_tournament_id,
        season_id,
        events,
        ROW_NUMBER() OVER (
            PARTITION BY unique_tournament_id
            ORDER BY season_id DESC
        ) AS rn
    FROM per_season_events
    WHERE events > 100
),
ut_expected AS (
    SELECT
        unique_tournament_id,
        ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY events))::int AS expected
    FROM recent_seasons
    WHERE rn <= 5
    GROUP BY unique_tournament_id
)
SELECT
    tr.sport_slug,
    tr.priority_rank,
    ut.id           AS ut_id,
    ut.name         AS ut_name,
    pse.season_id,
    pse.season_year AS year,
    pse.season_name,
    pse.events,
    pse.finished,
    ue.expected,
    CASE
        WHEN ue.expected IS NULL OR ue.expected = 0 THEN NULL
        ELSE LEAST(100, ROUND(pse.events::numeric * 100 / ue.expected))
    END             AS pct
FROM tournament_registry tr
JOIN unique_tournament ut ON ut.id = tr.unique_tournament_id
JOIN per_season_events pse ON pse.unique_tournament_id = ut.id
LEFT JOIN ut_expected ue ON ue.unique_tournament_id = ut.id
WHERE tr.is_active = true
  AND tr.historical_enabled = true;

COMMENT ON VIEW v_backfill_progress IS
  'Per-(UT, season) backfill progress. Expected = median over the UT''s '
  'most recent 5 seasons with events > 100. Bobur ACK 2026-05-16.';
