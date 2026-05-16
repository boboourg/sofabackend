-- Backfill progress visibility view (2026-05-16).
-- Bobur ACK: shows %-completeness per (unique_tournament, season) with the
-- "expected events" derived from the median across the same UT's other
-- seasons (Sofascore does not publish an explicit "total fixtures" hint).
--
-- Usage:
--   SELECT * FROM v_backfill_progress
--   WHERE sport_slug = 'football' AND priority_rank <= 1
--   ORDER BY ut_name, year DESC;
--
-- Columns:
--   sport_slug         — sport this row belongs to
--   priority_rank      — from tournament_registry (1 = top)
--   ut_id, ut_name     — unique tournament identity
--   season_id, year    — season identity
--   events             — actual rows in `event` table for this season
--   finished           — events with terminal status_code
--   expected           — heuristic: median(events) across other UT seasons
--                        with > 0 events. NULL when no reference seasons.
--   pct                — events / expected * 100, clamped 0..100. NULL when
--                        expected is NULL or 0.

CREATE OR REPLACE VIEW v_backfill_progress AS
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
ut_expected AS (
    SELECT
        unique_tournament_id,
        -- Median over non-empty seasons gives a stable expected value:
        -- one anomalous fully-loaded season (e.g. 1997/98 = 380) does not
        -- dominate the mean, and shorter seasons (e.g. cup-only year)
        -- are still represented.
        ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY events))::int AS expected
    FROM per_season_events
    WHERE events > 0
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
  'Per-(UT, season) backfill progress with median-based expected events. '
  'Source-of-truth for backfill visibility. Bobur ACK 2026-05-16.';
