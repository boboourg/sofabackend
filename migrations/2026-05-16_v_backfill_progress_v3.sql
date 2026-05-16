-- v_backfill_progress v3 (2026-05-16).
--
-- v2 used recent-only median for the expected baseline. v3 adds a
-- ``cursor_at_this_season`` boolean so consumers can see which season
-- the historical planner will pick next for each UT, alongside
-- existing events / expected / pct columns.

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
    END             AS pct,
    -- Phase 1 (2026-05-16): true for the row whose season_id matches the
    -- UT's current backfill cursor. Helps operators see "the planner is
    -- about to fetch THIS season next".
    (tr.next_season_backfill_id = pse.season_id) AS cursor_at_this_season
FROM tournament_registry tr
JOIN unique_tournament ut ON ut.id = tr.unique_tournament_id
JOIN per_season_events pse ON pse.unique_tournament_id = ut.id
LEFT JOIN ut_expected ue ON ue.unique_tournament_id = ut.id
WHERE tr.is_active = true
  AND tr.historical_enabled = true;

COMMENT ON VIEW v_backfill_progress IS
  'Per-(UT, season) backfill progress with cursor_at_this_season flag. '
  'Expected = median over UT''s recent 5 seasons (events > 100). '
  'Phase 1, Bobur ACK 2026-05-16.';
