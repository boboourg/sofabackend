-- P3.2 — Sportradar Coverage Matrix table.
--
-- Stores per-(sr_competition, ut) coverage profile sourced from Sportradar's
-- public coverage matrix. Used by SportradarCoverageGate (planner-level + worker-level)
-- to skip endpoints that are structurally not covered by Sportradar for a given
-- league, BEFORE issuing a Sofascore HTTP request.
--
-- Reconciliation maps Sportradar's `sr:competition:N` ID to Sofascore
-- `unique_tournament.id`. Methods (priority order):
--   - blind_id    : Sof unique_tournament.id == Sportradar competition id +
--                   sport matches + name similarity > 0.9 (top-tier popular leagues)
--   - name_match  : fuzzy name + country/category match (regional/edge cases)
--   - manual      : explicit override from config/sportradar_id_overrides.yaml
--   - unmatched   : nothing matched, gate fails open for this row
--
-- Source-of-truth column: `coverage_attrs` (JSONB). Per-attribute integer values
-- 0 (none), 1 (partial), 2 (full). Gate logic uses individual attributes —
-- NEVER tier (which is information-only).
--
-- This migration is idempotent (CREATE IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).

CREATE TABLE IF NOT EXISTS sportradar_coverage (
    sr_competition_id BIGINT PRIMARY KEY,
    sr_competition_name TEXT NOT NULL,
    sr_category_name TEXT NOT NULL,                        -- "England", "Saudi Arabia", ...
    sport_slug TEXT NOT NULL DEFAULT 'football',           -- pluggable for Phase 2

    -- Reconciliation result
    unique_tournament_id BIGINT,                           -- nullable when method='unmatched'
    reconciliation_method TEXT NOT NULL CHECK (
        reconciliation_method IN ('blind_id', 'name_match', 'manual', 'unmatched')
    ),
    reconciliation_confidence NUMERIC(3, 2) NOT NULL DEFAULT 0 CHECK (
        reconciliation_confidence >= 0 AND reconciliation_confidence <= 1
    ),

    -- Coverage data — source of truth for gate decisions.
    -- Shape: {"Live": 2, "Lineups": 0, "Leaders": 2, ...}
    coverage_attrs JSONB NOT NULL,

    -- Information-only metadata (NEVER read by gate).
    tier INTEGER,
    stage_coverage JSONB,                                  -- per simple_tournament booleans (future)
    season_id BIGINT,                                      -- Sportradar's active-season id
    season_start_date TIMESTAMPTZ,

    -- Sync metadata
    last_changed TIMESTAMPTZ NOT NULL,                     -- from Sportradar API; incremental sync key
    synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT sportradar_coverage_unique_tournament_fkey
        FOREIGN KEY (unique_tournament_id) REFERENCES unique_tournament(id)
);

-- Hot path: gate lookup by ut_id with confidence filter.
CREATE INDEX IF NOT EXISTS sportradar_coverage_ut_idx
    ON sportradar_coverage(unique_tournament_id)
    WHERE unique_tournament_id IS NOT NULL;

-- Diagnostic: find low-confidence rows for manual review.
CREATE INDEX IF NOT EXISTS sportradar_coverage_method_conf_idx
    ON sportradar_coverage(reconciliation_method, reconciliation_confidence);

-- Incremental sync watermark.
CREATE INDEX IF NOT EXISTS sportradar_coverage_last_changed_idx
    ON sportradar_coverage(last_changed);

-- Per-sport filter (Phase 2 scaling).
CREATE INDEX IF NOT EXISTS sportradar_coverage_sport_idx
    ON sportradar_coverage(sport_slug);

COMMENT ON TABLE sportradar_coverage IS
    'P3.2 — Sportradar coverage matrix per competition. Read by '
    'SportradarCoverageGate to short-circuit fetches for structurally-missing '
    'endpoints. See docs/superpowers/plans/2026-05-08-p3-2-sportradar-coverage-gate.md';

COMMENT ON COLUMN sportradar_coverage.coverage_attrs IS
    'Source of truth for gate decisions. JSONB mapping coverage attribute name '
    'to integer 0/1/2. Example: {"Live": 2, "Lineups": 0, "Leaders": 2}';

COMMENT ON COLUMN sportradar_coverage.tier IS
    'Information-only. Sportradar tier classification (1-9 for soccer, 1-3 for '
    'tennis/cricket). Never read by gate logic.';

COMMENT ON COLUMN sportradar_coverage.reconciliation_confidence IS
    'Range [0.00, 1.00]. Gate applies hard-block only when >= MIN_CONFIDENCE '
    '(env-configurable, default 0.9). Below threshold: gate fails open.';
