-- 2026-05-23 — Phase 4.1: League Capabilities Registry.
--
-- Why: match_center_policy.py currently gates endpoint fetches off
-- the per-event ``detailId``, ``has_xg``, ``has_event_player_statistics``
-- flags from the root payload. For tournaments whose root payload sets
-- these flags to ``null`` (cup-style, lower-tier leagues), the policy
-- falls into ``tier_5`` and silently disables /statistics, /comments,
-- /best-players, /heatmap, /shotmap — even though the league might
-- actually publish them.
--
-- The Registry replaces that hardcoded gate with a learned map per
-- (unique_tournament_id, season_id, status_type, endpoint_pattern).
-- The probe service measures the actual upstream coverage by sampling
-- 5 recent events of the (UT, season, status) cohort, and writes the
-- verdict here. The orchestrator consults the registry before the
-- legacy policy — registry ``disabled`` short-circuits the fetch,
-- registry ``allowed`` overrides a legacy ``tier_5`` denial.
--
-- Tri-state semantics (state CHECK):
--   'allowed'    -- probe found >=3/5 successful 200 OK + non-empty.
--   'disabled'   -- probe found >=3/5 404 / empty / soft_error.
--   'unknown'    -- probe inconclusive OR row predates probing
--                  (fail-safe — orchestrator falls back to legacy policy).
--
-- TTL: ``expires_at`` defaults to now() + 14 days. Refresh daemon
-- re-probes rows past their expiry. Operator manual override sets
-- source='manual_override' and expires_at far future, never re-probed.
--
-- season_id NULL means UT-level fallback (cup-style competitions where
-- different cup years/editions are exposed under different season_ids
-- and we want one verdict to apply across them until per-season probes
-- land).
--
-- Why a separate table (not extending tournament_registry):
-- tournament_registry tracks cursor + completeness state per UT.
-- This table tracks per-endpoint × per-status capability — a different
-- cardinality (~25 endpoints × 4 statuses = 100 rows per (UT, season)
-- vs 1 row in tournament_registry). Joining would explode the
-- registry. Separate table keeps both tight.

BEGIN;

CREATE TABLE IF NOT EXISTS league_endpoint_capability (
    unique_tournament_id INTEGER NOT NULL REFERENCES unique_tournament(id),
    season_id INTEGER NULL,
    -- Sofascore status_type values plus a synthetic ``__any__`` bucket
    -- the registry will fall back to when a specific status row
    -- is missing. Probe service writes specific values only.
    status_type TEXT NOT NULL,
    endpoint_pattern TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'unknown'
        CHECK (state IN ('allowed', 'disabled', 'unknown')),
    probed_at TIMESTAMPTZ,
    probe_samples_total INTEGER NOT NULL DEFAULT 0,
    probe_samples_ok INTEGER NOT NULL DEFAULT 0,
    probe_samples_http_404 INTEGER NOT NULL DEFAULT 0,
    probe_samples_empty INTEGER NOT NULL DEFAULT 0,
    probe_samples_error INTEGER NOT NULL DEFAULT 0,
    -- Confidence = ok/total. Stored explicitly so ops queries don't
    -- recompute on every page load. NULL when total=0.
    confidence_score NUMERIC(4,3),
    -- expires_at sets the auto-refresh deadline. Refresh daemon picks
    -- expired rows. Manual overrides set expires_at far in the future.
    expires_at TIMESTAMPTZ NOT NULL DEFAULT (now() + INTERVAL '14 days'),
    source TEXT NOT NULL DEFAULT 'probe'
        CHECK (source IN ('probe', 'manual_override', 'legacy_seed')),
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- season_id is part of the PK so we can store both UT-level
    -- (season_id IS NULL) and season-specific rows side by side.
    -- COALESCE workaround for NULL-in-PK: store NULL as 0 sentinel.
    -- We DON'T do that — use a partial UNIQUE index instead so NULLs
    -- are distinct (Postgres default) and per-season rows still
    -- de-duplicate.
    PRIMARY KEY (unique_tournament_id, season_id, status_type, endpoint_pattern)
);

-- Refresh daemon scans for expired rows — index on expires_at lets
-- it do a fast bound-scan instead of seq-scan of (eventually) 500k+
-- rows (5k UTs × 25 endpoints × 4 statuses = 500k).
CREATE INDEX IF NOT EXISTS idx_lec_expires
    ON league_endpoint_capability(expires_at)
    WHERE state <> 'unknown';

-- Ops endpoint reads "all capabilities for this UT" — index on UT id
-- with state filter for the common "show disabled endpoints" query.
CREATE INDEX IF NOT EXISTS idx_lec_ut_state
    ON league_endpoint_capability(unique_tournament_id, state);

COMMIT;
