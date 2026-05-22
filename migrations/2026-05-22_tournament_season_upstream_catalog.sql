-- 2026-05-22 — Phase 2.A: tournament_season_upstream_catalog.
--
-- Why: ``advance_backfill_cursor`` (Stage 1 / 2026-05-19) walks
-- ``next_season_backfill_id`` purely against the ``event`` table —
-- ``MAX(start_timestamp) < anchor`` joined on
-- ``event WHERE unique_tournament_id=X AND season_id=Y``. Seasons that
-- Sofascore publishes via ``/unique-tournament/{ut}/seasons`` but for
-- which we have zero ``event`` rows are INVISIBLE to the walk. The cursor
-- jumps straight from a populated season to the next-older populated
-- season, skipping every gap in between, and stamps
-- ``backfill_completed_at`` once it bottoms out.
--
-- Observed failure mode (2026-05-22 09:55-09:57): FIFA World Cup (UT=16),
-- U17 World Championship (UT=279), Women's WC (UT=290) all marked
-- ``backfill_completed_at`` while Sofascore's upstream ``/seasons`` list
-- carries 130+ historical seasons each with zero coverage in our DB.
--
-- This table fixes the semantic gap: it stores the AUTHORITATIVE list of
-- seasons Sofascore publishes per UT, populated by the same
-- ``CompetitionParser`` pass that hits ``/unique-tournament/{ut}/seasons``.
-- The walk then iterates THIS table, not ``event``, with a tri-state
-- ``bootstrap_state`` to track progress:
--
--   pending          -- catalog row exists but no events_loaded mark yet
--   events_loaded    -- event-list job successfully landed ≥1 event row
--   fully_processed  -- advance walk has moved past this (ut, season)
--
-- The advance gate stamps ``backfill_completed_at`` only when EVERY catalog
-- row reaches ``fully_processed``. Gaps in upstream → guaranteed retry on
-- next cursor walk instead of false-positive completion.
--
-- Why not REFERENCES season(id)? Sofascore can publish a season_id we
-- have never seen before (e.g. brand-new WC 2030 announcement). We MUST
-- be able to catalog it before any event row references it, so the FK
-- would be a chicken-and-egg block. The catalog populates first; the
-- ``season`` table populates from per-season ``/info`` fetches that the
-- bootstrap job triggers after this row exists.

BEGIN;

CREATE TABLE IF NOT EXISTS tournament_season_upstream_catalog (
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL,
    season_name TEXT,
    season_year TEXT,
    -- ``upstream_position`` = index in Sofascore /seasons response.
    -- Position 0 = newest (in upstream ordering). Used to sort the
    -- advance walk so it always walks newest → oldest within a UT,
    -- and as a tie-breaker when several seasons share a year.
    upstream_position INTEGER,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Tri-state machine. The CHECK constraint forces parsers / repository
    -- code to use one of the canonical strings — typos would silently
    -- block the walk if we permitted free text here.
    bootstrap_state TEXT NOT NULL DEFAULT 'pending'
        CHECK (bootstrap_state IN ('pending', 'events_loaded', 'fully_processed')),
    events_loaded_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    PRIMARY KEY (unique_tournament_id, season_id)
);

-- The advance walk filters by (unique_tournament_id, bootstrap_state).
-- Without this index every walk would seq-scan the whole catalog —
-- and the catalog will grow to ~50-150k rows once all sports have run
-- a ``/seasons`` discovery pass (5k UTs × ~10-30 seasons each).
CREATE INDEX IF NOT EXISTS idx_tsuc_state
    ON tournament_season_upstream_catalog(unique_tournament_id, bootstrap_state);

-- Reverse lookup: "which UTs published this season_id?". Sofascore
-- reuses season_id occasionally across closely related UTs (e.g.
-- World Cup Qualification rounds sharing a parent season). Index
-- helps the audit and ops surfaces that aggregate by season.
CREATE INDEX IF NOT EXISTS idx_tsuc_season
    ON tournament_season_upstream_catalog(season_id);

COMMIT;
