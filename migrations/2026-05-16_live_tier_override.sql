-- A3 Phase 0 (2026-05-16): per-UT live tier override.
--
-- Operator-controlled exception layer on top of the default
-- detail_id / user_count / tier heuristics in
-- ``schema_inspector/live_dispatch_policy.py``. When a row exists for
-- a given unique_tournament_id, the live worker dispatches every event
-- of that tournament to the override tier regardless of what the
-- heuristic would have picked. Use it to:
--
--   * force a top league into tier_1 when Sofascore did not flag it
--     (Premier League's ``tier`` column is NULL on prod);
--   * demote an over-eager tournament (e.g. a youth league flagged
--     tier_1 by detail_id but not actually worth tier_1 budget);
--   * test a tier change without rebuilding the whole policy table.
--
-- The orchestrator reads the override on startup and refreshes its
-- in-memory cache every 5 minutes. Manual inserts via psql do not
-- pick up live; admin UI (Phase 2) will trigger a reload.

CREATE TABLE IF NOT EXISTS live_tier_override (
    unique_tournament_id BIGINT PRIMARY KEY,
    override_tier        TEXT   NOT NULL,
    reason               TEXT   NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by           TEXT   NULL,
    CONSTRAINT live_tier_override_tier_check
        CHECK (override_tier IN ('tier_1', 'tier_2', 'tier_3')),
    CONSTRAINT live_tier_override_unique_tournament_id_fkey
        FOREIGN KEY (unique_tournament_id) REFERENCES unique_tournament (id)
);

COMMENT ON TABLE live_tier_override IS
    'Operator-controlled per-UT live-tier override. See schema_inspector/live_dispatch_policy.py:resolve_live_dispatch_tier.';
COMMENT ON COLUMN live_tier_override.override_tier IS
    'tier_1 = top priority (5s poll), tier_2 = mid (15s), tier_3 = low (30s).';
COMMENT ON COLUMN live_tier_override.reason IS
    'Free-form operator note (e.g. "Premier League — has_xg true but Sofascore tier=NULL").';

-- Index used by orchestrator startup load.
CREATE INDEX IF NOT EXISTS idx_live_tier_override_unique_tournament_id
    ON live_tier_override (unique_tournament_id);
