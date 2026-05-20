-- 2026-05-20 — historical layer Stage 3.1: champion-per-season.
--
-- Why: ``unique_tournament.title_holder_team_id`` is overwritten on every
-- upsert (competition_repository._upsert_unique_tournaments). The 2026-05-20
-- architecture audit (audit B) confirmed this is the only place a champion
-- is recorded, so the history of past champions is lost as soon as a new
-- season's payload lands. The composite-PK table below gives each
-- ``(unique_tournament_id, season_id)`` exactly one row — natural per-season
-- historicity without any DELETE+INSERT or write-path acrobatics.
--
-- Population path: the stream-flow ``SeasonStandingsParser`` (also added in
-- Stage 3) extracts ``rows[0].team.id`` from a final ``/standings/{scope=total}``
-- snapshot and emits a metric row consumed by ``normalize_repository``.
-- The UPSERT there uses Stage 1.2 ``IS DISTINCT FROM`` so identical re-ingests
-- (the same season closing snapshot fetched many times) do not bloat the
-- table.
--
-- Forward + backward derivation: the same ``team_id`` can be reconstructed
-- from existing data (final ``standing_row.position = 1`` joined to
-- ``standing.season_id``), so this table is also writable by a backfill
-- pass without re-fetching anything from upstream.

BEGIN;

CREATE TABLE IF NOT EXISTS unique_tournament_season_champion (
    unique_tournament_id BIGINT NOT NULL REFERENCES unique_tournament(id),
    season_id BIGINT NOT NULL REFERENCES season(id),
    team_id BIGINT NOT NULL REFERENCES team(id),
    -- ``ordinal=1`` for the literal first-place finisher. Reserved for
    -- future use if Sofascore changes a 2nd-place runner-up into a
    -- co-champion (rare cup-tournament case).
    ordinal INTEGER NOT NULL DEFAULT 1,
    -- ``source`` lets readers tell apart rows that came from a live
    -- standings snapshot vs a derivation pass that read the final
    -- ``standing_row`` for an already-locked season.
    --   - ``'standings'``       — extracted from /standings/{scope=total}
    --   - ``'standings_backfill'`` — derived offline from existing
    --                            standing_row rows during historical
    --                            re-runs.
    source TEXT NOT NULL DEFAULT 'standings',
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (unique_tournament_id, season_id)
);

-- Reverse lookup: "how many titles has Real Madrid won across all leagues
-- and seasons?". Without this index, queries by team_id would seq-scan
-- the whole table.
CREATE INDEX IF NOT EXISTS idx_unique_tournament_season_champion_team
    ON unique_tournament_season_champion(team_id);

COMMIT;
