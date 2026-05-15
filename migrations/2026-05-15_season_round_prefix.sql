-- 2026-05-15: add nullable `round_prefix` column to season_round.
--
-- Sofascore /unique-tournament/{ut}/season/{s}/rounds payload includes
-- an optional `prefix` field for rounds in cup competitions (e.g.
-- {"round": 636, "name": "Playoff round", "slug": "playoff-round",
--  "prefix": "Qualification"}). League payloads omit it and emit
-- simple {"round": N} rows.
--
-- Background: prefix was never read by the family parser nor stored,
-- so the API handler emitted name+slug but never prefix. Adding the
-- column closes the end-to-end gap (parser -> repo -> handler) and
-- preserves Sofascore-shape 1:1 for cup tournaments.
--
-- Safe to apply online: ADD COLUMN with no DEFAULT and nullable.

ALTER TABLE season_round
    ADD COLUMN IF NOT EXISTS round_prefix TEXT;
