-- Indexes for the scheduled-events / live-events synthesizer (2026-05-18).
--
-- The synthesizer (see schema_inspector/scheduled_events_synthesizer.py)
-- joins event → tournament → category → sport to filter by sport_slug.
-- Without an index on tournament(category_id) PG fell back to a
-- Parallel Seq Scan over all 60k tournaments, costing ~9.2 s out of a
-- 15 s total query time:
--
--   Parallel Seq Scan on tournament  (cost=0.00..37751.91 rows=25191)
--     Hash Cond: (t.category_id = c.id)
--     Buffers: shared hit=3329 read=34171 ...
--
-- After this index PG can drive the join from
-- ``category WHERE sport_id = football_sport.id`` → tournament via
-- index → event via idx_event_tournament_id → leaves. Expected total
-- query time: 50-200 ms (down from 15 s).
--
-- CONCURRENTLY so prod writes are not blocked while the build runs.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tournament_category_id
    ON tournament (category_id);

COMMENT ON INDEX idx_tournament_category_id IS
    'B-tree (category_id) — drives the synthesizer''s sport→category→'
    'tournament→event join. Without it the planner uses a parallel '
    'seq scan over all tournaments. Bobur ACK 2026-05-18.';
