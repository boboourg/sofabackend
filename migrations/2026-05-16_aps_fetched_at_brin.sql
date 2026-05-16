-- BRIN index on api_payload_snapshot.fetched_at (2026-05-16).
--
-- Background: api_payload_snapshot is ~99 GB / 5.5M rows on prod. /ops/health
-- and /ops/snapshots/summary both run ``SELECT MAX(fetched_at) FROM
-- api_payload_snapshot`` without a WHERE clause. The only existing fetched_at
-- index is a PARTIAL B-tree limited to ``endpoint_pattern LIKE
-- '/api/v1/sport/%/events/live'`` rows — useless for the full MAX. The MAX
-- planner falls back to a seqscan over 99 GB and dies on asyncpg's default
-- 30s command_timeout, returning HTTP 500.
--
-- A full B-tree on fetched_at would cost ~1 GB of disk + slow inserts. A BRIN
-- index over fetched_at is ~10 KB (one summary per 128 pages by default; we
-- tighten to 32 to make the lookup a bit more selective on the tail) because
-- snapshots are inserted in (roughly) ascending fetched_at order, so each
-- block range has a tight min/max. The planner uses BRIN to skip most of the
-- table when running ``MAX(fetched_at)`` and similar range scans.
--
-- IMPORTANT: this index is created CONCURRENTLY so prod writes are not
-- blocked. CONCURRENTLY *cannot* run inside a transaction block — invoke this
-- file via ``psql -f`` so the autocommit boundary is preserved.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_aps_fetched_at_brin
    ON api_payload_snapshot USING brin (fetched_at)
    WITH (pages_per_range = 32);

COMMENT ON INDEX idx_aps_fetched_at_brin IS
    'BRIN over fetched_at to make /ops/health and /ops/snapshots/summary '
    'MAX(fetched_at) lookups O(BRIN ranges) instead of seqscan. '
    'Bobur ACK 2026-05-16.';
