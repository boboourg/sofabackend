-- Fix #1: partial unique index on (scope_key, payload_hash).
--
-- Purpose: atomic deduplication key for api_payload_snapshot inserts.
-- Combined with INSERT ... ON CONFLICT DO UPDATE ... RETURNING id this
-- replaces the previous SELECT-then-INSERT CTE in
-- RawRepository.insert_payload_snapshot_if_missing_returning_id, which
-- used non-sargable "IS NOT DISTINCT FROM" comparisons and was prone to
-- races between concurrent workers.
--
-- The predicate excludes legacy rows where either scope_key or payload_hash
-- is NULL (historically about 72% of the table, ~1.5M rows). Those rows
-- stay in the heap as cold data and remain readable via existing indexes;
-- they are simply not part of the new dedup path. No destructive cleanup
-- is performed by this migration.
--
-- IMPORTANT: CREATE INDEX CONCURRENTLY cannot run inside a transaction
-- block. Do not wrap this file with BEGIN / COMMIT. Apply with:
--   sudo -u postgres psql -p 5432 -d sofascore_schema_inspector \
--       -v ON_ERROR_STOP=1 \
--       -f migrations/2026-04-20_api_payload_snapshot_scope_hash_uniq.sql
--
-- Expected build time: 10-30 minutes on the 15GB, 2.1M-row production
-- table. The build does not block concurrent readers or writers.

CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uniq_api_payload_snapshot_scope_hash
    ON api_payload_snapshot (scope_key, payload_hash)
    WHERE scope_key IS NOT NULL AND payload_hash IS NOT NULL;
