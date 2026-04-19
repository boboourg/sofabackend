-- Fix #1: add nullable scope_key column to api_payload_snapshot.
--
-- Purpose: provide a stable per-scope identifier that can be combined with
-- payload_hash for deduplication via a partial unique index (see companion
-- migration 2026-04-20_api_payload_snapshot_scope_hash_uniq.sql).
--
-- The column is nullable and written by application code. Legacy rows keep
-- scope_key = NULL and are intentionally excluded from the dedup index to
-- avoid a multi-hour rewrite of a 15GB, 2.1M-row table.
--
-- On PostgreSQL 11+ ADD COLUMN with a NULL default is a metadata-only
-- operation; it acquires only a brief ACCESS EXCLUSIVE lock and is safe to
-- run online.

BEGIN;

ALTER TABLE api_payload_snapshot
    ADD COLUMN IF NOT EXISTS scope_key TEXT;

COMMIT;
