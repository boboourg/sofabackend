-- Performance audit Phase 1 (2026-05-20)
-- Read PERFORMANCE_AUDIT_2026-05-20.md for the analysis.
--
-- Apply with CONCURRENTLY (does not block writes). Run each
-- statement separately if your client can't use multi-statement
-- transactions with CONCURRENTLY.

-- =====================================================================
-- INDEX 1: event_market_choice — fix 28.7B seq_tup_read in prod
-- =====================================================================
-- Root cause: ws_odds_writer.py:97-104 does
--   UPDATE event_market_choice SET ... WHERE event_market_id = $1 AND name = $2
-- without a backing index. PK is on source_id which is irrelevant
-- to this hot UPDATE path. Same query is also used by FK ON DELETE
-- CASCADE from event_market.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_market_choice_market_name
    ON event_market_choice (event_market_id, name);

-- =====================================================================
-- INDEX 2: event.custom_id — fix reconcile path seq scan
-- =====================================================================
-- local_api_server.py:1170-1207 (_reconcile_snapshot_payload) does
--   WHERE e.id = ANY($1::bigint[])
--      OR (cardinality($2::text[]) > 0 AND e.custom_id = ANY($2::text[]))
-- on every /scheduled-events/{date} and /sport/{slug}/events/live
-- response. The OR-branch without an index = seq scan on 4.5M rows.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_custom_id
    ON event (custom_id)
    WHERE custom_id IS NOT NULL;

-- =====================================================================
-- INDEX 3: event live window partial
-- =====================================================================
-- scheduled_events_synthesizer.py:_FETCH_QUERY_LIVE filters on
-- status_code (live statuses), start_timestamp (12h window), and
-- is_editor IS NOT TRUE. Existing single-column indexes don't cover
-- the composite path; this partial keeps the index small (editor
-- events are a tiny fraction) and lets PG use idx-only scan for the
-- live list path.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_live_window
    ON event (status_code, start_timestamp DESC, id DESC)
    WHERE is_editor IS NOT TRUE;

-- =====================================================================
-- INDEX 4 + 5: etl_job_run — fix 3.2B seq_tup_read (42.8% seq scan ratio)
-- =====================================================================
-- Queries by job_id, by (created_at + status filter for retries/dead jobs).
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_job_run_created_status
    ON etl_job_run (created_at DESC, status)
    WHERE status IN ('failed', 'retry_scheduled');

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_etl_job_run_job_id
    ON etl_job_run (job_id);
