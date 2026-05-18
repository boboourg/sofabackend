-- Materialized payload cache for hot read endpoints (Variant B, Stage 1).
--
-- Why this exists:
--   The waterfall in local_api_server (api_payload_snapshot lookup +
--   _reconcile_snapshot_payload overlay + per-resource synthesis) is
--   correct but does extra work per request:
--     - one SELECT against api_payload_snapshot (filtered by source_url
--       LIKE prefix, then linear scan up to 500 rows to match exact
--       path+query),
--     - 1-N extra fetches from event/event_score/event_status/event_time
--       to overlay volatile fields,
--     - JSONB decode + Python-side merge.
--
--   For the top read endpoints (mobile main screen polls
--   /sport/{slug}/events/live every 5 seconds, /event/{id} every 5
--   seconds, sub-resources on match-open), the cost dominates p50/p95.
--
-- Design:
--   Lazy materialization layer keyed by a synthetic cache_key. Reader
--   checks the cache first; on miss it falls through to the normal
--   waterfall and writes back. Invalidation is TTL-based with a
--   freshness gate against event.updated_at when the key is event-scoped.
--
--   The table is intentionally separate from api_payload_snapshot
--   (which stores raw upstream history) and api_snapshot_head (which
--   is a pointer to latest snapshot, not a built payload).
--
-- Scope of v1:
--   Hot endpoints:
--     - /api/v1/event/{event_id}
--     - /api/v1/event/{event_id}/incidents
--     - /api/v1/event/{event_id}/lineups
--     - /api/v1/event/{event_id}/statistics
--     - /api/v1/sport/{slug}/events/live
--
--   Not in scope yet: list endpoints with pagination (events/last/{p}),
--   long-tail endpoints, anything where build cost is already <5ms.

CREATE TABLE IF NOT EXISTS event_payload_cache (
    cache_key            TEXT PRIMARY KEY,
    endpoint_pattern     TEXT NOT NULL,
    context_entity_type  TEXT,                  -- 'event' | 'sport' | NULL
    context_entity_id    BIGINT,                -- numeric event_id, or NULL when key is sport-scoped
    sport_slug           TEXT,                  -- 'football' | 'tennis' | ... | NULL
    payload              JSONB NOT NULL,
    payload_hash         TEXT NOT NULL,         -- md5(payload::text) for write-avoidance
    generated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at           TIMESTAMPTZ NOT NULL,  -- generated_at + ttl
    source_version       JSONB,                 -- e.g. {"event_updated_at": "...", "snapshot_id": 123}
    hit_count            BIGINT NOT NULL DEFAULT 0,
    last_hit_at          TIMESTAMPTZ
);

-- Used by the writer when inserting a new key. UNIQUE by primary key, so
-- no separate unique index needed.

-- Used to invalidate every cache row tied to a given event whenever the
-- bulk live snapshot parser updates event/event_score/event_status/event_time.
CREATE INDEX IF NOT EXISTS idx_event_payload_cache_context_event
    ON event_payload_cache (context_entity_id)
    WHERE context_entity_type = 'event';

-- Used by the housekeeping sweeper that purges expired rows.
CREATE INDEX IF NOT EXISTS idx_event_payload_cache_expires_at
    ON event_payload_cache (expires_at);

-- Used by /sport/{slug}/events/live lookups (one row per sport).
CREATE INDEX IF NOT EXISTS idx_event_payload_cache_sport
    ON event_payload_cache (sport_slug)
    WHERE sport_slug IS NOT NULL;

COMMENT ON TABLE event_payload_cache IS
    'Lazy materialized payload cache for top-read endpoints. '
    'Reader checks here first; miss falls back to the snapshot waterfall '
    'and writes back. Invalidation: TTL (expires_at) + freshness gate '
    'against event.updated_at for event-scoped keys. Variant B Stage 1 '
    '(2026-05-18).';

COMMENT ON COLUMN event_payload_cache.cache_key IS
    'Synthetic key: "<endpoint_pattern>|<context>". '
    'Examples: "/api/v1/event/{event_id}|15171570", '
    '"/api/v1/sport/football/events/live|football".';

COMMENT ON COLUMN event_payload_cache.payload_hash IS
    'md5 of payload::text. Used by the writer to skip the UPDATE when '
    'the freshly-built payload is byte-identical to the cached one '
    '(write avoidance — keeps autovacuum noise low).';

COMMENT ON COLUMN event_payload_cache.source_version IS
    'Snapshot of the inputs that produced this payload. Reader uses it '
    'to decide whether to honour the cached row or rebuild — e.g. '
    'compare current event.updated_at to source_version->>"event_updated_at".';
