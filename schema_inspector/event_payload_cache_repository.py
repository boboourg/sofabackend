"""Lazy materialized payload cache for hot read endpoints (Variant B).

Reader checks ``event_payload_cache`` first; on miss the local API
falls back to the standard waterfall (api_payload_snapshot lookup +
overlay) and writes the freshly built payload back into the cache.

The cache is intentionally light-weight:
  * single table, indexed by synthetic ``cache_key``;
  * per-key TTL based on the event's status_type (live → 5s, scheduled
    → 30s, finished → 1h);
  * write avoidance via ``payload_hash`` (asyncpg encodes the row once,
    so identical payloads keep the existing row instead of rewriting it
    and producing autovacuum churn);
  * explicit invalidation by event_id (callable from any path that
    persists event/event_score/event_status/event_time updates).

This module is a thin wrapper over asyncpg — it does not own a pool;
callers pass in a live connection or a transactional executor.
"""
from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)


_DEFAULT_TTL_SECONDS = 10

# Task 2 Phase D (2026-05-20): events whose
# ``event_terminal_state.locked_at IS NOT NULL`` are frozen forever.
# Their cached payload is immutable, so a 24-hour TTL is a safe
# minimum — the read-path also reads the lock flag directly and can
# return the snapshot 1:1 even after this expiry. Treat as a perf
# hint, not a correctness gate.
LOCKED_TTL_SECONDS = 86400

# TTL policy by upstream ``status.type``. Mirrors the Redis api:resp:*
# layer that the local_api_server already uses, so the persistent cache
# has the same staleness window as the in-memory one.
_TTL_BY_STATUS = {
    "inprogress": 5,        # live
    "halftime": 5,          # live (intermission)
    "extratime": 5,         # live
    "penalties": 5,         # live (shoot-out)
    "interrupted": 5,
    "delayed": 5,
    "willcontinue": 5,
    "notstarted": 30,       # scheduled / pre-match
    "finished": 3600,       # post-match
    "aet": 3600,
    "ap": 3600,
    "postponed": 3600,
    "cancelled": 3600,
    "canceled": 3600,
    "suspended": 3600,
    "abandoned": 3600,
    "ended": 3600,
}


def make_cache_key(
    endpoint_pattern: str,
    context_entity_type: str | None,
    context_entity_id: int | str | None,
) -> str:
    """Build the synthetic primary key for an event_payload_cache row.

    The format is ``<endpoint_pattern>|<entity_type>|<entity_id>`` so a
    single PK lookup resolves both event-scoped (``event|15171570``)
    and sport-scoped (``sport|football``) requests uniformly.
    """
    return f"{endpoint_pattern}|{context_entity_type or ''}|{context_entity_id if context_entity_id is not None else ''}"


def resolve_ttl_seconds(status_type: str | None) -> int:
    """TTL policy for a payload whose underlying event has the given
    upstream status_type. None / unknown maps to a short conservative
    default (10s) so an unrecognised state does not pin a stale row
    longer than necessary."""
    if status_type is None:
        return _DEFAULT_TTL_SECONDS
    return _TTL_BY_STATUS.get(status_type.lower(), _DEFAULT_TTL_SECONDS)


def _coerce_payload(value: Any) -> Any:
    """asyncpg returns JSONB as ``str`` unless a codec is registered,
    so we decode here once. Pass-through for dict/list."""
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            return json.loads(value.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return value


async def read_cache(connection: Any, cache_key: str) -> Any | None:
    """Return the cached payload for ``cache_key`` if it is still fresh,
    else None. ``expires_at`` is checked in Python (rather than via SQL
    ``WHERE expires_at > now()``) so the read does not require a
    server-side now() roundtrip and the freshness window honours the
    caller's clock — important when this layer runs inside a long-lived
    asyncio task that may have drifted from the DB clock by a few ms.
    """
    row = await connection.fetchrow(
        """
        SELECT cache_key, payload, expires_at, source_version
        FROM event_payload_cache
        WHERE cache_key = $1
        """,
        cache_key,
    )
    if row is None:
        return None
    expires_at = row.get("expires_at") if isinstance(row, dict) else row["expires_at"]
    if expires_at is None:
        return None
    if isinstance(expires_at, datetime):
        now = datetime.now(tz=expires_at.tzinfo or timezone.utc)
        if expires_at <= now:
            return None
    payload = row.get("payload") if isinstance(row, dict) else row["payload"]
    return _coerce_payload(payload)


async def write_cache(
    connection: Any,
    *,
    cache_key: str,
    endpoint_pattern: str,
    context_entity_type: str | None,
    context_entity_id: int | None,
    sport_slug: str | None,
    payload: Any,
    ttl_seconds: int,
    source_version: dict[str, Any] | None,
) -> None:
    """Insert-or-update a single cache row.

    Write avoidance: when the freshly-built payload hashes to the same
    value as the cached one, the row is left alone (only its
    ``last_hit_at`` / ``hit_count`` would be updated by the read path,
    not by write). asyncpg keeps the same WAL footprint either way,
    but skipping the UPDATE removes downstream replication noise and
    avoids autovacuum work.
    """
    if payload is None:
        return
    encoded = json.dumps(payload, separators=(",", ":"), default=str)
    payload_hash = hashlib.md5(encoded.encode("utf-8")).hexdigest()
    generated_at = datetime.now(tz=timezone.utc)
    expires_at = generated_at + timedelta(seconds=max(int(ttl_seconds), 1))
    source_version_encoded = (
        json.dumps(source_version, default=str) if source_version is not None else None
    )
    await connection.execute(
        """
        INSERT INTO event_payload_cache (
            cache_key, endpoint_pattern, context_entity_type, context_entity_id,
            sport_slug, payload, payload_hash, generated_at, expires_at,
            source_version, hit_count, last_hit_at
        )
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, $9, $10::jsonb, 0, NULL)
        ON CONFLICT (cache_key) DO UPDATE SET
            payload = CASE
                WHEN event_payload_cache.payload_hash IS DISTINCT FROM EXCLUDED.payload_hash
                THEN EXCLUDED.payload
                ELSE event_payload_cache.payload
            END,
            payload_hash = EXCLUDED.payload_hash,
            generated_at = EXCLUDED.generated_at,
            expires_at = EXCLUDED.expires_at,
            source_version = EXCLUDED.source_version,
            endpoint_pattern = EXCLUDED.endpoint_pattern,
            context_entity_type = EXCLUDED.context_entity_type,
            context_entity_id = EXCLUDED.context_entity_id,
            sport_slug = EXCLUDED.sport_slug
        """,
        cache_key,
        endpoint_pattern,
        context_entity_type,
        context_entity_id,
        sport_slug,
        encoded,
        payload_hash,
        generated_at,
        expires_at,
        source_version_encoded,
    )


async def invalidate_event(connection: Any, *, event_id: int) -> None:
    """Drop every cache row tied to a given event_id.

    Called from the bulk live snapshot parser (and any other writer
    that updates event/event_score/event_status/event_time) so the
    next read repopulates the cache from fresh normalized state.
    """
    await connection.execute(
        """
        DELETE FROM event_payload_cache
        WHERE context_entity_type = 'event'
          AND context_entity_id = $1
        """,
        event_id,
    )


async def invalidate_sport(connection: Any, *, sport_slug: str) -> None:
    """Drop every cache row tied to a given sport_slug (used to
    invalidate /sport/{slug}/events/live on bulk live updates)."""
    await connection.execute(
        """
        DELETE FROM event_payload_cache
        WHERE sport_slug = $1
        """,
        sport_slug,
    )


async def sweep_expired(connection: Any, *, limit: int = 1000) -> int:
    """Housekeeping: delete up to ``limit`` rows whose expires_at is in
    the past. Returns the number of rows removed. Intended to be
    called periodically by the maintenance worker (every minute or so)."""
    result = await connection.execute(
        """
        DELETE FROM event_payload_cache
        WHERE cache_key IN (
            SELECT cache_key FROM event_payload_cache
            WHERE expires_at <= now()
            LIMIT $1
        )
        """,
        limit,
    )
    # asyncpg returns 'DELETE N'; extract the count.
    if isinstance(result, str) and result.startswith("DELETE "):
        try:
            return int(result.split(" ", 1)[1])
        except (IndexError, ValueError):
            return 0
    return 0
