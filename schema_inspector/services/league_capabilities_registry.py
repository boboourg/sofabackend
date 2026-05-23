"""League Capabilities Registry — read API for the hot path
(Phase 4.3, 2026-05-23).

Two-tier cache:

  1. Redis (hot path, 1h TTL) — scalar per-(UT, season, status, endpoint)
     key. Orchestrator reads here on every probe decision.
  2. PostgreSQL ``league_endpoint_capability`` table — authoritative
     store, written by the probe service.

Lookup contract:

  ``get_verdict(ut_id, season_id, status_type, endpoint_pattern)``
      → returns ``EndpointVerdict.ALLOWED | DISABLED | UNKNOWN``

  Resolution order:
    1. Redis cache for (ut, season, status, endpoint)
    2. Postgres row for the same quad
    3. Redis cache for (ut, NULL_season, status, endpoint)  — UT-level fallback
    4. Postgres row for UT-level fallback
    5. UNKNOWN (orchestrator falls back to legacy match_center_policy)

  Each Postgres hit primes Redis with a 1h TTL so the next call is
  served from cache.

Fail-safe philosophy:
  Any infrastructure failure (Redis down, DB timeout) returns
  ``UNKNOWN`` and the orchestrator uses legacy policy. The registry
  must never raise from the hot path.
"""

from __future__ import annotations

import enum
import logging
import os
from typing import Any

from ..storage.league_capabilities_repository import (
    STATE_ALLOWED,
    STATE_DISABLED,
    STATE_UNKNOWN,
    LeagueCapabilitiesRepository,
)


logger = logging.getLogger(__name__)


class EndpointVerdict(enum.Enum):
    ALLOWED = STATE_ALLOWED
    DISABLED = STATE_DISABLED
    UNKNOWN = STATE_UNKNOWN

    @classmethod
    def from_str(cls, value: str | None) -> "EndpointVerdict":
        if value == STATE_ALLOWED:
            return cls.ALLOWED
        if value == STATE_DISABLED:
            return cls.DISABLED
        return cls.UNKNOWN

    @property
    def cache_value(self) -> str:
        """Serializable value for Redis cache write."""
        return self.value


# Cache TTL: 1 hour. Probe verdicts in Postgres have 14-day TTL, so
# this cache lag is bounded by the slower of the two. Long enough to
# amortize DB roundtrips, short enough to pick up manual overrides
# from /ops/league-capabilities/set within an hour.
_DEFAULT_CACHE_TTL_SECONDS = 3600

# Phase 4.7.5 (2026-05-23): warmed entries get a 24h TTL instead of
# the 1h default — the Redis-only hot path must not lapse to DB
# between worker startups. 24h is long enough that even a forgotten
# refresh daemon doesn't strand the cache mid-day; short enough that
# a stale registry self-heals the next time the worker restarts.
_WARM_TTL_SECONDS = 24 * 3600

# Sentinel for UT-level (season_id IS NULL) cache keys. Distinguishes
# the fallback row from any real season_id=0 row (none exist — season
# IDs start at 1 in Sofascore).
_NULL_SEASON_SENTINEL = 0


_FEATURE_FLAG_ENV = "SOFASCORE_LEAGUE_CAPABILITIES_ENABLED"
_TRUTHY = frozenset({"1", "true", "yes", "on", "y", "t"})


async def resolve_capability_verdict(
    *,
    registry: "LeagueCapabilitiesRegistry | None",
    enabled: bool,
    unique_tournament_id: int | None,
    season_id: int | None,
    status_type: str | None,
    endpoint_pattern: str,
) -> str | None:
    """Phase 4.7 wire (2026-05-23): orchestrator-side resolver.

    Returns the capability_verdict string ('allowed' / 'disabled' /
    'unknown') for the gate functions, or None when:
      * Feature flag is OFF.
      * No registry instance configured.
      * unique_tournament_id or status_type missing (can't lookup).
      * Registry raises (Redis down, DB timeout) — fail-safe.

    A None return tells the gate functions to fall back to legacy
    tier-based logic. This helper is the single integration point
    so orchestrator code stays clean:

        verdict = await resolve_capability_verdict(...)
        if not football_edge_allowed(..., capability_verdict=verdict):
            continue
    """

    if not enabled or registry is None:
        return None
    if unique_tournament_id is None or status_type is None:
        return None
    try:
        verdict = await registry.get_verdict(
            unique_tournament_id=int(unique_tournament_id),
            season_id=None if season_id is None else int(season_id),
            status_type=str(status_type),
            endpoint_pattern=str(endpoint_pattern),
        )
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning(
            "resolve_capability_verdict swallowed registry error: %s",
            exc,
        )
        return None
    return verdict.cache_value


def is_league_capabilities_enabled() -> bool:
    """Phase 4.7 (2026-05-23): operator dial controlling whether the
    orchestrator consults the registry before policy gating. Default
    OFF — toggling ON in .env + rolling restart enables the feature.

    Accepts truthy variants: 1/true/yes/on/y/t (case-insensitive).
    Anything else (empty, missing, false, 0, no) is OFF.
    """

    raw = os.environ.get(_FEATURE_FLAG_ENV, "").strip().lower()
    return raw in _TRUTHY


class LeagueCapabilitiesRegistry:
    """Read-side facade combining Redis cache + Postgres repository."""

    def __init__(
        self,
        *,
        redis_backend: Any,
        database: Any,
        repository: LeagueCapabilitiesRepository | None = None,
        cache_ttl_seconds: int = _DEFAULT_CACHE_TTL_SECONDS,
    ) -> None:
        self.redis_backend = redis_backend
        self.database = database
        # Phase 4.7.5 (2026-05-23): one-shot warm flag. Set to True after
        # the first warm_cache_from_db call (successful or not) so the
        # lazy-warm in get_verdicts_batch never duplicates the work and
        # never retries a failed warm — a failed warm degrades gracefully
        # to empty-result + legacy fallback, identical to flag-off
        # behaviour.
        self._warmed: bool = False
        self.repository = repository or LeagueCapabilitiesRepository()
        self.cache_ttl_seconds = int(cache_ttl_seconds)

    @staticmethod
    def _cache_key(
        *,
        unique_tournament_id: int,
        season_id: int | None,
        status_type: str,
        endpoint_pattern: str,
    ) -> str:
        # Endpoint pattern can contain '{', '}', '/' — Redis key chars
        # are unrestricted but for readability we keep them. Sentinel
        # for NULL season_id so UT-level rows have a distinct key.
        season_segment = (
            str(_NULL_SEASON_SENTINEL)
            if season_id is None
            else str(int(season_id))
        )
        return (
            f"lcap:{int(unique_tournament_id)}:{season_segment}:"
            f"{status_type}:{endpoint_pattern}"
        )

    async def get_verdict(
        self,
        *,
        unique_tournament_id: int,
        season_id: int | None,
        status_type: str,
        endpoint_pattern: str,
    ) -> EndpointVerdict:
        """Return cached or DB verdict. Never raises — infrastructure
        failures degrade gracefully to UNKNOWN."""

        # 1) Redis hit (season-specific)
        verdict = self._read_cache(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            status_type=status_type,
            endpoint_pattern=endpoint_pattern,
        )
        if verdict is not None:
            return verdict

        # 2) Postgres hit (season-specific)
        verdict = await self._read_db(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            status_type=status_type,
            endpoint_pattern=endpoint_pattern,
        )
        if verdict is not None:
            self._write_cache(
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                status_type=status_type,
                endpoint_pattern=endpoint_pattern,
                verdict=verdict,
            )
            return verdict

        # 3) UT-level fallback (season_id=NULL) — only if we were
        # asked for a specific season originally. Skip if caller
        # already asked for NULL.
        if season_id is not None:
            # Redis hit for UT-level
            verdict = self._read_cache(
                unique_tournament_id=unique_tournament_id,
                season_id=None,
                status_type=status_type,
                endpoint_pattern=endpoint_pattern,
            )
            if verdict is not None:
                return verdict
            # Postgres hit for UT-level
            verdict = await self._read_db(
                unique_tournament_id=unique_tournament_id,
                season_id=None,
                status_type=status_type,
                endpoint_pattern=endpoint_pattern,
            )
            if verdict is not None:
                self._write_cache(
                    unique_tournament_id=unique_tournament_id,
                    season_id=None,
                    status_type=status_type,
                    endpoint_pattern=endpoint_pattern,
                    verdict=verdict,
                )
                return verdict

        # 4) Fail-safe — orchestrator will consult legacy policy.
        return EndpointVerdict.UNKNOWN

    async def warm_cache_from_db(self) -> int:
        """Phase 4.7.6 Track 1 Step 2 (2026-05-23): worker-side no-op.

        Phase 4.7.5 had this method run one bulk SELECT to populate
        Redis at worker startup. That race was the root cause of the
        third Phase 4.8 rollback — 73 workers × bulk SELECT × asyncpg
        pool not yet ready at restart = mass TimeoutError, ~42% of
        workers stranded with empty caches.

        The new design: workers NEVER read the registry table. Cache
        priming is done out-of-band by a separate process running
        ``cli league-capability prime-redis`` (Phase 4.7.6 Step 3) —
        once at deploy time and again from a systemd timer / cron
        every N hours.

        This method stays so the lazy ``if not self._warmed: await
        self.warm_cache_from_db()`` hook in ``get_verdicts_batch``
        keeps working — it just flips the flag and returns. No DB
        connection, no SELECT, no Redis writes.
        """

        self._warmed = True
        return 0

    async def get_verdicts_batch(
        self,
        *,
        unique_tournament_id: int,
        season_id: int | None,
        status_type: str,
        endpoint_patterns: tuple[str, ...],
    ) -> dict[str, EndpointVerdict]:
        """Phase 4.7.4 (2026-05-23): bulk verdict resolve in at most two
        Postgres roundtrips instead of one per pattern.

        Replaces the 12 sequential ``get_verdict`` calls that the detail-
        spec fanout was making per match-center fetch — those caused
        asyncpg pool starvation under the Phase 4.8 production flip
        (workers hit the 30 s statement-timeout, throughput dropped 17-
        20×). The batch version:

          * Reads all requested patterns from Redis first (sync, cheap).
          * On any miss, opens ONE connection and runs a single
            ``fetch_capabilities_for_quad`` SELECT for the season-
            specific quad. Hits are written back to Redis and added to
            the result dict.
          * Patterns still missing after that fall through to a second
            single-SELECT UT-level fallback (season_id=NULL).
          * Patterns absent from both DB queries are *omitted* from the
            returned dict — the caller (orchestrator detail filter)
            treats absence as "fall back to legacy", which is the
            correct semantics (UNKNOWN would short-circuit the gate to
            'no info' instead of letting legacy tier logic decide).

        Fail-safe: any infrastructure failure (Redis down, DB timeout)
        returns whatever was already resolved without raising. Empty
        ``endpoint_patterns`` → ``{}``.
        """

        if not endpoint_patterns:
            return {}

        # Phase 4.7.5 (2026-05-23): lazy startup warm. The first call
        # primes Redis from one bulk SELECT; every call after that —
        # including subsequent calls from the same worker — serves
        # purely from Redis. ``warm_cache_from_db`` is idempotent and
        # sets the ``_warmed`` flag in both the success and failure
        # branches so we never retry on the hot path.
        if not self._warmed:
            await self.warm_cache_from_db()

        result: dict[str, EndpointVerdict] = {}
        for pattern in endpoint_patterns:
            cached = self._read_cache(
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                status_type=status_type,
                endpoint_pattern=pattern,
            )
            if cached is not None:
                result[str(pattern)] = cached
        # Phase 4.7.5: no per-quad DB fallback. After warm every active
        # row is already in Redis (24h TTL). Patterns absent from the
        # warmed cache are absent from the registry — the caller treats
        # them as "fall back to legacy", which is the correct
        # semantics. The earlier per-quad fallback was the second-tier
        # DB hit that, combined with concurrent worker pressure, still
        # triggered Phase 4.8 pool starvation despite the Phase 4.7.4
        # batch fix.
        return result

    async def _read_quad_db(
        self,
        *,
        unique_tournament_id: int,
        season_id: int | None,
        status_type: str,
    ) -> list:
        """Single SELECT for a (UT, season, status) triple. Returns ``[]``
        on any infra failure — never raises."""
        try:
            async with self.database.connection() as connection:
                rows = await self.repository.fetch_capabilities_for_quad(
                    connection,
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    status_type=status_type,
                )
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning(
                "LeagueCapabilitiesRegistry batch db.fetch failed "
                "ut=%s season=%s err=%s",
                unique_tournament_id, season_id, exc,
            )
            return []
        return rows or []

    def _absorb_rows(
        self,
        *,
        rows,
        result: dict[str, EndpointVerdict],
        remaining: list[str],
        cache_season_id: int | None,
        status_type: str,
        unique_tournament_id: int,
    ) -> None:
        """Promote DB rows into the result dict, prime Redis for each
        hit, and shrink the ``remaining`` list in place."""
        rows_by_pattern = {str(row.endpoint_pattern): row for row in rows}
        still_remaining: list[str] = []
        for pattern in remaining:
            row = rows_by_pattern.get(pattern)
            if row is None:
                still_remaining.append(pattern)
                continue
            verdict = EndpointVerdict.from_str(row.state)
            result[pattern] = verdict
            self._write_cache(
                unique_tournament_id=unique_tournament_id,
                season_id=cache_season_id,
                status_type=status_type,
                endpoint_pattern=pattern,
                verdict=verdict,
            )
        remaining[:] = still_remaining

    async def invalidate_quad(
        self,
        *,
        unique_tournament_id: int,
        season_id: int | None,
        status_type: str,
        endpoint_patterns: tuple[str, ...] = (),
    ) -> int:
        """Evict cached verdicts for the given (UT, season, status)
        quad. Called by the probe service after a re-probe to make
        new verdicts visible without waiting for the 1h TTL.

        Returns the number of keys deleted (best-effort)."""

        if not endpoint_patterns:
            return 0
        deleted = 0
        for pattern in endpoint_patterns:
            key = self._cache_key(
                unique_tournament_id=unique_tournament_id,
                season_id=season_id,
                status_type=status_type,
                endpoint_pattern=pattern,
            )
            try:
                deleted += int(self.redis_backend.delete(key) or 0)
            except Exception as exc:  # pragma: no cover — defensive
                logger.warning(
                    "LeagueCapabilitiesRegistry.invalidate_quad delete failed key=%s err=%s",
                    key, exc,
                )
        return deleted

    # ---- internals -----------------------------------------------------------

    def _read_cache(
        self,
        *,
        unique_tournament_id: int,
        season_id: int | None,
        status_type: str,
        endpoint_pattern: str,
    ) -> EndpointVerdict | None:
        key = self._cache_key(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            status_type=status_type,
            endpoint_pattern=endpoint_pattern,
        )
        try:
            value = self.redis_backend.get(key)
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning(
                "LeagueCapabilitiesRegistry redis.get failed key=%s err=%s",
                key, exc,
            )
            return None
        if value is None:
            return None
        return EndpointVerdict.from_str(str(value))

    async def _read_db(
        self,
        *,
        unique_tournament_id: int,
        season_id: int | None,
        status_type: str,
        endpoint_pattern: str,
    ) -> EndpointVerdict | None:
        try:
            async with self.database.connection() as connection:
                row = await self.repository.fetch_capability(
                    connection,
                    unique_tournament_id=unique_tournament_id,
                    season_id=season_id,
                    status_type=status_type,
                    endpoint_pattern=endpoint_pattern,
                )
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning(
                "LeagueCapabilitiesRegistry db.fetch failed ut=%s season=%s err=%s",
                unique_tournament_id, season_id, exc,
            )
            return None
        if row is None:
            return None
        return EndpointVerdict.from_str(row.state)

    def _write_cache(
        self,
        *,
        unique_tournament_id: int,
        season_id: int | None,
        status_type: str,
        endpoint_pattern: str,
        verdict: EndpointVerdict,
    ) -> None:
        key = self._cache_key(
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            status_type=status_type,
            endpoint_pattern=endpoint_pattern,
        )
        try:
            self.redis_backend.set(
                key, verdict.cache_value, ex=self.cache_ttl_seconds
            )
        except Exception as exc:  # pragma: no cover — defensive
            logger.warning(
                "LeagueCapabilitiesRegistry redis.set failed key=%s err=%s",
                key, exc,
            )
