"""``TeamWithUpcomingMatchResolver`` — pre-match-only team scope.

Stage 5.1 (2026-05-21). Created to gate ``TEAM_FEATURED_PLAYERS_ENDPOINT``
to teams that actually have an upcoming football match in the next 14
days. The previous ``team-of-active-ut`` scope returned every team in
every active UT once a day (regardless of fixture calendar), which
inflated the fetch volume for an editorial resource — featured-players
is keyed by upcoming event_id, so it changes only when a new fixture
appears.

Resolver semantics
------------------

Returns ``ResourceTarget(entity_type="team", entity_id=<team_id>)`` for
every distinct team_id that appears as home OR away on an ``event``
whose ``status_code = 0`` (notstarted) AND ``start_timestamp BETWEEN
now AND now + 14 days`` (window configurable via env).

Combined with ``TEAM_FEATURED_PLAYERS_ENDPOINT.freshness_ttl_seconds =
14 * 86400`` the behaviour is:

* Team enters the 14-day pre-match window for the first time →
  resolver yields target → planner publishes refresh job (freshness
  miss) → upstream fetched once.
* Team already fetched within ``freshness_ttl_seconds`` → planner
  skips → no upstream call.
* Team is in live or finished states with no future match in window
  → resolver does NOT yield target → no job published.
* New future match appears after the previous one finished → resolver
  yields target again → freshness TTL check decides whether to fetch
  (typically yes if ≥14 days passed).

This collapses the 13-14 snapshots/team/13 days observed in the prior
``team-of-active-ut`` scope down to ~1 per fixture cycle.

Configuration
-------------

``SCHEMA_INSPECTOR_RESOURCE_UPCOMING_WINDOW_DAYS`` — future window in
days (default 14). The window must comfortably cover the longest
pre-match holding period for a single team without being so long that
distant fixtures inflate the resolver output.

``SCHEMA_INSPECTOR_RESOURCE_UPCOMING_LIMIT`` — hard cap on the number
of teams returned per resolver tick (default 100 000). Used as the
LIMIT clause on the SQL.

``SCHEMA_INSPECTOR_RESOURCE_UPCOMING_CACHE_TTL_SECONDS`` — Redis cache
TTL on the resolved set (default 600 s, half of the existing
``team-of-registry-ut`` TTL since the upcoming-match window shifts
faster).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

from .base import ResourceTarget

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_DAYS_FUTURE = 14
DEFAULT_LIMIT = 100_000
DEFAULT_CACHE_TTL_SECONDS = 600
CACHE_KEY = "set:resource_refresh:team_with_upcoming_match"

WINDOW_FUTURE_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_UPCOMING_WINDOW_DAYS"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_UPCOMING_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_UPCOMING_CACHE_TTL_SECONDS"


class TeamWithUpcomingMatchResolver:
    """Resolve teams whose next fixture sits inside the pre-match window."""

    kind = "team-with-upcoming-match"

    def __init__(
        self,
        *,
        database: Any,
        redis_backend: Any | None = None,
        env: dict[str, str] | None = None,
        cache_key: str = CACHE_KEY,
        sport_slug: str = "football",
    ) -> None:
        self.database = database
        self.redis_backend = redis_backend
        self.cache_key = cache_key
        self.sport_slug = sport_slug
        self._env = env if env is not None else dict(os.environ)

    @property
    def window_days_future(self) -> int:
        return _positive_int(self._env.get(WINDOW_FUTURE_ENV_KEY), DEFAULT_WINDOW_DAYS_FUTURE)

    @property
    def limit(self) -> int:
        return _positive_int(self._env.get(LIMIT_ENV_KEY), DEFAULT_LIMIT)

    @property
    def cache_ttl_seconds(self) -> int:
        return _positive_int(self._env.get(CACHE_TTL_ENV_KEY), DEFAULT_CACHE_TTL_SECONDS)

    async def resolve(self) -> Iterable[ResourceTarget]:
        team_ids = await self._read_cache()
        if team_ids is None:
            team_ids = await self._query_database()
            self._write_cache(team_ids)
        return tuple(
            ResourceTarget(
                entity_type="team",
                entity_id=int(team_id),
                path_params={"team_id": int(team_id)},
                sport_slug=self.sport_slug,
            )
            for team_id in team_ids
        )

    # ------------------------------------------------------------------

    async def _query_database(self) -> tuple[int, ...]:
        future_seconds = self.window_days_future * 86_400
        # status_code = 0 is the notstarted status (planner/live.py
        # TERMINAL_STATUS_TYPES + live_dispatch_policy classify 0 as
        # pre-match). The start_timestamp BETWEEN now AND now + window
        # filter additionally guarantees the match is FUTURE — without
        # it, an event sitting in notstarted status because Sofascore
        # never updated it would also qualify, which is undesirable.
        sql = """
        WITH upcoming AS (
            SELECT home_team_id AS team_id
            FROM event
            WHERE status_code = 0
              AND start_timestamp IS NOT NULL
              AND start_timestamp >= EXTRACT(EPOCH FROM now())::bigint
              AND start_timestamp <= EXTRACT(EPOCH FROM now())::bigint + $1::bigint
              AND home_team_id IS NOT NULL
            UNION
            SELECT away_team_id AS team_id
            FROM event
            WHERE status_code = 0
              AND start_timestamp IS NOT NULL
              AND start_timestamp >= EXTRACT(EPOCH FROM now())::bigint
              AND start_timestamp <= EXTRACT(EPOCH FROM now())::bigint + $1::bigint
              AND away_team_id IS NOT NULL
        )
        SELECT team_id FROM upcoming ORDER BY team_id LIMIT $2::bigint
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(sql, int(future_seconds), int(self.limit))
        team_ids = tuple(int(row["team_id"]) for row in rows if row["team_id"] is not None)
        logger.info(
            "TeamWithUpcomingMatchResolver: %s teams (window=%sd, sport=%s, limit=%s)",
            len(team_ids),
            self.window_days_future,
            self.sport_slug,
            self.limit,
        )
        return team_ids

    async def _read_cache(self) -> tuple[int, ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:  # noqa: BLE001 — defensive
            logger.warning(
                "TeamWithUpcomingMatchResolver: redis GET failed (fail-open): %s", exc
            )
            return None
        if raw in (None, ""):
            return None
        text = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, list):
            return None
        return tuple(
            int(value)
            for value in data
            if isinstance(value, (int, str)) and str(value).lstrip("-").isdigit()
        )

    def _write_cache(self, team_ids: tuple[int, ...]) -> None:
        if self.redis_backend is None:
            return
        try:
            payload = json.dumps(list(team_ids), separators=(",", ":"))
            self.redis_backend.set(self.cache_key, payload, ex=self.cache_ttl_seconds)
        except Exception as exc:  # noqa: BLE001 — defensive
            logger.warning("TeamWithUpcomingMatchResolver: redis SET failed: %s", exc)


def _positive_int(raw: object, default: int) -> int:
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default
