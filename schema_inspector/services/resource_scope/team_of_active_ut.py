"""``TeamOfActiveUTResolver`` — SQL-driven scope of teams in active leagues.

A team is considered "active" if it appears in any ``standing_row`` whose
``standing.updated_at_timestamp`` is within the configured window. This is
the most reliable signal of "we recently ingested standings for this UT,
so its current squad matters". Other signals (events fixtures within N
days) cover too much surface (~70k teams) and would saturate the proxy
budget.

The resolver caches the SQL result in Redis for 30 minutes so each tick
of the planner does not re-run the query. Cache invalidation is by TTL
only -- new teams discovered through fresh standings ingest become
visible to the planner within at most 30 minutes.

Configuration::

    SCHEMA_INSPECTOR_RESOURCE_TEAM_ACTIVE_WINDOW_DAYS=30
    SCHEMA_INSPECTOR_RESOURCE_TEAM_ACTIVE_LIMIT=10000
    SCHEMA_INSPECTOR_RESOURCE_TEAM_ACTIVE_CACHE_TTL_SECONDS=1800
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

from .base import ResourceTarget

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_DAYS = 30
DEFAULT_LIMIT = 10_000
DEFAULT_CACHE_TTL_SECONDS = 1800
CACHE_KEY = "set:resource_refresh:team_active"

WINDOW_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_TEAM_ACTIVE_WINDOW_DAYS"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_TEAM_ACTIVE_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_TEAM_ACTIVE_CACHE_TTL_SECONDS"


class TeamOfActiveUTResolver:
    """Resolve teams from recently-updated standings, cached in Redis."""

    kind = "team-of-active-ut"

    def __init__(
        self,
        *,
        database: Any,
        redis_backend: Any | None = None,
        env: dict[str, str] | None = None,
        cache_key: str = CACHE_KEY,
    ) -> None:
        self.database = database
        self.redis_backend = redis_backend
        self.cache_key = cache_key
        self._env = env if env is not None else dict(os.environ)

    @property
    def window_days(self) -> int:
        return _positive_int(self._env.get(WINDOW_ENV_KEY), DEFAULT_WINDOW_DAYS)

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
            )
            for team_id in team_ids
        )

    # ------------------------------------------------------------------

    async def _query_database(self) -> tuple[int, ...]:
        cutoff_seconds = self.window_days * 86_400
        sql = """
        SELECT DISTINCT sr.team_id
        FROM standing_row AS sr
        JOIN standing AS s ON s.id = sr.standing_id
        WHERE sr.team_id IS NOT NULL
          AND s.updated_at_timestamp IS NOT NULL
          AND s.updated_at_timestamp >= EXTRACT(EPOCH FROM now())::bigint - $1::bigint
        ORDER BY sr.team_id
        LIMIT $2::bigint
        """
        rows: list[tuple[int]]
        async with self.database.connection() as connection:
            rows = await connection.fetch(sql, int(cutoff_seconds), int(self.limit))
        team_ids = tuple(int(row["team_id"]) for row in rows if row["team_id"] is not None)
        logger.info(
            "TeamOfActiveUTResolver: fetched %s active team ids (window=%sd, limit=%s)",
            len(team_ids),
            self.window_days,
            self.limit,
        )
        return team_ids

    async def _read_cache(self) -> tuple[int, ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:
            logger.warning("TeamOfActiveUTResolver: redis GET failed (fail-open): %s", exc)
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
        return tuple(int(value) for value in data if isinstance(value, (int, str)) and str(value).lstrip("-").isdigit())

    def _write_cache(self, team_ids: tuple[int, ...]) -> None:
        if self.redis_backend is None:
            return
        ttl = self.cache_ttl_seconds
        try:
            payload = json.dumps(list(team_ids), separators=(",", ":"))
            self.redis_backend.set(self.cache_key, payload, ex=ttl)
        except Exception as exc:
            logger.warning("TeamOfActiveUTResolver: redis SET failed: %s", exc)


def _positive_int(raw: object, default: int) -> int:
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


__all__ = ["TeamOfActiveUTResolver", "CACHE_KEY"]
