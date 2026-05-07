"""``TeamOfActiveUTSeasonResolver`` — (team, ut, season) triples for season-aware endpoints.

Stage D2 scope: every distinct ``(team_id, unique_tournament_id, season_id)``
triple seen in standings updated within the configured window. This is the
addressing for endpoints whose path template carries all three placeholders,
e.g. ``/api/v1/team/{team_id}/unique-tournament/{ut_id}/season/{season_id}/goal-distributions``.

Why a separate resolver from ``TeamOfActiveUTResolver``: that one yields
``team_id`` only, which is enough for ``/team/{id}/players``-style endpoints
but not for season-scoped aggregates. Splitting the resolver also keeps the
Redis cache key independent so the scope sizes are observable separately.

Configuration::

    SCHEMA_INSPECTOR_RESOURCE_TEAM_SEASON_ACTIVE_WINDOW_DAYS=30
    SCHEMA_INSPECTOR_RESOURCE_TEAM_SEASON_ACTIVE_LIMIT=20000
    SCHEMA_INSPECTOR_RESOURCE_TEAM_SEASON_ACTIVE_CACHE_TTL_SECONDS=1800
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

from .base import ResourceTarget

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_DAYS = 30
DEFAULT_LIMIT = 20_000
DEFAULT_CACHE_TTL_SECONDS = 1800
CACHE_KEY = "set:resource_refresh:team_season_active"

WINDOW_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_TEAM_SEASON_ACTIVE_WINDOW_DAYS"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_TEAM_SEASON_ACTIVE_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_TEAM_SEASON_ACTIVE_CACHE_TTL_SECONDS"


class TeamOfActiveUTSeasonResolver:
    """Resolve (team, ut, season) triples from recent standings, cached in Redis."""

    kind = "team-of-active-ut-season"

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
        triples = await self._read_cache()
        if triples is None:
            triples = await self._query_database()
            self._write_cache(triples)
        return tuple(
            ResourceTarget(
                entity_type="team",
                entity_id=int(team_id),
                path_params={
                    "team_id": int(team_id),
                    "unique_tournament_id": int(ut_id),
                    "season_id": int(season_id),
                },
                context_unique_tournament_id=int(ut_id),
                context_season_id=int(season_id),
            )
            for (team_id, ut_id, season_id) in triples
        )

    # ------------------------------------------------------------------

    async def _query_database(self) -> tuple[tuple[int, int, int], ...]:
        cutoff_seconds = self.window_days * 86_400
        sql = """
        SELECT DISTINCT sr.team_id, t.unique_tournament_id, s.season_id
        FROM standing s
        JOIN standing_row sr ON sr.standing_id = s.id
        JOIN tournament t ON t.id = s.tournament_id
        WHERE sr.team_id IS NOT NULL
          AND t.unique_tournament_id IS NOT NULL
          AND s.season_id IS NOT NULL
          AND s.updated_at_timestamp IS NOT NULL
          AND s.updated_at_timestamp >= EXTRACT(EPOCH FROM now())::bigint - $1::bigint
        ORDER BY sr.team_id, t.unique_tournament_id, s.season_id
        LIMIT $2::bigint
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(sql, int(cutoff_seconds), int(self.limit))
        triples = tuple(
            (int(row["team_id"]), int(row["unique_tournament_id"]), int(row["season_id"]))
            for row in rows
            if row["team_id"] is not None
            and row["unique_tournament_id"] is not None
            and row["season_id"] is not None
        )
        logger.info(
            "TeamOfActiveUTSeasonResolver: %s triples (window=%sd, limit=%s)",
            len(triples),
            self.window_days,
            self.limit,
        )
        return triples

    async def _read_cache(self) -> tuple[tuple[int, int, int], ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:
            logger.warning("TeamOfActiveUTSeasonResolver: redis GET failed (fail-open): %s", exc)
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
        out: list[tuple[int, int, int]] = []
        for item in data:
            if (
                isinstance(item, list)
                and len(item) == 3
                and all(isinstance(x, (int, str)) and str(x).lstrip("-").isdigit() for x in item)
            ):
                out.append((int(item[0]), int(item[1]), int(item[2])))
        return tuple(out)

    def _write_cache(self, triples: tuple[tuple[int, int, int], ...]) -> None:
        if self.redis_backend is None:
            return
        try:
            payload = json.dumps([list(triple) for triple in triples], separators=(",", ":"))
            self.redis_backend.set(self.cache_key, payload, ex=self.cache_ttl_seconds)
        except Exception as exc:
            logger.warning("TeamOfActiveUTSeasonResolver: redis SET failed: %s", exc)


def _positive_int(raw: object, default: int) -> int:
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


__all__ = ["TeamOfActiveUTSeasonResolver", "CACHE_KEY"]
