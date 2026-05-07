"""``PeriodOfManagedPairsResolver`` — D12 team-of-the-week period fan-out.

Reads ``period`` table (parsed by leaderboards parser from the
``/team-of-the-week/periods`` snapshot) for each managed (ut, season)
pair, yields one ResourceTarget per period_id so
``UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT``
('/team-of-the-week/{period_id}') can be fetched.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

from .base import ResourceTarget
from .managed_football_pairs import load_managed_pairs

logger = logging.getLogger(__name__)

DEFAULT_CACHE_TTL_SECONDS = 1800
CACHE_KEY = "set:resource_refresh:period_managed_pairs"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_PERIOD_MANAGED_CACHE_TTL_SECONDS"


class PeriodOfManagedPairsResolver:
    """Fan-out (ut, season, period_id) triples for managed football leagues."""

    kind = "period-of-managed-football-pairs"

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
    def cache_ttl_seconds(self) -> int:
        raw = self._env.get(CACHE_TTL_ENV_KEY)
        if raw in (None, ""):
            return DEFAULT_CACHE_TTL_SECONDS
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return DEFAULT_CACHE_TTL_SECONDS
        return value if value > 0 else DEFAULT_CACHE_TTL_SECONDS

    def managed_pairs(self) -> tuple[tuple[int, int], ...]:
        return load_managed_pairs(self._env)

    async def resolve(self) -> Iterable[ResourceTarget]:
        pairs = self.managed_pairs()
        if not pairs:
            return ()
        triples = await self._read_cache()
        if triples is None:
            triples = await self._query_database(pairs)
            self._write_cache(triples)
        return tuple(
            ResourceTarget(
                entity_type="period",
                entity_id=int(period_id),
                path_params={
                    "unique_tournament_id": int(ut),
                    "season_id": int(season),
                    "period_id": int(period_id),
                },
                context_unique_tournament_id=int(ut),
                context_season_id=int(season),
                sport_slug="football",
            )
            for (ut, season, period_id) in triples
        )

    # ------------------------------------------------------------------

    async def _query_database(
        self, pairs: tuple[tuple[int, int], ...]
    ) -> tuple[tuple[int, int, int], ...]:
        ut_arr = [p[0] for p in pairs]
        season_arr = [p[1] for p in pairs]
        sql = """
        WITH managed AS (
            SELECT unnest($1::bigint[]) AS ut, unnest($2::bigint[]) AS season
        )
        SELECT DISTINCT m.ut, m.season, p.id AS period_id
        FROM period p
        JOIN managed m
          ON m.ut = p.unique_tournament_id
         AND m.season = p.season_id
        ORDER BY m.ut, m.season, p.id
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(sql, ut_arr, season_arr)
        triples = tuple(
            (int(r["ut"]), int(r["season"]), int(r["period_id"])) for r in rows
        )
        logger.info(
            "PeriodOfManagedPairsResolver: %s periods for %s managed pairs",
            len(triples), len(pairs),
        )
        return triples

    async def _read_cache(self) -> tuple[tuple[int, int, int], ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:
            logger.warning(
                "PeriodOfManagedPairsResolver: redis GET failed (fail-open): %s", exc
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
            payload = json.dumps([list(t) for t in triples], separators=(",", ":"))
            self.redis_backend.set(self.cache_key, payload, ex=self.cache_ttl_seconds)
        except Exception as exc:
            logger.warning("PeriodOfManagedPairsResolver: redis SET failed: %s", exc)


__all__ = ["PeriodOfManagedPairsResolver", "CACHE_KEY", "DEFAULT_CACHE_TTL_SECONDS"]
