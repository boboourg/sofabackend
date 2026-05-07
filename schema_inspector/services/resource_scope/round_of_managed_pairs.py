"""``RoundOfManagedPairsResolver`` — D12 parent→child rounds fan-out.

Reads the parsed ``/api/v1/unique-tournament/{ut}/season/{s}/rounds``
snapshot for each managed (ut, season) pair, extracts ``rounds[*].round``
numbers from the JSON payload (bypasses the empty ``season_round``
normalised table), and yields one ResourceTarget per round so the
``UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT`` ('/events/round/{round_number}')
can be fetched.

Why read from the JSON snapshot instead of the ``season_round`` table:
that table is currently 0 rows on prod (competition_parser path that
should populate it does not run for these endpoints in the active
ingest topology). Reading from ``api_payload_snapshot.payload->'rounds'``
gives us the same data without depending on that parser's correctness.
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
CACHE_KEY = "set:resource_refresh:round_managed_pairs"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_ROUND_MANAGED_CACHE_TTL_SECONDS"


class RoundOfManagedPairsResolver:
    """Fan-out (ut, season, round_number) triples for managed football leagues."""

    kind = "round-of-managed-football-pairs"

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
                entity_type="season",
                entity_id=int(season),
                path_params={
                    "unique_tournament_id": int(ut),
                    "season_id": int(season),
                    "round_number": int(round_number),
                },
                context_unique_tournament_id=int(ut),
                context_season_id=int(season),
                sport_slug="football",
            )
            for (ut, season, round_number) in triples
        )

    # ------------------------------------------------------------------

    async def _query_database(
        self, pairs: tuple[tuple[int, int], ...]
    ) -> tuple[tuple[int, int, int], ...]:
        # Build VALUES list for managed pairs join
        ut_arr = [p[0] for p in pairs]
        season_arr = [p[1] for p in pairs]
        sql = """
        WITH managed AS (
            SELECT unnest($1::bigint[]) AS ut, unnest($2::bigint[]) AS season
        ),
        rounds_payload AS (
            SELECT m.ut, m.season,
                   (jsonb_array_elements(s.payload->'rounds')->>'round')::int AS round_number
            FROM managed m
            JOIN api_payload_snapshot s
              ON s.endpoint_pattern = '/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/rounds'
             AND s.context_entity_type = 'season'
             AND s.context_entity_id = m.season
             AND s.context_unique_tournament_id = m.ut
             AND coalesce(s.is_soft_error_payload, false) = false
             AND coalesce(s.http_status, 200) < 400
            WHERE jsonb_typeof(s.payload->'rounds') = 'array'
        )
        SELECT DISTINCT ut, season, round_number
        FROM rounds_payload
        WHERE round_number IS NOT NULL
        ORDER BY ut, season, round_number
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(sql, ut_arr, season_arr)
        triples = tuple(
            (int(r["ut"]), int(r["season"]), int(r["round_number"])) for r in rows
        )
        logger.info(
            "RoundOfManagedPairsResolver: %s (ut, season, round) triples for %s pairs",
            len(triples),
            len(pairs),
        )
        return triples

    async def _read_cache(self) -> tuple[tuple[int, int, int], ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:
            logger.warning("RoundOfManagedPairsResolver: redis GET failed (fail-open): %s", exc)
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
            logger.warning("RoundOfManagedPairsResolver: redis SET failed: %s", exc)


__all__ = ["RoundOfManagedPairsResolver", "CACHE_KEY", "DEFAULT_CACHE_TTL_SECONDS"]
