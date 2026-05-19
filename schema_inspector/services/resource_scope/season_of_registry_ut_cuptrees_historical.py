"""``SeasonOfRegistryUTCuptreesHistoricalResolver`` — (UT, season)
pairs needing ``/season/{s}/cuptrees`` ingestion.

Item 4 (2026-05-19): parallels Item 1's rounds-historical scope but
for the bracket-tree (cuptrees) endpoint. The legacy
``season-of-registry-ut`` scope ridden by ``/cuptrees`` is bounded to
a ±60-day window — historical cup-style competitions
(FIFA WC 2018-2022, UCL knockout history, EURO 2020, Copa America,
domestic cups like FA Cup / DFB-Pokal previous seasons) never had
their bracket payload fetched.

Mechanics
---------
Resolver yields (UT, season) pairs where:
* ``tournament_registry`` has the UT marked active + historical_enabled
* ``season_cup_tree`` has 0 rows for the (UT, season)

After the first successful fetch (or absorbed 404 for league
tournaments via the negative cache), the pair drops out of the
scope. No re-fetch cadence — for finished cups the bracket structure
is immutable.

Why not piggyback on Item 1's rounds scope
------------------------------------------
Independent control: Item 1's traffic profile is light (rounds
payload is small, schema is uniform across sports). Item 4 has a
heavier traffic profile (cuptrees only returns 200 for actual cup
formats; league UTs respond 404 which the negative cache absorbs at
one wasted request per UT per 7 days). Keeping the scopes separate
lets us pilot Item 4 to a smaller UT list first.

Config (all optional):
  SCHEMA_INSPECTOR_RESOURCE_SEASON_CUPTREES_HISTORICAL_PILOT_UTS
  SCHEMA_INSPECTOR_RESOURCE_SEASON_CUPTREES_HISTORICAL_LIMIT (5000)
  SCHEMA_INSPECTOR_RESOURCE_SEASON_CUPTREES_HISTORICAL_CACHE_TTL_SECONDS (1800)
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

from .base import ResourceTarget

logger = logging.getLogger(__name__)


DEFAULT_LIMIT = 5_000
DEFAULT_CACHE_TTL_SECONDS = 1800
CACHE_KEY = "set:resource_refresh:season_cuptrees_historical"

PILOT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_CUPTREES_HISTORICAL_PILOT_UTS"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_CUPTREES_HISTORICAL_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_CUPTREES_HISTORICAL_CACHE_TTL_SECONDS"


def _positive_int(raw: str | None, default: int) -> int:
    if raw is None:
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _parse_pilot_ut_list(raw: str) -> tuple[int, ...]:
    pieces = [piece.strip() for piece in raw.split(",") if piece.strip()]
    out: list[int] = []
    for piece in pieces:
        try:
            out.append(int(piece))
        except ValueError:
            logger.warning(
                "SeasonOfRegistryUTCuptreesHistoricalResolver: ignoring pilot UT id %r — not an int",
                piece,
            )
    return tuple(out)


class SeasonOfRegistryUTCuptreesHistoricalResolver:
    """Yield (UT, season) targets for the historical ``/cuptrees`` sweep."""

    kind = "season-of-registry-ut-cuptrees-historical"

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
    def limit(self) -> int:
        return _positive_int(self._env.get(LIMIT_ENV_KEY), DEFAULT_LIMIT)

    @property
    def cache_ttl_seconds(self) -> int:
        return _positive_int(
            self._env.get(CACHE_TTL_ENV_KEY), DEFAULT_CACHE_TTL_SECONDS
        )

    def pilot_unique_tournament_ids(self) -> tuple[int, ...]:
        return _parse_pilot_ut_list(self._env.get(PILOT_ENV_KEY) or "")

    async def resolve(self) -> Iterable[ResourceTarget]:
        pairs = await self._read_cache()
        if pairs is None:
            pairs = await self._query_database()
            self._write_cache(pairs)
        return tuple(
            ResourceTarget(
                entity_type="season",
                entity_id=int(season_id),
                path_params={
                    "unique_tournament_id": int(ut_id),
                    "season_id": int(season_id),
                },
                context_unique_tournament_id=int(ut_id),
                context_season_id=int(season_id),
                sport_slug=self.sport_slug,
            )
            for (ut_id, season_id) in pairs
        )

    # ------------------------------------------------------------------

    async def _query_database(self) -> tuple[tuple[int, int], ...]:
        pilot_uts = list(self.pilot_unique_tournament_ids())
        sql = """
        SELECT DISTINCT t.unique_tournament_id AS ut_id, e.season_id AS season_id
        FROM event e
        JOIN tournament t ON t.id = e.tournament_id
        JOIN tournament_registry tr
            ON tr.unique_tournament_id = t.unique_tournament_id
            AND tr.sport_slug = $1
            AND tr.is_active
            AND tr.historical_enabled
        WHERE t.unique_tournament_id IS NOT NULL
          AND e.season_id IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM season_cup_tree sct
              WHERE sct.unique_tournament_id = t.unique_tournament_id
                AND sct.season_id = e.season_id
          )
          AND (
            cardinality($2::bigint[]) = 0
            OR t.unique_tournament_id = ANY($2::bigint[])
          )
        ORDER BY t.unique_tournament_id, e.season_id
        LIMIT $3::bigint
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(
                sql,
                str(self.sport_slug),
                pilot_uts,
                int(self.limit),
            )
        pairs = tuple(
            (int(row["ut_id"]), int(row["season_id"]))
            for row in rows
            if row["ut_id"] is not None and row["season_id"] is not None
        )
        logger.info(
            "SeasonOfRegistryUTCuptreesHistoricalResolver: %s pairs (sport=%s, pilot_uts=%s, limit=%s)",
            len(pairs),
            self.sport_slug,
            len(pilot_uts) if pilot_uts else "all",
            self.limit,
        )
        return pairs

    async def _read_cache(self) -> tuple[tuple[int, int], ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "SeasonOfRegistryUTCuptreesHistoricalResolver: redis GET failed: %s",
                exc,
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
        out: list[tuple[int, int]] = []
        for item in data:
            if (
                isinstance(item, list)
                and len(item) == 2
                and all(isinstance(x, (int, str)) and str(x).lstrip("-").isdigit() for x in item)
            ):
                out.append((int(item[0]), int(item[1])))
        return tuple(out)

    def _write_cache(self, pairs: tuple[tuple[int, int], ...]) -> None:
        if self.redis_backend is None:
            return
        try:
            self.redis_backend.set(
                self.cache_key,
                json.dumps([list(pair) for pair in pairs]),
                ex=self.cache_ttl_seconds,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "SeasonOfRegistryUTCuptreesHistoricalResolver: redis SET failed: %s",
                exc,
            )


__all__ = [
    "SeasonOfRegistryUTCuptreesHistoricalResolver",
    "PILOT_ENV_KEY",
    "LIMIT_ENV_KEY",
    "CACHE_TTL_ENV_KEY",
    "CACHE_KEY",
]
