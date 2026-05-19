"""``SeasonOfRegistryUTRoundsHistoricalResolver`` — (UT, season) pairs
needing ``/season/{s}/rounds`` ingestion, scoped to registered active
+ historical_enabled UTs whose ``season_round`` is still empty.

Item 1 (UCL strategy C, 2026-05-19): decouples the round catalog
fetch from the cursor walk. The Phase 4 orchestrator routes per-round
fetches against ``season_round``, but for historical cup-style
seasons (UCL 24/25, EURO 2020, FIFA WC 2018, ...) the catalog is
empty because the cursor never reached them (strict cat-priority
barrier holds cat>=19 behind cat=20 indefinitely).

This resolver opens a side channel: ANY (UT, season) pair from
``tournament_registry`` (is_active + historical_enabled) where
``season_round`` has 0 rows gets a ``/rounds`` fetch scheduled. Once
the catalog lands, the ``NOT EXISTS`` filter drops the pair from
subsequent ticks — no re-fetch, no refresh cadence pressure.

Why not just expand the existing ±60d scope
-------------------------------------------
The existing ``SeasonOfRegistryUTResolver`` is shared between
``/cuptrees`` and (used to be) ``/rounds``. Broadening its window
would also pull historical seasons into ``/cuptrees`` traffic, where
404 rates are high (only multi-leg knockout cups have a tree). Item 4
will handle cuptrees historical coverage with its own targeted scope.

Window
------
No date filter. Historical seasons reach this scope based on
registry membership alone. Pilot mode (``PILOT_ENV_KEY``) lets us
restrict to a hand-picked UT list during rollout.

Configuration
-------------
``SCHEMA_INSPECTOR_RESOURCE_SEASON_ROUNDS_HISTORICAL_PILOT_UTS``
    Optional pilot list — comma-separated UT ids. When set, only
    these UTs are considered (the resolver returns at most their
    rounds-missing seasons).

``SCHEMA_INSPECTOR_RESOURCE_SEASON_ROUNDS_HISTORICAL_LIMIT``
    Hard cap on returned pairs per tick. Default 5000 — large enough
    for an initial historical sweep, small enough that a single tick
    doesn't overflow the per-tick publish budget.

``SCHEMA_INSPECTOR_RESOURCE_SEASON_ROUNDS_HISTORICAL_CACHE_TTL_SECONDS``
    Redis cache TTL for the resolved pair list. Default 1800
    (30 min) — mirrors the existing scope's TTL since the underlying
    join is similar.
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
CACHE_KEY = "set:resource_refresh:season_rounds_historical"

PILOT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_ROUNDS_HISTORICAL_PILOT_UTS"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_ROUNDS_HISTORICAL_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_ROUNDS_HISTORICAL_CACHE_TTL_SECONDS"


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
                "SeasonOfRegistryUTRoundsHistoricalResolver: ignoring pilot UT id %r — not an int",
                piece,
            )
    return tuple(out)


class SeasonOfRegistryUTRoundsHistoricalResolver:
    """Yield (UT, season) targets for the historical ``/rounds`` sweep."""

    kind = "season-of-registry-ut-rounds-historical"

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
              SELECT 1 FROM season_round sr
              WHERE sr.unique_tournament_id = t.unique_tournament_id
                AND sr.season_id = e.season_id
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
            "SeasonOfRegistryUTRoundsHistoricalResolver: %s pairs (sport=%s, pilot_uts=%s, limit=%s)",
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
                "SeasonOfRegistryUTRoundsHistoricalResolver: redis GET failed (fail-open): %s",
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
                "SeasonOfRegistryUTRoundsHistoricalResolver: redis SET failed (fail-open): %s",
                exc,
            )


__all__ = [
    "SeasonOfRegistryUTRoundsHistoricalResolver",
    "PILOT_ENV_KEY",
    "LIMIT_ENV_KEY",
    "CACHE_TTL_ENV_KEY",
    "CACHE_KEY",
]
