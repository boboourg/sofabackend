"""``PeriodOfRegistryFootballResolver`` — D13.2 globalised TotW period fan-out.

Replaces the env-gated D12 :class:`PeriodOfManagedPairsResolver`. Reads the
``period`` table — populated by the leaderboards parser from the
``/team-of-the-week/periods`` snapshots — for every football tournament
marked active+current_enabled in ``tournament_registry`` whose event
window overlaps ``[now-60d, now+60d]``. Yields one ``ResourceTarget`` per
period_id so ``UNIQUE_TOURNAMENT_TEAM_OF_THE_WEEK_ENDPOINT``
('/team-of-the-week/{period_id}') can be fetched.

Smart 404 by season (D13.3 hook)
--------------------------------

Probes against Sofascore upstream show that only ~5% of football seasons
support TotW (178 / 3,314). Without filtering, scope generation would
hammer thousands of (ut, season) pairs that always return 404 against
``/team-of-the-week/periods``. The ``ToTW404Store`` interface (passed via
``totw_404_store``) keeps a Redis hash of seasons proven not to support
TotW; the resolver consults it before yielding to skip blacklisted
seasons. The store is populated by the resource-refresh worker on first
404 (see D13.3 worker hook).

Until that store is wired the resolver still works — it simply yields
all available periods without filtering — so D13.2 and D13.3 can land in
separate steps.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

from .base import ResourceTarget

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_DAYS_PAST = 60
DEFAULT_WINDOW_DAYS_FUTURE = 60
DEFAULT_LIMIT = 50_000
DEFAULT_CACHE_TTL_SECONDS = 1800
CACHE_KEY = "set:resource_refresh:period_registry_football"

PILOT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_PERIOD_REGISTRY_PILOT_UTS"
WINDOW_PAST_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_PERIOD_REGISTRY_WINDOW_DAYS_PAST"
WINDOW_FUTURE_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_PERIOD_REGISTRY_WINDOW_DAYS_FUTURE"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_PERIOD_REGISTRY_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_PERIOD_REGISTRY_CACHE_TTL_SECONDS"


class PeriodOfRegistryFootballResolver:
    """Yield (ut, season, period_id) triples for registry-active football leagues."""

    kind = "period-of-registry-football"

    def __init__(
        self,
        *,
        database: Any,
        redis_backend: Any | None = None,
        env: dict[str, str] | None = None,
        cache_key: str = CACHE_KEY,
        sport_slug: str = "football",
        totw_404_store: Any | None = None,
    ) -> None:
        self.database = database
        self.redis_backend = redis_backend
        self.cache_key = cache_key
        self.sport_slug = sport_slug
        self.totw_404_store = totw_404_store
        self._env = env if env is not None else dict(os.environ)

    @property
    def window_days_past(self) -> int:
        return _positive_int(self._env.get(WINDOW_PAST_ENV_KEY), DEFAULT_WINDOW_DAYS_PAST)

    @property
    def window_days_future(self) -> int:
        return _positive_int(self._env.get(WINDOW_FUTURE_ENV_KEY), DEFAULT_WINDOW_DAYS_FUTURE)

    @property
    def limit(self) -> int:
        return _positive_int(self._env.get(LIMIT_ENV_KEY), DEFAULT_LIMIT)

    @property
    def cache_ttl_seconds(self) -> int:
        return _positive_int(self._env.get(CACHE_TTL_ENV_KEY), DEFAULT_CACHE_TTL_SECONDS)

    def pilot_unique_tournament_ids(self) -> tuple[int, ...]:
        return _parse_pilot_ut_list(self._env.get(PILOT_ENV_KEY) or "")

    async def resolve(self) -> Iterable[ResourceTarget]:
        triples = await self._read_cache()
        if triples is None:
            triples = await self._query_database()
            self._write_cache(triples)
        # Smart 404 (D13.3): drop triples whose season is known not to
        # support TotW. We filter post-cache so adding/removing entries
        # from the blacklist takes effect on the next planner tick
        # without needing to invalidate the SQL-cache.
        if self.totw_404_store is not None:
            triples = tuple(
                triple
                for triple in triples
                if not _is_blacklisted(self.totw_404_store, triple[0], triple[1])
            )
        return tuple(
            ResourceTarget(
                entity_type="period",
                entity_id=int(period_id),
                path_params={
                    "unique_tournament_id": int(ut_id),
                    "season_id": int(season_id),
                    "period_id": int(period_id),
                },
                context_unique_tournament_id=int(ut_id),
                context_season_id=int(season_id),
                sport_slug=self.sport_slug,
            )
            for (ut_id, season_id, period_id) in triples
        )

    # ------------------------------------------------------------------

    async def _query_database(self) -> tuple[tuple[int, int, int], ...]:
        past_seconds = self.window_days_past * 86_400
        future_seconds = self.window_days_future * 86_400
        pilot_uts = list(self.pilot_unique_tournament_ids())
        sql = """
        WITH base_pairs AS (
            SELECT DISTINCT t.unique_tournament_id AS ut, e.season_id AS season
            FROM event e
            JOIN tournament t ON t.id = e.tournament_id
            JOIN tournament_registry tr
                ON tr.unique_tournament_id = t.unique_tournament_id
                AND tr.sport_slug = $1
                AND tr.is_active
                AND tr.current_enabled
            WHERE t.unique_tournament_id IS NOT NULL
              AND e.season_id IS NOT NULL
              AND e.start_timestamp IS NOT NULL
              AND e.start_timestamp >= EXTRACT(EPOCH FROM now())::bigint - $2::bigint
              AND e.start_timestamp <= EXTRACT(EPOCH FROM now())::bigint + $3::bigint
              AND (
                cardinality($4::bigint[]) = 0
                OR t.unique_tournament_id = ANY($4::bigint[])
              )
        )
        SELECT DISTINCT bp.ut, bp.season, p.id AS period_id
        FROM period p
        JOIN base_pairs bp
          ON bp.ut = p.unique_tournament_id
         AND bp.season = p.season_id
        ORDER BY bp.ut, bp.season, p.id
        LIMIT $5::bigint
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(
                sql,
                str(self.sport_slug),
                int(past_seconds),
                int(future_seconds),
                pilot_uts,
                int(self.limit),
            )
        triples = tuple(
            (int(r["ut"]), int(r["season"]), int(r["period_id"])) for r in rows
        )
        logger.info(
            "PeriodOfRegistryFootballResolver: %s periods (sport=%s, past=%sd, future=%sd, pilot_uts=%s, limit=%s)",
            len(triples),
            self.sport_slug,
            self.window_days_past,
            self.window_days_future,
            len(pilot_uts) if pilot_uts else "all",
            self.limit,
        )
        return triples

    async def _read_cache(self) -> tuple[tuple[int, int, int], ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:
            logger.warning("PeriodOfRegistryFootballResolver: redis GET failed (fail-open): %s", exc)
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
            logger.warning("PeriodOfRegistryFootballResolver: redis SET failed: %s", exc)


def _is_blacklisted(store: Any, ut_id: int, season_id: int) -> bool:
    """Return True iff the season is marked as not-supporting TotW.

    The store is duck-typed: any object with an ``is_blacklisted(ut_id,
    season_id) -> bool`` method works. Errors fail-open (do not blacklist).
    """

    try:
        return bool(store.is_blacklisted(int(ut_id), int(season_id)))
    except Exception as exc:
        logger.warning(
            "PeriodOfRegistryFootballResolver: ToTW 404 store is_blacklisted failed (fail-open): %s",
            exc,
        )
        return False


def _positive_int(raw: object, default: int) -> int:
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _parse_pilot_ut_list(raw: str) -> tuple[int, ...]:
    if not raw:
        return ()
    seen: set[int] = set()
    out: list[int] = []
    for chunk in str(raw).split(","):
        token = chunk.strip()
        if not token:
            continue
        try:
            value = int(token)
        except ValueError:
            logger.warning("PeriodOfRegistryFootballResolver: ignoring non-integer pilot id %r", token)
            continue
        if value <= 0 or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return tuple(out)


__all__ = [
    "PeriodOfRegistryFootballResolver",
    "CACHE_KEY",
    "PILOT_ENV_KEY",
    "WINDOW_PAST_ENV_KEY",
    "WINDOW_FUTURE_ENV_KEY",
    "LIMIT_ENV_KEY",
    "CACHE_TTL_ENV_KEY",
    "DEFAULT_WINDOW_DAYS_PAST",
    "DEFAULT_WINDOW_DAYS_FUTURE",
    "DEFAULT_LIMIT",
    "DEFAULT_CACHE_TTL_SECONDS",
]
