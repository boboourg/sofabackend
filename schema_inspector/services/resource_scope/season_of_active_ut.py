"""``SeasonOfActiveUTResolver`` family — resolve (unique_tournament, season) pairs.

Stage 4 wires standings + season events list/next under a single conceptual
scope: "the current season of every league we care about". Three concrete
resolver subclasses sit on top of one shared base so every endpoint that
consumes this scope hits the same Redis cache + database query per tick:

* ``SeasonOfActiveUTBaseResolver`` — internal. Yields raw ``(ut, season)``
  pairs either from a pilot env list or from SQL. Not registered as a
  scope_kind by itself; production resolvers wrap it.
* ``SeasonOfActiveUTEventsResolver``  (kind=``season-of-active-ut-events``):
  one ResourceTarget per ``(ut, season)``, ``path_params={ut, season, page=0}``.
  Used by both ``/season/{s}/events/last/{page}`` and ``.../events/next/{page}``.
* ``SeasonOfActiveUTStandingsResolver`` (kind=``season-of-active-ut-standings``):
  fans out ×3 standings scopes (``total``, ``home``, ``away``) per pair, so a
  single endpoint with the ``{scope}`` placeholder covers all three buckets
  via the per-target cursor.

Pilot mode (Stage 4 first cut)
------------------------------

When ``SCHEMA_INSPECTOR_RESOURCE_PILOT_SEASONS`` is set, the base resolver
yields ONLY the configured pairs and skips the SQL query entirely. This is
the safe rollout path: we activate the four endpoints for 3-10 hand-picked
``(ut, season)`` pairs, watch metrics, then unset the env to expand to the
SQL-driven scope.

Env format::

    SCHEMA_INSPECTOR_RESOURCE_PILOT_SEASONS=8:77559,17:76986,7:76953

SQL mode (post-pilot)
---------------------

Uses ``standing.updated_at_timestamp`` (last 30d window, env-tunable) as the
"active" signal, exactly as ``TeamOfActiveUTResolver`` does for teams. The
join goes ``standing -> tournament -> unique_tournament`` and selects
``DISTINCT (unique_tournament_id, season_id)``. Cached in Redis for 30 min.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

from .base import ResourceTarget

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_DAYS = 30
DEFAULT_LIMIT = 5_000
DEFAULT_CACHE_TTL_SECONDS = 1800
CACHE_KEY = "set:resource_refresh:season_active_ut"

PILOT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_PILOT_SEASONS"
WINDOW_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_ACTIVE_WINDOW_DAYS"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_ACTIVE_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_ACTIVE_CACHE_TTL_SECONDS"

STANDINGS_SCOPES: tuple[str, ...] = ("total", "home", "away")


class SeasonOfActiveUTBaseResolver:
    """Yield ``(ut_id, season_id)`` pairs from pilot env OR SQL+Redis cache.

    This class is intentionally NOT a public ScopeResolver -- it has no
    ``kind`` attribute. The two production resolvers below wrap it.
    """

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

    def pilot_pairs(self) -> tuple[tuple[int, int], ...]:
        raw = self._env.get(PILOT_ENV_KEY) or ""
        return _parse_pilot_pairs(raw)

    async def pairs(self) -> tuple[tuple[int, int], ...]:
        pilot = self.pilot_pairs()
        if pilot:
            return pilot
        cached = await self._read_cache()
        if cached is not None:
            return cached
        fetched = await self._query_database()
        self._write_cache(fetched)
        return fetched

    # ------------------------------------------------------------------

    async def _query_database(self) -> tuple[tuple[int, int], ...]:
        cutoff_seconds = self.window_days * 86_400
        sql = """
        SELECT DISTINCT t.unique_tournament_id AS ut_id, s.season_id AS season_id
        FROM standing s
        JOIN tournament t ON t.id = s.tournament_id
        WHERE t.unique_tournament_id IS NOT NULL
          AND s.season_id IS NOT NULL
          AND s.updated_at_timestamp IS NOT NULL
          AND s.updated_at_timestamp >= EXTRACT(EPOCH FROM now())::bigint - $1::bigint
        ORDER BY t.unique_tournament_id, s.season_id
        LIMIT $2::bigint
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(sql, int(cutoff_seconds), int(self.limit))
        pairs = tuple(
            (int(row["ut_id"]), int(row["season_id"]))
            for row in rows
            if row["ut_id"] is not None and row["season_id"] is not None
        )
        logger.info(
            "SeasonOfActiveUTBaseResolver: %s pairs (window=%sd, limit=%s)",
            len(pairs),
            self.window_days,
            self.limit,
        )
        return pairs

    async def _read_cache(self) -> tuple[tuple[int, int], ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:
            logger.warning(
                "SeasonOfActiveUTBaseResolver: redis GET failed (fail-open): %s", exc
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
            payload = json.dumps([list(pair) for pair in pairs], separators=(",", ":"))
            self.redis_backend.set(self.cache_key, payload, ex=self.cache_ttl_seconds)
        except Exception as exc:
            logger.warning("SeasonOfActiveUTBaseResolver: redis SET failed: %s", exc)


class SeasonOfActiveUTEventsResolver:
    """``season-of-active-ut-events``: one target per pair with ``page=0``."""

    kind = "season-of-active-ut-events"

    def __init__(self, *, base: SeasonOfActiveUTBaseResolver) -> None:
        self.base = base

    async def resolve(self) -> Iterable[ResourceTarget]:
        pairs = await self.base.pairs()
        return tuple(
            ResourceTarget(
                entity_type="season",
                entity_id=int(season_id),
                path_params={
                    "unique_tournament_id": int(ut_id),
                    "season_id": int(season_id),
                    "page": 0,
                },
                context_unique_tournament_id=int(ut_id),
                context_season_id=int(season_id),
            )
            for (ut_id, season_id) in pairs
        )


class SeasonOfActiveUTStandingsResolver:
    """``season-of-active-ut-standings``: one target per pair × scope ∈ {total,home,away}."""

    kind = "season-of-active-ut-standings"

    def __init__(
        self,
        *,
        base: SeasonOfActiveUTBaseResolver,
        scopes: tuple[str, ...] = STANDINGS_SCOPES,
    ) -> None:
        self.base = base
        self.scopes = tuple(scopes)

    async def resolve(self) -> Iterable[ResourceTarget]:
        pairs = await self.base.pairs()
        return tuple(
            ResourceTarget(
                entity_type="season",
                entity_id=int(season_id),
                path_params={
                    "unique_tournament_id": int(ut_id),
                    "season_id": int(season_id),
                    "scope": scope,
                },
                context_unique_tournament_id=int(ut_id),
                context_season_id=int(season_id),
            )
            for (ut_id, season_id) in pairs
            for scope in self.scopes
        )


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _parse_pilot_pairs(raw: str) -> tuple[tuple[int, int], ...]:
    """Parse ``ut1:season1,ut2:season2,...`` env list. Drops malformed items."""

    if not raw:
        return ()
    out: list[tuple[int, int]] = []
    seen: set[tuple[int, int]] = set()
    for chunk in str(raw).split(","):
        token = chunk.strip()
        if not token:
            continue
        if ":" not in token:
            logger.warning("SeasonOfActiveUT pilot: ignoring %r (missing ':')", token)
            continue
        left, right = token.split(":", 1)
        try:
            ut_id = int(left.strip())
            season_id = int(right.strip())
        except ValueError:
            logger.warning("SeasonOfActiveUT pilot: ignoring non-integer pair %r", token)
            continue
        if ut_id <= 0 or season_id <= 0:
            continue
        pair = (ut_id, season_id)
        if pair in seen:
            continue
        seen.add(pair)
        out.append(pair)
    return tuple(out)


def _positive_int(raw: object, default: int) -> int:
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


__all__ = [
    "SeasonOfActiveUTBaseResolver",
    "SeasonOfActiveUTEventsResolver",
    "SeasonOfActiveUTStandingsResolver",
    "STANDINGS_SCOPES",
    "CACHE_KEY",
    "PILOT_ENV_KEY",
]
