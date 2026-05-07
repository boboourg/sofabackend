"""``SeasonOfRegistryUTResolver`` — (ut, season) pairs from registry-active UTs.

Stage D5 introduces a wide (ut, season) scope source for endpoints that
need every active tournament — including knockout cups whose standings are
never ingested. The standings-driven ``SeasonOfActiveUTBaseResolver`` only
sees ``(ut, season)`` pairs whose ``standing.updated_at_timestamp`` was
refreshed in the last 30 days, which excludes cup tournaments (FA Cup,
Copa del Rey, etc.) and any league whose historical-tournament archive
worker has not processed it recently.

The cuptrees endpoint is the primary motivator: upstream returns 200 only
for tournaments that actually have a cup tree (multi-leg knockout) and
404 otherwise. Rather than maintain a separate ``has_playoff_series`` flag
(probe established the upstream field is unreliable — UCL/FA Cup/DFB
Pokal all return ``hasPlayoffSeries=false``), we lean on the existing
``ResourceNegativeCache``: leagues hit upstream 404 once, then the
negative cache suppresses re-publishing for 7 days. After the first sweep
only the actual cup tournaments contribute traffic.

Window
------

Pairs are resolved by joining ``event``-side season ids with
``tournament_registry`` (active + current_enabled) within
``[now-window_days_past, now+window_days_future]`` (default 60d/60d) — the
same surface ``TeamOfRegistryUTResolver`` uses, so the two scopes describe
the same "active tournaments right now" universe.

Configuration::

    SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_PILOT_UTS=         # optional pilot list
    SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_WINDOW_DAYS_PAST=60
    SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_WINDOW_DAYS_FUTURE=60
    SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_LIMIT=20000
    SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_CACHE_TTL_SECONDS=1800
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
DEFAULT_LIMIT = 20_000
DEFAULT_CACHE_TTL_SECONDS = 1800
CACHE_KEY = "set:resource_refresh:season_registry_ut"

PILOT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_PILOT_UTS"
WINDOW_PAST_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_WINDOW_DAYS_PAST"
WINDOW_FUTURE_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_WINDOW_DAYS_FUTURE"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_SEASON_REGISTRY_CACHE_TTL_SECONDS"


class SeasonOfRegistryUTResolver:
    """Resolve (ut, season) pairs from registry-active UTs via the event window."""

    kind = "season-of-registry-ut"

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
        past_seconds = self.window_days_past * 86_400
        future_seconds = self.window_days_future * 86_400
        pilot_uts = list(self.pilot_unique_tournament_ids())
        sql = """
        SELECT DISTINCT t.unique_tournament_id AS ut_id, e.season_id AS season_id
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
        ORDER BY t.unique_tournament_id, e.season_id
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
        pairs = tuple(
            (int(row["ut_id"]), int(row["season_id"]))
            for row in rows
            if row["ut_id"] is not None and row["season_id"] is not None
        )
        logger.info(
            "SeasonOfRegistryUTResolver: %s pairs (sport=%s, past=%sd, future=%sd, pilot_uts=%s, limit=%s)",
            len(pairs),
            self.sport_slug,
            self.window_days_past,
            self.window_days_future,
            len(pilot_uts) if pilot_uts else "all",
            self.limit,
        )
        return pairs

    async def _read_cache(self) -> tuple[tuple[int, int], ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:
            logger.warning("SeasonOfRegistryUTResolver: redis GET failed (fail-open): %s", exc)
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
            logger.warning("SeasonOfRegistryUTResolver: redis SET failed: %s", exc)


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
            logger.warning("SeasonOfRegistryUTResolver: ignoring non-integer pilot id %r", token)
            continue
        if value <= 0 or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return tuple(out)


__all__ = [
    "SeasonOfRegistryUTResolver",
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
