"""``TeamOfRegistryUTResolver`` — registry-driven scope, no standings dependency.

Stage D3 introduces a parallel scope source for endpoints currently keyed on
``team-of-active-ut`` (``/team/{id}/players``, ``/featured-players``,
``/transfers`` …). The existing standings-driven resolver only sees teams
whose ``standing_row.team_id`` was updated within the last 30 days, which
in turn requires the ``worker-historical-tournament`` archive lane to have
processed that league successfully and recently. As of this stage that
lane is single-instance and parked under permanent ``historical_enrichment``
backpressure, so leagues like Saudi Pro League (UT 955) and MLS (UT 242) —
both fully present in ``tournament_registry`` and discoverable via the
event/lineup pipelines — never enter the team-active set.

The registry-driven resolver bypasses standings entirely. It enumerates
teams seen on either side of any ``event`` whose ``start_timestamp`` falls
inside a configurable window around now (default ``[now-60d, now+60d]``)
for any UT marked active+current_enabled in ``tournament_registry``.
This covers both recently-played and upcoming matches, so future-season
fixtures and newly-promoted clubs become visible the moment their first
event is ingested.

Pilot mode (D3 first cut)
-------------------------

When ``SCHEMA_INSPECTOR_RESOURCE_REGISTRY_PILOT_UTS`` is set to a
comma-separated list of UT ids, the resolver yields ONLY teams from those
UTs. This is the safe rollout path: we activate this resolver for ~10
hand-picked problem UTs (Saudi/MLS/Scottish Prem/qualifiers/cups), watch
the resulting refresh load and 4xx rate, then unset the env to expand to
the full registry-driven scope.

Configuration::

    SCHEMA_INSPECTOR_RESOURCE_REGISTRY_PILOT_UTS=955,242,152,36,47,11,13,14,16,21
    SCHEMA_INSPECTOR_RESOURCE_REGISTRY_WINDOW_DAYS_PAST=60
    SCHEMA_INSPECTOR_RESOURCE_REGISTRY_WINDOW_DAYS_FUTURE=60
    SCHEMA_INSPECTOR_RESOURCE_REGISTRY_LIMIT=100000
    SCHEMA_INSPECTOR_RESOURCE_REGISTRY_CACHE_TTL_SECONDS=1800

This resolver is registered side-by-side with ``TeamOfActiveUTResolver``
under a distinct ``kind = "team-of-registry-ut"``. No existing endpoint
opts into it during D3 — the activation step is to switch
``TEAM_PLAYERS_ENDPOINT.scope_kind`` (and siblings) from
``"team-of-active-ut"`` to ``"team-of-registry-ut"`` once the pilot
metrics are healthy. That switch is intentionally a separate, isolated
commit so it can be reverted without rolling back the resolver code.
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
DEFAULT_LIMIT = 100_000
DEFAULT_CACHE_TTL_SECONDS = 1800
CACHE_KEY = "set:resource_refresh:team_registry_ut"

PILOT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_REGISTRY_PILOT_UTS"
WINDOW_PAST_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_REGISTRY_WINDOW_DAYS_PAST"
WINDOW_FUTURE_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_REGISTRY_WINDOW_DAYS_FUTURE"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_REGISTRY_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_REGISTRY_CACHE_TTL_SECONDS"


class TeamOfRegistryUTResolver:
    """Resolve teams from registry-active UTs via the event window."""

    kind = "team-of-registry-ut"

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
        past_seconds = self.window_days_past * 86_400
        future_seconds = self.window_days_future * 86_400
        pilot_uts = list(self.pilot_unique_tournament_ids())
        sql = """
        WITH window_events AS (
            SELECT e.home_team_id, e.away_team_id, t.unique_tournament_id
            FROM event e
            JOIN tournament t ON t.id = e.tournament_id
            JOIN tournament_registry tr
                ON tr.unique_tournament_id = t.unique_tournament_id
                AND tr.sport_slug = $1
                AND tr.is_active
                AND tr.current_enabled
            WHERE e.start_timestamp IS NOT NULL
              AND e.start_timestamp >= EXTRACT(EPOCH FROM now())::bigint - $2::bigint
              AND e.start_timestamp <= EXTRACT(EPOCH FROM now())::bigint + $3::bigint
              AND (
                cardinality($4::bigint[]) = 0
                OR t.unique_tournament_id = ANY($4::bigint[])
              )
        ),
        teams AS (
            SELECT home_team_id AS team_id FROM window_events WHERE home_team_id IS NOT NULL
            UNION
            SELECT away_team_id AS team_id FROM window_events WHERE away_team_id IS NOT NULL
        )
        SELECT team_id
        FROM teams
        ORDER BY team_id
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
        team_ids = tuple(int(row["team_id"]) for row in rows if row["team_id"] is not None)
        logger.info(
            "TeamOfRegistryUTResolver: %s teams (sport=%s, past=%sd, future=%sd, pilot_uts=%s, limit=%s)",
            len(team_ids),
            self.sport_slug,
            self.window_days_past,
            self.window_days_future,
            len(pilot_uts) if pilot_uts else "all",
            self.limit,
        )
        return team_ids

    async def _read_cache(self) -> tuple[int, ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:
            logger.warning("TeamOfRegistryUTResolver: redis GET failed (fail-open): %s", exc)
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
        except Exception as exc:
            logger.warning("TeamOfRegistryUTResolver: redis SET failed: %s", exc)


def _positive_int(raw: object, default: int) -> int:
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _parse_pilot_ut_list(raw: str) -> tuple[int, ...]:
    """Parse ``ut1,ut2,ut3`` env list. Drops malformed/non-positive items."""

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
            logger.warning("TeamOfRegistryUTResolver: ignoring non-integer pilot id %r", token)
            continue
        if value <= 0:
            continue
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return tuple(out)


__all__ = [
    "TeamOfRegistryUTResolver",
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
