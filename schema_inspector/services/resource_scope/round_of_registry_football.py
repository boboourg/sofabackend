"""``RoundOfRegistryFootballResolver`` — D13.2 globalised round fan-out.

Replaces the env-gated D12 :class:`RoundOfManagedPairsResolver`. The new
resolver enumerates ``(unique_tournament_id, season_id, round_number)``
triples for every football tournament marked active+current_enabled in
``tournament_registry`` whose event window overlaps ``[now-60d, now+60d]``.

For each pair we read the ``rounds[*].round`` numbers from the cached
``/unique-tournament/{ut}/season/{season}/rounds`` snapshot and yield one
``ResourceTarget`` per round so ``UNIQUE_TOURNAMENT_ROUND_EVENTS_ENDPOINT``
('/events/round/{round_number}') can be fetched.

Finished-rounds filter
----------------------

The ``/rounds`` payload exposes ``currentRound.round`` — the round upstream
considers "in progress" right now. Fetching ``/events/round/{N}`` for
``N == currentRound`` is wasteful: the round is not yet finalized, so the
event list is identical (or nearly identical) to what the live planner
already polls. We therefore yield only ``round_number < currentRound``.
The ``currentRound`` itself is left to a separate, hourly refresh path
(planned in D13.4 backlog).

Why we read JSON, not the ``season_round`` table: until D13.1 the
``normalize-worker`` was not running on prod, so ``season_round`` was
empty. After D13.1 the table is populated, but reading from the snapshot
keeps this resolver independent of the parser-chain timing — newly
ingested ``/rounds`` payloads contribute round triples on the next
resolver tick instead of waiting for the normalize worker to drain its
backlog.
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
CACHE_KEY = "set:resource_refresh:round_registry_football"

PILOT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_ROUND_REGISTRY_PILOT_UTS"
WINDOW_PAST_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_ROUND_REGISTRY_WINDOW_DAYS_PAST"
WINDOW_FUTURE_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_ROUND_REGISTRY_WINDOW_DAYS_FUTURE"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_ROUND_REGISTRY_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_ROUND_REGISTRY_CACHE_TTL_SECONDS"

ROUNDS_ENDPOINT_PATTERN = (
    "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/rounds"
)


class RoundOfRegistryFootballResolver:
    """Yield (ut, season, round_number) triples for registry-active football leagues."""

    kind = "round-of-registry-football"

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
        triples = await self._read_cache()
        if triples is None:
            triples = await self._query_database()
            self._write_cache(triples)
        return tuple(
            ResourceTarget(
                entity_type="season",
                entity_id=int(season_id),
                path_params={
                    "unique_tournament_id": int(ut_id),
                    "season_id": int(season_id),
                    "round_number": int(round_number),
                },
                context_unique_tournament_id=int(ut_id),
                context_season_id=int(season_id),
                sport_slug=self.sport_slug,
            )
            for (ut_id, season_id, round_number) in triples
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
        ),
        rounds_payload AS (
            SELECT bp.ut, bp.season,
                   (jsonb_array_elements(s.payload->'rounds')->>'round')::int AS round_number,
                   NULLIF((s.payload->'currentRound'->>'round'), '')::int AS current_round
            FROM base_pairs bp
            JOIN api_payload_snapshot s
              ON s.endpoint_pattern = $5
             AND s.context_entity_type = 'season'
             AND s.context_entity_id = bp.season
             AND s.context_unique_tournament_id = bp.ut
             AND coalesce(s.is_soft_error_payload, false) = false
             AND coalesce(s.http_status, 200) < 400
            WHERE jsonb_typeof(s.payload->'rounds') = 'array'
        )
        SELECT DISTINCT ut, season, round_number
        FROM rounds_payload
        WHERE round_number IS NOT NULL
          AND (current_round IS NULL OR round_number < current_round)
        ORDER BY ut, season, round_number
        LIMIT $6::bigint
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(
                sql,
                str(self.sport_slug),
                int(past_seconds),
                int(future_seconds),
                pilot_uts,
                ROUNDS_ENDPOINT_PATTERN,
                int(self.limit),
            )
        triples = tuple(
            (int(r["ut"]), int(r["season"]), int(r["round_number"])) for r in rows
        )
        logger.info(
            "RoundOfRegistryFootballResolver: %s (ut, season, round) triples (sport=%s, past=%sd, future=%sd, pilot_uts=%s, limit=%s)",
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
            logger.warning("RoundOfRegistryFootballResolver: redis GET failed (fail-open): %s", exc)
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
            logger.warning("RoundOfRegistryFootballResolver: redis SET failed: %s", exc)


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
            logger.warning("RoundOfRegistryFootballResolver: ignoring non-integer pilot id %r", token)
            continue
        if value <= 0 or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return tuple(out)


__all__ = [
    "RoundOfRegistryFootballResolver",
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
    "ROUNDS_ENDPOINT_PATTERN",
]
