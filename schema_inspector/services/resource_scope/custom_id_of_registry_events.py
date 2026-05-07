"""``CustomIdOfRegistryEventsResolver`` — D13.2 globalised h2h custom_id fan-out.

Replaces the env-gated D12 :class:`CustomIdOfManagedEventsResolver`.
Reads ``event.custom_id`` for every football event whose tournament is
active+current_enabled in ``tournament_registry`` and whose
``start_timestamp`` falls inside ``[now-60d, now+60d]``. Yields one
``ResourceTarget`` per event with ``path_params={'custom_id': ...}`` so
``EVENT_H2H_EVENTS_ENDPOINT`` ('/event/{custom_id}/h2h/events') can be
fetched.

Finished-events filter
----------------------

H2H ("history of head-to-head events") is meaningful only for events
that have already concluded — for upcoming or in-progress events the
upstream payload is identical to what we already get from the live
pipeline. We therefore restrict the scope to ``event_status.type =
'finished'`` events, which keeps the per-tick custom_id volume bounded
to ~50–80% of the rolling 60-day window depending on the season phase.
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
DEFAULT_LIMIT = 500_000
DEFAULT_CACHE_TTL_SECONDS = 1800
CACHE_KEY = "set:resource_refresh:custom_id_registry_events"

PILOT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_CUSTOM_ID_REGISTRY_PILOT_UTS"
WINDOW_PAST_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_CUSTOM_ID_REGISTRY_WINDOW_DAYS_PAST"
WINDOW_FUTURE_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_CUSTOM_ID_REGISTRY_WINDOW_DAYS_FUTURE"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_CUSTOM_ID_REGISTRY_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_CUSTOM_ID_REGISTRY_CACHE_TTL_SECONDS"


class CustomIdOfRegistryEventsResolver:
    """Yield event custom_id targets for registry-active football leagues."""

    kind = "custom-id-of-registry-events"

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
        rows = await self._read_cache()
        if rows is None:
            rows = await self._query_database()
            self._write_cache(rows)
        return tuple(
            ResourceTarget(
                entity_type="event",
                entity_id=int(event_id),
                path_params={"custom_id": custom_id},
                context_event_id=int(event_id),
                sport_slug=self.sport_slug,
            )
            for (event_id, custom_id) in rows
        )

    # ------------------------------------------------------------------

    async def _query_database(self) -> tuple[tuple[int, str], ...]:
        past_seconds = self.window_days_past * 86_400
        future_seconds = self.window_days_future * 86_400
        pilot_uts = list(self.pilot_unique_tournament_ids())
        sql = """
        SELECT e.id AS event_id, e.custom_id
        FROM event e
        JOIN tournament t ON t.id = e.tournament_id
        JOIN tournament_registry tr
            ON tr.unique_tournament_id = t.unique_tournament_id
            AND tr.sport_slug = $1
            AND tr.is_active
            AND tr.current_enabled
        JOIN event_status es ON es.code = e.status_code
        WHERE e.custom_id IS NOT NULL
          AND length(e.custom_id) > 0
          AND es.type = 'finished'
          AND e.start_timestamp IS NOT NULL
          AND e.start_timestamp >= EXTRACT(EPOCH FROM now())::bigint - $2::bigint
          AND e.start_timestamp <= EXTRACT(EPOCH FROM now())::bigint + $3::bigint
          AND (
            cardinality($4::bigint[]) = 0
            OR t.unique_tournament_id = ANY($4::bigint[])
          )
        ORDER BY e.id
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
        out = tuple(
            (int(r["event_id"]), str(r["custom_id"]))
            for r in rows
            if r["custom_id"] is not None and str(r["custom_id"])
        )
        logger.info(
            "CustomIdOfRegistryEventsResolver: %s events with custom_id (sport=%s, past=%sd, future=%sd, pilot_uts=%s, limit=%s)",
            len(out),
            self.sport_slug,
            self.window_days_past,
            self.window_days_future,
            len(pilot_uts) if pilot_uts else "all",
            self.limit,
        )
        return out

    async def _read_cache(self) -> tuple[tuple[int, str], ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:
            logger.warning("CustomIdOfRegistryEventsResolver: redis GET failed (fail-open): %s", exc)
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
        out: list[tuple[int, str]] = []
        for item in data:
            if (
                isinstance(item, list)
                and len(item) == 2
                and isinstance(item[0], (int, str))
                and str(item[0]).lstrip("-").isdigit()
                and isinstance(item[1], str)
                and item[1]
            ):
                out.append((int(item[0]), str(item[1])))
        return tuple(out)

    def _write_cache(self, rows: tuple[tuple[int, str], ...]) -> None:
        if self.redis_backend is None:
            return
        try:
            payload = json.dumps([[r[0], r[1]] for r in rows], separators=(",", ":"))
            self.redis_backend.set(self.cache_key, payload, ex=self.cache_ttl_seconds)
        except Exception as exc:
            logger.warning("CustomIdOfRegistryEventsResolver: redis SET failed: %s", exc)


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
            logger.warning("CustomIdOfRegistryEventsResolver: ignoring non-integer pilot id %r", token)
            continue
        if value <= 0 or value in seen:
            continue
        seen.add(value)
        out.append(value)
    return tuple(out)


__all__ = [
    "CustomIdOfRegistryEventsResolver",
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
