"""``CustomIdOfManagedEventsResolver`` — D12 custom_id fan-out for /h2h/events.

Reads ``event.custom_id`` for finished/live events under each managed
(ut, season) pair, yields one ResourceTarget per event with
``path_params={'custom_id': ...}`` so ``EVENT_H2H_EVENTS_ENDPOINT``
('/event/{custom_id}/h2h/events') can be fetched. Note: that endpoint
template uses ``{custom_id}`` (string), not ``{event_id}`` — the only
endpoint in the registry that does so.
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
DEFAULT_LIMIT = 50_000
CACHE_KEY = "set:resource_refresh:custom_id_managed_events"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_CUSTOM_ID_CACHE_TTL_SECONDS"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_CUSTOM_ID_LIMIT"


class CustomIdOfManagedEventsResolver:
    """Fan-out custom_id targets for managed football leagues."""

    kind = "custom-id-of-managed-events"

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

    @property
    def limit(self) -> int:
        raw = self._env.get(LIMIT_ENV_KEY)
        if raw in (None, ""):
            return DEFAULT_LIMIT
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return DEFAULT_LIMIT
        return value if value > 0 else DEFAULT_LIMIT

    def managed_pairs(self) -> tuple[tuple[int, int], ...]:
        return load_managed_pairs(self._env)

    async def resolve(self) -> Iterable[ResourceTarget]:
        pairs = self.managed_pairs()
        if not pairs:
            return ()
        rows = await self._read_cache()
        if rows is None:
            rows = await self._query_database(pairs)
            self._write_cache(rows)
        return tuple(
            ResourceTarget(
                entity_type="event",
                entity_id=int(event_id),
                path_params={"custom_id": custom_id},
                context_event_id=int(event_id),
                sport_slug="football",
            )
            for (event_id, custom_id) in rows
        )

    # ------------------------------------------------------------------

    async def _query_database(
        self, pairs: tuple[tuple[int, int], ...]
    ) -> tuple[tuple[int, str], ...]:
        ut_arr = [p[0] for p in pairs]
        season_arr = [p[1] for p in pairs]
        sql = """
        WITH managed AS (
            SELECT unnest($1::bigint[]) AS ut, unnest($2::bigint[]) AS season
        )
        SELECT e.id AS event_id, e.custom_id
        FROM event e
        JOIN tournament t ON t.id = e.tournament_id
        JOIN managed m
          ON m.ut = t.unique_tournament_id
         AND m.season = e.season_id
        WHERE e.custom_id IS NOT NULL
          AND length(e.custom_id) > 0
        ORDER BY e.id
        LIMIT $3::bigint
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(sql, ut_arr, season_arr, int(self.limit))
        out = tuple(
            (int(r["event_id"]), str(r["custom_id"]))
            for r in rows
            if r["custom_id"] is not None
        )
        logger.info(
            "CustomIdOfManagedEventsResolver: %s events with custom_id for %s pairs",
            len(out), len(pairs),
        )
        return out

    async def _read_cache(self) -> tuple[tuple[int, str], ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:
            logger.warning(
                "CustomIdOfManagedEventsResolver: redis GET failed (fail-open): %s", exc
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
            logger.warning(
                "CustomIdOfManagedEventsResolver: redis SET failed: %s", exc
            )


__all__ = [
    "CustomIdOfManagedEventsResolver",
    "CACHE_KEY",
    "DEFAULT_CACHE_TTL_SECONDS",
    "DEFAULT_LIMIT",
]
