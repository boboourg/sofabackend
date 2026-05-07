"""``EventOfFinishedBaseballResolver`` — finished baseball events for /at-bats refresh.

Stage D2 scope: baseball events with ``status_code = 100`` (finished) whose
``start_timestamp`` falls inside the configured rolling window. The
``/api/v1/event/{event_id}/at-bats`` payload is one-shot in nature — once
the game ends, the at-bats list is frozen. We still re-publish via the
refresh planner periodically (default once per year per event) so a
late-arriving correction from upstream can be picked up.

Combined with the endpoint's ``freshness_ttl_seconds`` (350 days) this
produces effectively a single fetch per event for the lifetime of the
event in this rolling window.

Configuration::

    SCHEMA_INSPECTOR_RESOURCE_BASEBALL_FINISHED_WINDOW_DAYS=30
    SCHEMA_INSPECTOR_RESOURCE_BASEBALL_FINISHED_LIMIT=20000
    SCHEMA_INSPECTOR_RESOURCE_BASEBALL_FINISHED_CACHE_TTL_SECONDS=1800
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

from .base import ResourceTarget

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_DAYS = 30
DEFAULT_LIMIT = 20_000
DEFAULT_CACHE_TTL_SECONDS = 1800
CACHE_KEY = "set:resource_refresh:event_finished_baseball"
BASEBALL_FINISHED_STATUS_CODE = 100

WINDOW_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_BASEBALL_FINISHED_WINDOW_DAYS"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_BASEBALL_FINISHED_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_BASEBALL_FINISHED_CACHE_TTL_SECONDS"


class EventOfFinishedBaseballResolver:
    """Resolve finished baseball events from a rolling window, cached in Redis."""

    kind = "event-of-finished-baseball"

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

    async def resolve(self) -> Iterable[ResourceTarget]:
        event_ids = await self._read_cache()
        if event_ids is None:
            event_ids = await self._query_database()
            self._write_cache(event_ids)
        return tuple(
            ResourceTarget(
                entity_type="event",
                entity_id=int(event_id),
                path_params={"event_id": int(event_id)},
                context_event_id=int(event_id),
                sport_slug="baseball",
            )
            for event_id in event_ids
        )

    # ------------------------------------------------------------------

    async def _query_database(self) -> tuple[int, ...]:
        cutoff_seconds = self.window_days * 86_400
        sql = """
        SELECT e.id AS event_id
        FROM event e
        JOIN tournament t ON t.id = e.tournament_id
        JOIN unique_tournament ut ON ut.id = t.unique_tournament_id
        JOIN category c ON c.id = ut.category_id
        JOIN sport sp ON sp.id = c.sport_id
        WHERE sp.slug = 'baseball'
          AND e.status_code = $1::int
          AND e.start_timestamp IS NOT NULL
          AND e.start_timestamp >= EXTRACT(EPOCH FROM now())::bigint - $2::bigint
        ORDER BY e.start_timestamp DESC
        LIMIT $3::bigint
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(
                sql,
                int(BASEBALL_FINISHED_STATUS_CODE),
                int(cutoff_seconds),
                int(self.limit),
            )
        event_ids = tuple(int(row["event_id"]) for row in rows if row["event_id"] is not None)
        logger.info(
            "EventOfFinishedBaseballResolver: %s events (window=%sd, limit=%s)",
            len(event_ids),
            self.window_days,
            self.limit,
        )
        return event_ids

    async def _read_cache(self) -> tuple[int, ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:
            logger.warning("EventOfFinishedBaseballResolver: redis GET failed (fail-open): %s", exc)
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

    def _write_cache(self, event_ids: tuple[int, ...]) -> None:
        if self.redis_backend is None:
            return
        try:
            payload = json.dumps(list(event_ids), separators=(",", ":"))
            self.redis_backend.set(self.cache_key, payload, ex=self.cache_ttl_seconds)
        except Exception as exc:
            logger.warning("EventOfFinishedBaseballResolver: redis SET failed: %s", exc)


def _positive_int(raw: object, default: int) -> int:
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


__all__ = ["EventOfFinishedBaseballResolver", "CACHE_KEY", "BASEBALL_FINISHED_STATUS_CODE"]
