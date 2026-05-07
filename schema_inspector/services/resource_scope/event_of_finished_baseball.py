"""``EventOfFinishedBaseballResolver`` — active baseball events for /at-bats refresh.

Despite the historical class name (kept for git-blame continuity), the
resolver now covers BOTH finished and inprogress baseball events. Sofascore
returns 200 with a live-updating ``atBats`` array even mid-game (probe
established this for status_code 23/24/29 — innings — and 30 — pause), so
the Resource Refresh Loop picks them up and re-fetches every refresh
interval. Once the game ends (status_code 100/110), the payload freezes
and subsequent refreshes get deduped by the FreshnessStore until the
event drops out of the rolling window.

Status codes covered:

* 100 — Ended
* 110 — AET (extra-inning finish)
* 23, 24, 29 — Inning N (live)
* 30 — Pause (live, between innings)

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
# Status codes the resolver picks up. 100/110 are finished; 23/24/29 are
# in-progress innings (Sofascore exposes one code per inning); 30 is the
# inter-inning pause. Live games surface here so /at-bats refreshes during
# the match and not just after the final out.
BASEBALL_ACTIVE_STATUS_CODES: tuple[int, ...] = (100, 110, 23, 24, 29, 30)
# Deprecated alias kept for any external import path; do not rely on it.
BASEBALL_FINISHED_STATUS_CODE = 100

WINDOW_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_BASEBALL_FINISHED_WINDOW_DAYS"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_BASEBALL_FINISHED_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_BASEBALL_FINISHED_CACHE_TTL_SECONDS"


class EventOfFinishedBaseballResolver:
    """Resolve active (finished + live) baseball events, cached in Redis."""

    kind = "event-of-active-baseball"

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
          AND e.status_code = ANY($1::int[])
          AND e.start_timestamp IS NOT NULL
          AND e.start_timestamp >= EXTRACT(EPOCH FROM now())::bigint - $2::bigint
        ORDER BY e.start_timestamp DESC
        LIMIT $3::bigint
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(
                sql,
                list(BASEBALL_ACTIVE_STATUS_CODES),
                int(cutoff_seconds),
                int(self.limit),
            )
        event_ids = tuple(int(row["event_id"]) for row in rows if row["event_id"] is not None)
        logger.info(
            "EventOfFinishedBaseballResolver: %s events (window=%sd, limit=%s, statuses=%s)",
            len(event_ids),
            self.window_days,
            self.limit,
            BASEBALL_ACTIVE_STATUS_CODES,
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


__all__ = [
    "EventOfFinishedBaseballResolver",
    "CACHE_KEY",
    "BASEBALL_ACTIVE_STATUS_CODES",
    "BASEBALL_FINISHED_STATUS_CODE",
]
