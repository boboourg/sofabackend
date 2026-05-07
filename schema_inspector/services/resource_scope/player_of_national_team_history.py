"""``PlayerOfNationalTeamHistoryResolver`` — narrow scope for /national-team-statistics.

Stage D2 scope: players who appeared in at least one ingested
``event_lineup_player`` row whose ``team.national`` flag is true. This is
narrower than ``player-of-active-squad`` (~50k players) and avoids hitting
the upstream ``/national-team-statistics`` endpoint for ~40k players who
have never represented a national team.

Pre-D2 upstream probe established that ``/national-team-statistics``
returns HTTP 200 even for players without any national-team history (the
``statistics`` array is empty). That makes the endpoint a poor candidate
for ``ResourceNegativeCache`` 4xx-driven narrowing — we instead narrow at
the resolver level.

Important caveat: the scope is bounded by what we have lineup-hydrated.
Players whose national-team matches have not yet been hydrated will not
appear here even if they do have national-team history upstream. The
resolver will catch up automatically as lineup ingest continues.

Configuration::

    SCHEMA_INSPECTOR_RESOURCE_PLAYER_NATIONAL_LIMIT=200000
    SCHEMA_INSPECTOR_RESOURCE_PLAYER_NATIONAL_CACHE_TTL_SECONDS=1800
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

from .base import ResourceTarget

logger = logging.getLogger(__name__)

DEFAULT_LIMIT = 200_000
DEFAULT_CACHE_TTL_SECONDS = 1800
CACHE_KEY = "set:resource_refresh:player_national_team_history"

LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_PLAYER_NATIONAL_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_PLAYER_NATIONAL_CACHE_TTL_SECONDS"


class PlayerOfNationalTeamHistoryResolver:
    """Resolve players with at least one ingested national-team appearance."""

    kind = "player-of-national-team-history"

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
    def limit(self) -> int:
        return _positive_int(self._env.get(LIMIT_ENV_KEY), DEFAULT_LIMIT)

    @property
    def cache_ttl_seconds(self) -> int:
        return _positive_int(self._env.get(CACHE_TTL_ENV_KEY), DEFAULT_CACHE_TTL_SECONDS)

    async def resolve(self) -> Iterable[ResourceTarget]:
        player_ids = await self._read_cache()
        if player_ids is None:
            player_ids = await self._query_database()
            self._write_cache(player_ids)
        return tuple(
            ResourceTarget(
                entity_type="player",
                entity_id=int(player_id),
                path_params={"player_id": int(player_id)},
            )
            for player_id in player_ids
        )

    # ------------------------------------------------------------------

    async def _query_database(self) -> tuple[int, ...]:
        sql = """
        SELECT DISTINCT lp.player_id
        FROM event_lineup_player lp
        JOIN team t ON t.id = lp.team_id
        WHERE t.national = true
          AND lp.player_id IS NOT NULL
        ORDER BY lp.player_id
        LIMIT $1::bigint
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(sql, int(self.limit))
        player_ids = tuple(int(row["player_id"]) for row in rows if row["player_id"] is not None)
        logger.info(
            "PlayerOfNationalTeamHistoryResolver: %s players (limit=%s)",
            len(player_ids),
            self.limit,
        )
        return player_ids

    async def _read_cache(self) -> tuple[int, ...] | None:
        if self.redis_backend is None:
            return None
        try:
            raw = self.redis_backend.get(self.cache_key)
        except Exception as exc:
            logger.warning(
                "PlayerOfNationalTeamHistoryResolver: redis GET failed (fail-open): %s", exc
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
        return tuple(
            int(value)
            for value in data
            if isinstance(value, (int, str)) and str(value).lstrip("-").isdigit()
        )

    def _write_cache(self, player_ids: tuple[int, ...]) -> None:
        if self.redis_backend is None:
            return
        try:
            payload = json.dumps(list(player_ids), separators=(",", ":"))
            self.redis_backend.set(self.cache_key, payload, ex=self.cache_ttl_seconds)
        except Exception as exc:
            logger.warning("PlayerOfNationalTeamHistoryResolver: redis SET failed: %s", exc)


def _positive_int(raw: object, default: int) -> int:
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


__all__ = ["PlayerOfNationalTeamHistoryResolver", "CACHE_KEY"]
