"""``PlayerOfActiveSquadResolver`` — players from recently-ingested team rosters.

Stage C.1 scope: players that appear in any successful (HTTP 200) team
roster snapshot within the configured window. Source of truth is the same
``api_payload_snapshot`` table the resource refresh worker writes into,
namely the ``/api/v1/team/{team_id}/players`` endpoint.

Why this source rather than e.g. ``event_player_statistics`` or a join
through ``team`` table:

* Players come straight from the upstream squad payload, so the scope
  follows the same "active leagues" definition as Stage B (active teams
  with recent standings → fetched team rosters → their current squad).
* Filtering on ``http_status = 200`` automatically excludes the
  soft-error rows produced by stale team_ids -- so we do not feed the
  player pipeline ghost teams.
* New players discovered after the next team-roster refresh become
  visible to the planner within at most ``cache_ttl_seconds`` (default
  30 minutes).

Configuration::

    SCHEMA_INSPECTOR_RESOURCE_PLAYER_ACTIVE_WINDOW_DAYS=7
    SCHEMA_INSPECTOR_RESOURCE_PLAYER_ACTIVE_LIMIT=200000
    SCHEMA_INSPECTOR_RESOURCE_PLAYER_ACTIVE_CACHE_TTL_SECONDS=1800
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Iterable

from .base import ResourceTarget

logger = logging.getLogger(__name__)

DEFAULT_WINDOW_DAYS = 7
DEFAULT_LIMIT = 200_000
DEFAULT_CACHE_TTL_SECONDS = 1800
CACHE_KEY = "set:resource_refresh:player_active_squad"

WINDOW_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_PLAYER_ACTIVE_WINDOW_DAYS"
LIMIT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_PLAYER_ACTIVE_LIMIT"
CACHE_TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_PLAYER_ACTIVE_CACHE_TTL_SECONDS"


class PlayerOfActiveSquadResolver:
    """Resolve players from recently-ingested team rosters, cached in Redis."""

    kind = "player-of-active-squad"

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
        cutoff_seconds = self.window_days * 86_400
        sql = """
        WITH recent_team_rosters AS (
            SELECT payload
            FROM api_payload_snapshot
            WHERE endpoint_pattern = '/api/v1/team/{team_id}/players'
              AND http_status = 200
              AND is_soft_error_payload IS NOT TRUE
              AND fetched_at >= now() - make_interval(secs => $1::bigint)
        ),
        squad_items AS (
            SELECT jsonb_array_elements(payload->'players') AS item
            FROM recent_team_rosters
        )
        SELECT DISTINCT (item->'player'->>'id')::bigint AS player_id
        FROM squad_items
        WHERE (item->'player'->>'id') ~ '^[0-9]+$'
        ORDER BY player_id
        LIMIT $2::bigint
        """
        async with self.database.connection() as connection:
            rows = await connection.fetch(sql, int(cutoff_seconds), int(self.limit))
        player_ids = tuple(int(row["player_id"]) for row in rows if row["player_id"] is not None)
        logger.info(
            "PlayerOfActiveSquadResolver: fetched %s active player ids "
            "(window=%sd, limit=%s)",
            len(player_ids),
            self.window_days,
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
                "PlayerOfActiveSquadResolver: redis GET failed (fail-open): %s", exc
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
        ttl = self.cache_ttl_seconds
        try:
            payload = json.dumps(list(player_ids), separators=(",", ":"))
            self.redis_backend.set(self.cache_key, payload, ex=ttl)
        except Exception as exc:
            logger.warning("PlayerOfActiveSquadResolver: redis SET failed: %s", exc)


def _positive_int(raw: object, default: int) -> int:
    if raw in (None, ""):
        return default
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


__all__ = ["PlayerOfActiveSquadResolver", "CACHE_KEY"]
