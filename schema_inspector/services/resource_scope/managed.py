"""``ManagedScopeResolver`` — env-driven team list for Stage A pilot.

This resolver reads a comma-separated env list of team IDs and yields one
``ResourceTarget`` per team. It is intentionally minimal: the goal of
Stage A is to prove the end-to-end pipeline (planner -> stream -> worker
-> snapshot -> local API) on a small, controllable scope. Stage B will
introduce a SQL-driven resolver (`team-of-active-ut`).

Env contract::

    SCHEMA_INSPECTOR_RESOURCE_PILOT_TEAMS=42,2672,17,44,2829

Empty / unset / non-integer values are silently filtered out.
"""

from __future__ import annotations

import logging
import os
from typing import Iterable

from .base import ResourceTarget, ScopeResolver

logger = logging.getLogger(__name__)

DEFAULT_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_PILOT_TEAMS"


class ManagedScopeResolver:
    """Yields one ``ResourceTarget(entity_type='team')`` per pilot team id."""

    kind = "managed"

    def __init__(
        self,
        *,
        env_key: str = DEFAULT_ENV_KEY,
        env: dict[str, str] | None = None,
    ) -> None:
        self.env_key = env_key
        self._env_snapshot = env

    async def resolve(self) -> Iterable[ResourceTarget]:
        raw = self._read_env()
        team_ids = _parse_team_ids(raw)
        if not team_ids:
            logger.info(
                "ManagedScopeResolver: no team ids in %s — skipping resolve()",
                self.env_key,
            )
            return ()
        return tuple(
            ResourceTarget(
                entity_type="team",
                entity_id=team_id,
                path_params={"team_id": team_id},
            )
            for team_id in team_ids
        )

    def _read_env(self) -> str:
        if self._env_snapshot is not None:
            return str(self._env_snapshot.get(self.env_key, "") or "")
        return os.environ.get(self.env_key, "") or ""


def _parse_team_ids(raw: str) -> tuple[int, ...]:
    """Parse a comma-separated list of team IDs, dropping invalid entries."""

    if not raw:
        return ()
    out: list[int] = []
    seen: set[int] = set()
    for chunk in str(raw).split(","):
        token = chunk.strip()
        if not token:
            continue
        try:
            value = int(token)
        except ValueError:
            logger.warning("ManagedScopeResolver: skipping non-integer team id %r", token)
            continue
        if value <= 0:
            continue
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return tuple(out)


__all__ = ["ManagedScopeResolver", "DEFAULT_ENV_KEY"]
