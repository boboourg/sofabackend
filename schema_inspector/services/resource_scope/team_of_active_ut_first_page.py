"""``TeamOfActiveUTFirstPageResolver`` — page=0 wrapper for active teams.

Stage 5 covers ``team/{id}/events/last/0`` and ``team/{id}/events/next/0``
through the planner. Mirrors ``PlayerOfActiveSquadFirstPageResolver``:
shares the underlying SQL+cache with the base ``TeamOfActiveUTResolver``,
adds ``page=0`` to ``path_params``, and exposes a distinct ``kind`` so
the page=0 endpoint and the bare-team endpoint can opt into the resource
refresh loop with different cadences.
"""

from __future__ import annotations

import logging
from typing import Iterable

from .base import ResourceTarget
from .team_of_active_ut import TeamOfActiveUTResolver

logger = logging.getLogger(__name__)


class TeamOfActiveUTFirstPageResolver:
    """Yields ``(team_id, page=0)`` targets for the active-UT team scope."""

    kind = "team-of-active-ut-first-page"

    def __init__(self, *, base: TeamOfActiveUTResolver) -> None:
        self.base = base

    async def resolve(self) -> Iterable[ResourceTarget]:
        base_targets = await self.base.resolve()
        return tuple(
            ResourceTarget(
                entity_type=target.entity_type,
                entity_id=target.entity_id,
                path_params={"team_id": int(target.entity_id), "page": 0},
            )
            for target in base_targets
        )


__all__ = ["TeamOfActiveUTFirstPageResolver"]
