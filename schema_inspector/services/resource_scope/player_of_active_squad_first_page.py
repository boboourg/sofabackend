"""``PlayerOfActiveSquadFirstPageResolver`` — page=0 wrapper for active squad.

Stage C.3 covers the first page of ``/api/v1/player/{player_id}/events/last/{page}``
for every player on a recently-fetched active-squad team roster. This is
the page the frontend shows on the player profile "Matches" tab.

This resolver is intentionally a thin wrapper over
``PlayerOfActiveSquadResolver`` rather than a copy of the SQL: by sharing
the same instance both scope_kinds hit the *same* Redis cache and the
same database query (one SQL per tick, not one per scope_kind).

Stage C.4 will add worker-side auto-pagination (page>=1 walked until
``hasNextPage=false`` via the delayed scheduler) without touching this
resolver — page=0 stays the planner-driven entry point at the configured
6h cadence, deep history pages live on a separate, much longer cadence.
"""

from __future__ import annotations

import logging
from typing import Iterable

from .base import ResourceTarget
from .player_of_active_squad import PlayerOfActiveSquadResolver

logger = logging.getLogger(__name__)


class PlayerOfActiveSquadFirstPageResolver:
    """Yields ``(player_id, page=0)`` targets for the active squad scope."""

    kind = "player-of-active-squad-first-page"

    def __init__(self, *, base: PlayerOfActiveSquadResolver) -> None:
        self.base = base

    async def resolve(self) -> Iterable[ResourceTarget]:
        base_targets = await self.base.resolve()
        return tuple(
            ResourceTarget(
                entity_type=target.entity_type,
                entity_id=target.entity_id,
                path_params={"player_id": int(target.entity_id), "page": 0},
            )
            for target in base_targets
        )


__all__ = ["PlayerOfActiveSquadFirstPageResolver"]
