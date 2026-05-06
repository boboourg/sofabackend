"""Protocol + value type for scope resolvers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Mapping, Protocol, runtime_checkable


@dataclass(frozen=True)
class ResourceTarget:
    """One ``(endpoint, entity)`` target for the resource refresh planner.

    ``path_params`` MUST contain every placeholder used by the endpoint's
    ``path_template`` (e.g. ``team_id``, ``page``). The planner uses the
    full ``path_params`` mapping as part of the cursor key, so two targets
    that differ only in ``page`` track separate refresh times.
    """

    entity_type: str
    entity_id: int
    path_params: Mapping[str, object] = field(default_factory=dict)
    context_unique_tournament_id: int | None = None
    context_season_id: int | None = None
    context_event_id: int | None = None
    sport_slug: str | None = None


@runtime_checkable
class ScopeResolver(Protocol):
    """Yield resource targets for a given scope_kind.

    Implementations declare ``kind`` (matching ``SofascoreEndpoint.scope_kind``)
    and a single async ``resolve`` method. The planner caches resolver outputs
    only for the duration of one tick; resolvers are expected to do their
    own caching if the underlying query is expensive.
    """

    kind: str

    async def resolve(self) -> Iterable[ResourceTarget]:
        ...


__all__ = ["ResourceTarget", "ScopeResolver"]
