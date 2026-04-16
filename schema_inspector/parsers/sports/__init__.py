"""Sport adapter metadata and resolver for archetype-driven parsing."""

from __future__ import annotations

from dataclasses import dataclass

from .american_football import SPORT_ADAPTER as AMERICAN_FOOTBALL_ADAPTER
from .baseball import SPORT_ADAPTER as BASEBALL_ADAPTER
from .basketball import SPORT_ADAPTER as BASKETBALL_ADAPTER
from .cricket import SPORT_ADAPTER as CRICKET_ADAPTER
from .esports import SPORT_ADAPTER as ESPORTS_ADAPTER
from .football import SPORT_ADAPTER as FOOTBALL_ADAPTER
from .futsal import SPORT_ADAPTER as FUTSAL_ADAPTER
from .handball import SPORT_ADAPTER as HANDBALL_ADAPTER
from .ice_hockey import SPORT_ADAPTER as ICE_HOCKEY_ADAPTER
from .rugby import SPORT_ADAPTER as RUGBY_ADAPTER
from .table_tennis import SPORT_ADAPTER as TABLE_TENNIS_ADAPTER
from .tennis import SPORT_ADAPTER as TENNIS_ADAPTER
from .volleyball import SPORT_ADAPTER as VOLLEYBALL_ADAPTER


@dataclass(frozen=True)
class SportAdapter:
    sport_slug: str
    archetype: str
    core_event_edges: tuple[str, ...]
    live_optional_edges: tuple[str, ...] = ()
    special_families: tuple[str, ...] = ()
    hydrate_entity_profiles: bool = False


GENERIC_ADAPTER = SportAdapter(
    sport_slug="generic",
    archetype="generic_team",
    core_event_edges=("meta", "statistics", "lineups"),
    live_optional_edges=(),
    special_families=(),
    hydrate_entity_profiles=False,
)


def _coerce(spec: dict[str, object]) -> SportAdapter:
    return SportAdapter(
        sport_slug=str(spec["sport_slug"]),
        archetype=str(spec["archetype"]),
        core_event_edges=tuple(spec.get("core_event_edges", ()) or ()),
        live_optional_edges=tuple(spec.get("live_optional_edges", ()) or ()),
        special_families=tuple(spec.get("special_families", ()) or ()),
        hydrate_entity_profiles=bool(spec.get("hydrate_entity_profiles", False)),
    )


_ADAPTERS = {
    adapter.sport_slug: adapter
    for adapter in (
        _coerce(FOOTBALL_ADAPTER),
        _coerce(TENNIS_ADAPTER),
        _coerce(BASKETBALL_ADAPTER),
        _coerce(VOLLEYBALL_ADAPTER),
        _coerce(BASEBALL_ADAPTER),
        _coerce(AMERICAN_FOOTBALL_ADAPTER),
        _coerce(HANDBALL_ADAPTER),
        _coerce(TABLE_TENNIS_ADAPTER),
        _coerce(ICE_HOCKEY_ADAPTER),
        _coerce(RUGBY_ADAPTER),
        _coerce(CRICKET_ADAPTER),
        _coerce(FUTSAL_ADAPTER),
        _coerce(ESPORTS_ADAPTER),
    )
}


def resolve_sport_adapter(sport_slug: str) -> SportAdapter:
    normalized = str(sport_slug).strip().lower()
    if not normalized:
        return GENERIC_ADAPTER
    return _ADAPTERS.get(normalized, SportAdapter(
        sport_slug=normalized,
        archetype=GENERIC_ADAPTER.archetype,
        core_event_edges=GENERIC_ADAPTER.core_event_edges,
        live_optional_edges=GENERIC_ADAPTER.live_optional_edges,
        special_families=GENERIC_ADAPTER.special_families,
        hydrate_entity_profiles=GENERIC_ADAPTER.hydrate_entity_profiles,
    ))


__all__ = ["SportAdapter", "resolve_sport_adapter", "GENERIC_ADAPTER"]
