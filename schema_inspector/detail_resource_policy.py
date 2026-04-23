"""Shared policy for event-detail resources that depend on current event state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

from .endpoints import (
    EVENT_BASEBALL_INNINGS_ENDPOINT,
    EVENT_COMMENTS_ENDPOINT,
    EVENT_ESPORTS_GAMES_ENDPOINT,
    EVENT_GRAPH_ENDPOINT,
    EVENT_H2H_ENDPOINT,
    EVENT_HEATMAP_ENDPOINT,
    EVENT_MANAGERS_ENDPOINT,
    EVENT_ODDS_ALL_ENDPOINT,
    EVENT_ODDS_FEATURED_ENDPOINT,
    EVENT_POINT_BY_POINT_ENDPOINT,
    EVENT_PREGAME_FORM_ENDPOINT,
    EVENT_SHOTMAP_ENDPOINT,
    EVENT_TENNIS_POWER_ENDPOINT,
    EVENT_VOTES_ENDPOINT,
    EVENT_WINNING_ODDS_ENDPOINT,
    SofascoreEndpoint,
)
from .parsers.sports import resolve_sport_adapter


LIVE_DETAIL_STATUS_TYPES = frozenset({"inprogress", "finished"})


@dataclass(frozen=True)
class EventDetailRequestSpec:
    endpoint: SofascoreEndpoint
    path_params: Mapping[str, int] = field(default_factory=dict)

    def resolved_path_params(self, *, event_id: int) -> dict[str, int]:
        resolved = {"event_id": int(event_id)}
        for key, value in self.path_params.items():
            resolved[str(key)] = int(value)
        return resolved


def supports_live_detail_resources(status_type: str | None) -> bool:
    normalized = str(status_type or "").strip().lower()
    return normalized in LIVE_DETAIL_STATUS_TYPES


def build_event_detail_request_specs(
    *,
    sport_slug: str | None,
    status_type: str | None,
    team_ids: tuple[int, ...] | list[int] = (),
    provider_ids: tuple[int, ...] | list[int] = (1,),
    has_event_player_heat_map: bool | None = None,
    has_xg: bool | None = None,
    core_only: bool = False,
) -> tuple[EventDetailRequestSpec, ...]:
    normalized_sport_slug = str(sport_slug or "").strip().lower()
    adapter = resolve_sport_adapter(normalized_sport_slug)
    is_live_detail = supports_live_detail_resources(status_type)
    deduped: list[EventDetailRequestSpec] = []
    seen: set[tuple[str, tuple[tuple[str, int], ...]]] = set()

    def add(endpoint: SofascoreEndpoint, **path_params: int) -> None:
        signature = (endpoint.pattern, tuple(sorted((str(key), int(value)) for key, value in path_params.items())))
        if signature in seen:
            return
        seen.add(signature)
        deduped.append(EventDetailRequestSpec(endpoint=endpoint, path_params={key: int(value) for key, value in path_params.items()}))

    if not core_only:
        for endpoint in (
            EVENT_MANAGERS_ENDPOINT,
            EVENT_H2H_ENDPOINT,
            EVENT_PREGAME_FORM_ENDPOINT,
            EVENT_VOTES_ENDPOINT,
        ):
            add(endpoint)
        for provider_id in _dedupe_ints(provider_ids):
            add(EVENT_ODDS_ALL_ENDPOINT, provider_id=provider_id)
            add(EVENT_ODDS_FEATURED_ENDPOINT, provider_id=provider_id)
            add(EVENT_WINNING_ODDS_ENDPOINT, provider_id=provider_id)

    if "baseball_innings" in adapter.special_families:
        add(EVENT_BASEBALL_INNINGS_ENDPOINT)
    if "esports_games" in adapter.special_families:
        add(EVENT_ESPORTS_GAMES_ENDPOINT)

    if not is_live_detail:
        return tuple(deduped)

    add(EVENT_COMMENTS_ENDPOINT)

    if "tennis_point_by_point" in adapter.special_families:
        add(EVENT_POINT_BY_POINT_ENDPOINT)
    if "tennis_power" in adapter.special_families:
        add(EVENT_TENNIS_POWER_ENDPOINT)
    if "tennis_point_by_point" in adapter.special_families or "tennis_power" in adapter.special_families:
        return tuple(deduped)

    add(EVENT_GRAPH_ENDPOINT)

    if not core_only and has_event_player_heat_map is True:
        for team_id in _dedupe_ints(team_ids):
            add(EVENT_HEATMAP_ENDPOINT, team_id=team_id)

    if not core_only and (has_xg is True or "shotmap" in adapter.special_families):
        add(EVENT_SHOTMAP_ENDPOINT)

    return tuple(deduped)


def _dedupe_ints(values: tuple[int, ...] | list[int]) -> tuple[int, ...]:
    deduped: list[int] = []
    for value in values:
        if not isinstance(value, int) or value in deduped:
            continue
        deduped.append(value)
    return tuple(deduped)
