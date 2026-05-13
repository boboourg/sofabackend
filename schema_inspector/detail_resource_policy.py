"""Shared policy for event-detail resources that depend on current event state."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

from .endpoints import (
    EVENT_BASEBALL_INNINGS_ENDPOINT,
    EVENT_INNINGS_ENDPOINT,
    EVENT_AVERAGE_POSITIONS_ENDPOINT,
    EVENT_BEST_PLAYERS_SUMMARY_ENDPOINT,
    EVENT_COMMENTS_ENDPOINT,
    EVENT_ESPORTS_GAMES_ENDPOINT,
    EVENT_GRAPH_ENDPOINT,
    EVENT_H2H_ENDPOINT,
    EVENT_H2H_EVENTS_ENDPOINT,
    EVENT_HEATMAP_ENDPOINT,
    EVENT_HIGHLIGHTS_ENDPOINT,
    EVENT_MANAGERS_ENDPOINT,
    EVENT_ODDS_ALL_ENDPOINT,
    EVENT_ODDS_FEATURED_ENDPOINT,
    EVENT_OFFICIAL_TWEETS_ENDPOINT,
    EVENT_PLAYER_HEATMAP_ENDPOINT,
    EVENT_POINT_BY_POINT_ENDPOINT,
    EVENT_PREGAME_FORM_ENDPOINT,
    EVENT_TEAM_STREAKS_BETTING_ODDS_ENDPOINT,
    EVENT_TEAM_STREAKS_ENDPOINT,
    EVENT_SHOTMAP_ENDPOINT,
    EVENT_TENNIS_POWER_ENDPOINT,
    EVENT_VOTES_ENDPOINT,
    EVENT_WINNING_ODDS_ENDPOINT,
    SofascoreEndpoint,
)
from .event_endpoint_static_denylist import is_static_dead_event_endpoint
from .live_delta_policy import live_delta_detail_endpoints
from .match_center_policy import filter_football_detail_specs, football_highlights_allowed
from .parsers.sports import resolve_sport_adapter


LIVE_DETAIL_STATUS_TYPES = frozenset({"inprogress", "finished"})


@dataclass(frozen=True)
class EventDetailRequestSpec:
    endpoint: SofascoreEndpoint
    path_params: Mapping[str, Any] = field(default_factory=dict)

    def resolved_path_params(self, *, event_id: int) -> dict[str, Any]:
        resolved = {"event_id": int(event_id)}
        for key, value in self.path_params.items():
            resolved[str(key)] = value
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
    has_event_player_statistics: bool | None = None,
    has_event_player_heat_map: bool | None = None,
    has_global_highlights: bool | None = None,
    has_xg: bool | None = None,
    detail_id: int | None = None,
    custom_id: str | None = None,
    start_timestamp: int | None = None,
    now_timestamp: int | None = None,
    core_only: bool = False,
    hydration_mode: str = "full",
    is_editor: bool | None = None,
) -> tuple[EventDetailRequestSpec, ...]:
    """Build the event-detail request specs for a given event.

    X'' patch: ``is_editor=True`` short-circuits the football path to an
    empty tuple — SofaEditor events do not generate any detail fetches.
    Non-football sports are unaffected (legacy bypass preserved).
    """
    normalized_sport_slug = str(sport_slug or "").strip().lower()
    normalized_hydration_mode = str(hydration_mode or "full").strip().lower()

    # X'' HARD BAN: football + isEditor=True → no detail fetches.
    # Short-circuit before any work is done; the per-spec filter would
    # also drop everything but bailing here is cheaper and clearer.
    if normalized_sport_slug == "football" and is_editor is True:
        return ()
    adapter = resolve_sport_adapter(normalized_sport_slug)
    is_live_detail = supports_live_detail_resources(status_type)
    deduped: list[EventDetailRequestSpec] = []
    seen: set[tuple[str, tuple[tuple[str, str], ...]]] = set()

    def add(endpoint: SofascoreEndpoint, **path_params: Any) -> None:
        if is_static_dead_event_endpoint(normalized_sport_slug, endpoint.pattern):
            return
        signature = (endpoint.pattern, tuple(sorted((str(key), repr(value)) for key, value in path_params.items())))
        if signature in seen:
            return
        seen.add(signature)
        deduped.append(EventDetailRequestSpec(endpoint=endpoint, path_params={str(key): value for key, value in path_params.items()}))

    if normalized_hydration_mode == "live_delta":
        if not is_live_detail:
            return ()
        if normalized_sport_slug == "football":
            # X4 (2026-05-13): football fall-through to the full matchcenter
            # spec path below. Production probe matrix (event_endpoint_availability_log,
            # 7d, non-editor football) confirmed managers, h2h, pregame-form, votes,
            # odds×3, team-streaks, comments, best-players, graph, heatmap, shotmap,
            # average-positions are all 89-100% useful during inprogress phase.
            # Prior to X4 this branch returned `()` for football → live polling
            # fetched only ROOT + 5 core edges → matchcenter populated only at
            # final_sweep → user-visible "live has no data, appears after match end".
            # Per-event filtering still happens via _filter_specs →
            # football_detail_endpoint_allowed, which enforces tier_1/2 vs
            # tier_3/5 gating and the isEditor HARD BAN. Per-player followups
            # (heatmap, rating-breakdown, shotmap/player, etc.) are NOT enabled
            # because `lightweight_only` stays True for live_delta in orchestrator.
            pass  # intentional fall-through to non-live-delta path
        else:
            for endpoint in live_delta_detail_endpoints(normalized_sport_slug):
                add(endpoint)
            return _filter_specs(
                tuple(deduped),
                sport_slug=normalized_sport_slug,
                status_type=status_type,
                detail_id=detail_id,
                has_xg=has_xg,
                has_event_player_heat_map=has_event_player_heat_map,
                has_event_player_statistics=has_event_player_statistics,
                has_global_highlights=has_global_highlights,
                start_timestamp=start_timestamp,
                now_timestamp=now_timestamp,
                is_editor=is_editor,
            )

    if not core_only:
        if normalized_sport_slug == "tennis":
            if custom_id:
                add(EVENT_H2H_EVENTS_ENDPOINT, custom_id=str(custom_id))
            add(EVENT_TEAM_STREAKS_ENDPOINT)
        else:
            for endpoint in (
                EVENT_MANAGERS_ENDPOINT,
                EVENT_H2H_ENDPOINT,
                EVENT_PREGAME_FORM_ENDPOINT,
                EVENT_VOTES_ENDPOINT,
            ):
                add(endpoint)
            if normalized_sport_slug == "football":
                if custom_id:
                    add(EVENT_H2H_EVENTS_ENDPOINT, custom_id=str(custom_id))
                add(EVENT_TEAM_STREAKS_ENDPOINT)
        for provider_id in _dedupe_ints(provider_ids):
            add(EVENT_ODDS_ALL_ENDPOINT, provider_id=provider_id)
            add(EVENT_ODDS_FEATURED_ENDPOINT, provider_id=provider_id)
            add(EVENT_WINNING_ODDS_ENDPOINT, provider_id=provider_id)
            if normalized_sport_slug == "football":
                add(EVENT_TEAM_STREAKS_BETTING_ODDS_ENDPOINT, provider_id=provider_id)

    # P0.2: /innings used to be wired here for baseball — confirmed dead
    # on prod (100% soft-error 404 over 30 days). The endpoint is now
    # cricket-only via the cricket adapter's ``cricket_innings`` family.
    if "baseball_innings" in adapter.special_families:
        # Legacy: keep the import path alive so a hot-fix can resurrect
        # baseball /innings without redeploying. Currently no sport
        # adapter sets baseball_innings, so this is dormant.
        add(EVENT_BASEBALL_INNINGS_ENDPOINT)
    if "cricket_innings" in adapter.special_families:
        add(EVENT_INNINGS_ENDPOINT)
    if "esports_games" in adapter.special_families:
        add(EVENT_ESPORTS_GAMES_ENDPOINT)
    if not core_only and has_global_highlights is True:
        add(EVENT_HIGHLIGHTS_ENDPOINT)

    if not is_live_detail:
        return _filter_specs(
            tuple(deduped),
            sport_slug=normalized_sport_slug,
            status_type=status_type,
            detail_id=detail_id,
            has_xg=has_xg,
            has_event_player_heat_map=has_event_player_heat_map,
            has_event_player_statistics=has_event_player_statistics,
            has_global_highlights=has_global_highlights,
            start_timestamp=start_timestamp,
            now_timestamp=now_timestamp,
            is_editor=is_editor,
        )

    if normalized_sport_slug != "tennis":
        add(EVENT_COMMENTS_ENDPOINT)
    if normalized_sport_slug == "football":
        add(EVENT_OFFICIAL_TWEETS_ENDPOINT)
    if not core_only and normalized_sport_slug == "football" and has_event_player_statistics is True:
        add(EVENT_BEST_PLAYERS_SUMMARY_ENDPOINT)
    if football_highlights_allowed(
        sport_slug=normalized_sport_slug,
        detail_id=detail_id,
        status_type=status_type,
        has_global_highlights=has_global_highlights,
        start_timestamp=start_timestamp,
        now_timestamp=now_timestamp,
        is_editor=is_editor,
    ):
        add(EVENT_HIGHLIGHTS_ENDPOINT)

    if "tennis_point_by_point" in adapter.special_families:
        add(EVENT_POINT_BY_POINT_ENDPOINT)
    if "tennis_power" in adapter.special_families:
        add(EVENT_TENNIS_POWER_ENDPOINT)
    if "tennis_point_by_point" in adapter.special_families or "tennis_power" in adapter.special_families:
        return _filter_specs(
            tuple(deduped),
            sport_slug=normalized_sport_slug,
            status_type=status_type,
            detail_id=detail_id,
            has_xg=has_xg,
            has_event_player_heat_map=has_event_player_heat_map,
            has_event_player_statistics=has_event_player_statistics,
            has_global_highlights=has_global_highlights,
            start_timestamp=start_timestamp,
            now_timestamp=now_timestamp,
            is_editor=is_editor,
        )

    add(EVENT_GRAPH_ENDPOINT)
    if normalized_sport_slug == "football":
        add(EVENT_AVERAGE_POSITIONS_ENDPOINT)

    if not core_only and has_event_player_heat_map is True:
        for team_id in _dedupe_ints(team_ids):
            add(EVENT_HEATMAP_ENDPOINT, team_id=team_id)

    if not core_only and (has_xg is True or "shotmap" in adapter.special_families):
        add(EVENT_SHOTMAP_ENDPOINT)

    return _filter_specs(
        tuple(deduped),
        sport_slug=normalized_sport_slug,
        status_type=status_type,
        detail_id=detail_id,
        has_xg=has_xg,
        has_event_player_heat_map=has_event_player_heat_map,
        has_event_player_statistics=has_event_player_statistics,
        has_global_highlights=has_global_highlights,
        start_timestamp=start_timestamp,
        now_timestamp=now_timestamp,
        is_editor=is_editor,
    )


def _dedupe_ints(values: tuple[int, ...] | list[int]) -> tuple[int, ...]:
    deduped: list[int] = []
    for value in values:
        if not isinstance(value, int) or value in deduped:
            continue
        deduped.append(value)
    return tuple(deduped)


def _filter_specs(
    specs: tuple[EventDetailRequestSpec, ...],
    *,
    sport_slug: str | None,
    status_type: str | None,
    detail_id: int | None,
    has_xg: bool | None,
    has_event_player_heat_map: bool | None,
    has_event_player_statistics: bool | None,
    has_global_highlights: bool | None,
    start_timestamp: int | None,
    now_timestamp: int | None,
    is_editor: bool | None = None,
) -> tuple[EventDetailRequestSpec, ...]:
    return filter_football_detail_specs(
        specs,
        sport_slug=sport_slug,
        detail_id=detail_id,
        status_type=status_type,
        has_xg=has_xg,
        has_event_player_heat_map=has_event_player_heat_map,
        has_event_player_statistics=has_event_player_statistics,
        has_global_highlights=has_global_highlights,
        start_timestamp=start_timestamp,
        now_timestamp=now_timestamp,
        is_editor=is_editor,
    )
