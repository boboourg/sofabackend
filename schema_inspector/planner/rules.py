"""Planner rule helpers for job expansion and capability-aware gating."""

from __future__ import annotations

from typing import Iterable

from ..detail_resource_policy import supports_live_detail_resources
from ..endpoints import (
    UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT,
    unique_tournament_top_players_endpoint,
    unique_tournament_top_players_per_game_endpoint,
    unique_tournament_top_teams_endpoint,
)
from ..live_delta_policy import live_delta_edge_kinds
from ..jobs.types import (
    JOB_FINALIZE_EVENT,
    JOB_HYDRATE_EVENT_EDGE,
    JOB_HYDRATE_SPECIAL_ROUTE,
    JOB_SYNC_SEASON_WIDGET,
    JOB_TRACK_LIVE_EVENT,
)
from .live import ACTIVE_LIVE_STATUS_TYPES, TERMINAL_STATUS_TYPES
from ..parsers.sports import resolve_sport_adapter
from ..sport_profiles import resolve_sport_profile
from ..season_widget_negative_cache import widget_endpoint_pattern_from_params


def event_edge_candidates(
    *,
    sport_slug: str | None,
    status_type: str | None,
    hydration_mode: str | None = None,
) -> tuple[str, ...]:
    if str(hydration_mode or "").strip().lower() == "live_delta" and supports_live_detail_resources(status_type):
        return live_delta_edge_kinds(sport_slug)
    adapter = resolve_sport_adapter(str(sport_slug or ""))
    if supports_live_detail_resources(status_type):
        return adapter.core_event_edges + adapter.live_optional_edges
    return adapter.core_event_edges


def should_schedule_edge(edge_kind: str, capability_rollup: dict[str, str] | None, *, sport_slug: str | None) -> bool:
    adapter = resolve_sport_adapter(str(sport_slug or ""))
    if edge_kind in adapter.core_event_edges:
        return True
    if not capability_rollup:
        return True
    pattern = _edge_kind_pattern(edge_kind)
    if pattern is None:
        return True
    support_level = capability_rollup.get(pattern, "unknown")
    return support_level not in {"unsupported", "deprecated_candidate"}


def edge_jobs_for_event(job, capability_rollup: dict[str, str] | None) -> tuple[object, ...]:
    status_type = str(job.params.get("status_type") or "").strip().lower()
    hydration_mode = str(job.params.get("hydration_mode") or "").strip().lower()
    planned = []
    for edge_kind in event_edge_candidates(sport_slug=job.sport_slug, status_type=status_type, hydration_mode=hydration_mode):
        if not should_schedule_edge(edge_kind, capability_rollup, sport_slug=job.sport_slug):
            continue
        planned.append(
            job.spawn_child(
                job_type=JOB_HYDRATE_EVENT_EDGE,
                entity_type="event",
                entity_id=job.entity_id,
                scope=job.scope,
                params={"edge_kind": edge_kind},
                priority=0 if edge_kind in {"statistics", "incidents"} else 1,
            )
        )
    if status_type in ACTIVE_LIVE_STATUS_TYPES:
        planned.append(
            job.spawn_child(
                job_type=JOB_TRACK_LIVE_EVENT,
                entity_type="event",
                entity_id=job.entity_id,
                scope="live",
                params={"status_type": status_type},
                priority=0,
            )
        )
    if status_type in TERMINAL_STATUS_TYPES:
        planned.append(
            job.spawn_child(
                job_type=JOB_FINALIZE_EVENT,
                entity_type="event",
                entity_id=job.entity_id,
                scope="terminal",
                params={"status_type": status_type},
                priority=0,
            )
        )
    return tuple(planned)


def season_widget_jobs(
    *,
    sport_slug: str,
    unique_tournament_id: int,
    season_id: int,
    blocked_endpoint_patterns: tuple[str, ...] = (),
) -> tuple[object, ...]:
    from ..jobs.envelope import JobEnvelope

    profile = resolve_sport_profile(sport_slug)
    if not any(
        (
            profile.top_players_suffix,
            profile.top_players_per_game_suffix,
            profile.top_teams_suffix,
            profile.include_player_of_the_season,
        )
    ):
        return ()

    seed = JobEnvelope.create(
        job_type=JOB_SYNC_SEASON_WIDGET,
        sport_slug=sport_slug,
        entity_type="season",
        entity_id=season_id,
        scope="season",
        params={"unique_tournament_id": unique_tournament_id, "season_id": season_id},
        priority=2,
        trace_id=None,
    )

    blocked = {str(item) for item in blocked_endpoint_patterns}
    planned = []
    if profile.top_players_suffix:
        params = {
            "widget_kind": "top_players",
            "suffix": profile.top_players_suffix,
            "unique_tournament_id": unique_tournament_id,
            "season_id": season_id,
        }
        pattern = widget_endpoint_pattern_from_params(params)
        if pattern and pattern not in blocked:
            planned.append(
                seed.spawn_child(
                    job_type=JOB_SYNC_SEASON_WIDGET,
                    entity_type="season",
                    entity_id=season_id,
                    scope="season",
                    params={**params, "_endpoint_pattern": pattern},
                    priority=2,
                )
            )
    if profile.top_players_per_game_suffix:
        params = {
            "widget_kind": "top_players_per_game",
            "suffix": profile.top_players_per_game_suffix,
            "unique_tournament_id": unique_tournament_id,
            "season_id": season_id,
        }
        pattern = widget_endpoint_pattern_from_params(params)
        if pattern and pattern not in blocked:
            planned.append(
                seed.spawn_child(
                    job_type=JOB_SYNC_SEASON_WIDGET,
                    entity_type="season",
                    entity_id=season_id,
                    scope="season",
                    params={**params, "_endpoint_pattern": pattern},
                    priority=2,
                )
            )
    if profile.top_teams_suffix:
        params = {
            "widget_kind": "top_teams",
            "suffix": profile.top_teams_suffix,
            "unique_tournament_id": unique_tournament_id,
            "season_id": season_id,
        }
        pattern = widget_endpoint_pattern_from_params(params)
        if pattern and pattern not in blocked:
            planned.append(
                seed.spawn_child(
                    job_type=JOB_SYNC_SEASON_WIDGET,
                    entity_type="season",
                    entity_id=season_id,
                    scope="season",
                    params={**params, "_endpoint_pattern": pattern},
                    priority=2,
                )
            )
    if profile.include_player_of_the_season:
        params = {
            "widget_kind": "player_of_the_season",
            "unique_tournament_id": unique_tournament_id,
            "season_id": season_id,
        }
        pattern = widget_endpoint_pattern_from_params(params)
        if pattern and pattern not in blocked:
            planned.append(
                seed.spawn_child(
                    job_type=JOB_SYNC_SEASON_WIDGET,
                    entity_type="season",
                    entity_id=season_id,
                    scope="season",
                    params={**params, "_endpoint_pattern": pattern},
                    priority=2,
                )
            )
    return tuple(planned)


def season_widget_endpoint_patterns(
    *,
    sport_slug: str,
    unique_tournament_id: int,
    season_id: int,
) -> tuple[str, ...]:
    del unique_tournament_id, season_id
    profile = resolve_sport_profile(sport_slug)
    patterns: list[str] = []
    if profile.top_players_suffix:
        pattern = unique_tournament_top_players_endpoint(str(profile.top_players_suffix)).pattern
        patterns.append(pattern)
    if profile.top_players_per_game_suffix:
        pattern = unique_tournament_top_players_per_game_endpoint(str(profile.top_players_per_game_suffix)).pattern
        patterns.append(pattern)
    if profile.top_teams_suffix:
        pattern = unique_tournament_top_teams_endpoint(str(profile.top_teams_suffix)).pattern
        patterns.append(pattern)
    if profile.include_player_of_the_season:
        patterns.append(UNIQUE_TOURNAMENT_PLAYER_OF_THE_SEASON_ENDPOINT.pattern)
    return tuple(patterns)


def lineup_followup_jobs(job, parse_result, capability_rollup: dict[str, str] | None) -> tuple[object, ...]:
    if getattr(parse_result, "parser_family", None) != "event_lineups":
        return ()
    if str(job.sport_slug or "").strip().lower() != "football":
        return ()

    starters = sorted(
        {
            int(player_id)
            for row in parse_result.relation_upserts.get("event_lineup_player", ())
            for player_id in (row.get("player_id"),)
            if isinstance(player_id, int) and row.get("substitute") is False
        }
    )
    if not starters:
        return ()

    planned = []
    if _special_kind_supported("best_players_summary", capability_rollup):
        planned.append(
            job.spawn_child(
                job_type=JOB_HYDRATE_SPECIAL_ROUTE,
                entity_type="event",
                entity_id=job.entity_id,
                scope="event_player_analytics",
                params={"special_kind": "best_players_summary"},
                priority=1,
            )
        )
    for player_id in starters:
        if _special_kind_supported("event_player_statistics", capability_rollup):
            planned.append(
                job.spawn_child(
                    job_type=JOB_HYDRATE_SPECIAL_ROUTE,
                    entity_type="player",
                    entity_id=player_id,
                    scope="event_player_analytics",
                    params={
                        "special_kind": "event_player_statistics",
                        "event_id": job.entity_id,
                        "player_id": player_id,
                    },
                    priority=1,
                )
            )
        if _special_kind_supported("event_player_heatmap", capability_rollup):
            planned.append(
                job.spawn_child(
                    job_type=JOB_HYDRATE_SPECIAL_ROUTE,
                    entity_type="player",
                    entity_id=player_id,
                    scope="event_player_analytics",
                    params={
                        "special_kind": "event_player_heatmap",
                        "event_id": job.entity_id,
                        "player_id": player_id,
                    },
                    priority=1,
                )
            )
        if _special_kind_supported("event_player_rating_breakdown", capability_rollup):
            planned.append(
                job.spawn_child(
                    job_type=JOB_HYDRATE_SPECIAL_ROUTE,
                    entity_type="player",
                    entity_id=player_id,
                    scope="event_player_analytics",
                    params={
                        "special_kind": "event_player_rating_breakdown",
                        "event_id": job.entity_id,
                        "player_id": player_id,
                    },
                    priority=1,
                )
            )
    return tuple(planned)


def _edge_kind_pattern(edge_kind: str) -> str | None:
    mapping = {
        "graph": "/api/v1/event/{event_id}/graph",
        "meta": "/api/v1/event/{event_id}",
        "statistics": "/api/v1/event/{event_id}/statistics",
        "lineups": "/api/v1/event/{event_id}/lineups",
        "incidents": "/api/v1/event/{event_id}/incidents",
    }
    return mapping.get(edge_kind)


def _special_kind_supported(special_kind: str, capability_rollup: dict[str, str] | None) -> bool:
    if not capability_rollup:
        return True
    pattern = _special_kind_pattern(special_kind)
    if pattern is None:
        return True
    support_level = capability_rollup.get(pattern, "unknown")
    return support_level not in {"unsupported", "deprecated_candidate"}


def _special_kind_pattern(special_kind: str) -> str | None:
    mapping = {
        "best_players_summary": "/api/v1/event/{event_id}/best-players/summary",
        "event_player_statistics": "/api/v1/event/{event_id}/player/{player_id}/statistics",
        "event_player_heatmap": "/api/v1/event/{event_id}/player/{player_id}/heatmap",
        "event_player_rating_breakdown": "/api/v1/event/{event_id}/player/{player_id}/rating-breakdown",
        "event_player_shotmap": "/api/v1/event/{event_id}/shotmap/player/{player_id}",
        "event_goalkeeper_shotmap": "/api/v1/event/{event_id}/goalkeeper-shotmap/player/{player_id}",
    }
    return mapping.get(special_kind)
