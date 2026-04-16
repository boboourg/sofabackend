"""Planner rule helpers for job expansion and capability-aware gating."""

from __future__ import annotations

from typing import Iterable

from ..jobs.types import (
    JOB_FINALIZE_EVENT,
    JOB_HYDRATE_EVENT_EDGE,
    JOB_SYNC_SEASON_WIDGET,
    JOB_TRACK_LIVE_EVENT,
)
from ..parsers.sports import resolve_sport_adapter
from ..sport_profiles import resolve_sport_profile

def event_edge_candidates(*, sport_slug: str | None, status_type: str | None) -> tuple[str, ...]:
    adapter = resolve_sport_adapter(str(sport_slug or ""))
    normalized = str(status_type or "").strip().lower()
    if normalized in {"inprogress", "live"}:
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
    planned = []
    for edge_kind in event_edge_candidates(sport_slug=job.sport_slug, status_type=status_type):
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
    if status_type in {"inprogress", "live"}:
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
    if status_type in {"finished", "afterextra", "afterpen"}:
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


def season_widget_jobs(*, sport_slug: str, unique_tournament_id: int, season_id: int) -> tuple[object, ...]:
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

    planned = []
    if profile.top_players_suffix:
        planned.append(
            seed.spawn_child(
                job_type=JOB_SYNC_SEASON_WIDGET,
                entity_type="season",
                entity_id=season_id,
                scope="season",
                params={
                    "widget_kind": "top_players",
                    "suffix": profile.top_players_suffix,
                    "unique_tournament_id": unique_tournament_id,
                    "season_id": season_id,
                },
                priority=2,
            )
        )
    if profile.top_players_per_game_suffix:
        planned.append(
            seed.spawn_child(
                job_type=JOB_SYNC_SEASON_WIDGET,
                entity_type="season",
                entity_id=season_id,
                scope="season",
                params={
                    "widget_kind": "top_players_per_game",
                    "suffix": profile.top_players_per_game_suffix,
                    "unique_tournament_id": unique_tournament_id,
                    "season_id": season_id,
                },
                priority=2,
            )
        )
    if profile.top_teams_suffix:
        planned.append(
            seed.spawn_child(
                job_type=JOB_SYNC_SEASON_WIDGET,
                entity_type="season",
                entity_id=season_id,
                scope="season",
                params={
                    "widget_kind": "top_teams",
                    "suffix": profile.top_teams_suffix,
                    "unique_tournament_id": unique_tournament_id,
                    "season_id": season_id,
                },
                priority=2,
            )
        )
    if profile.include_player_of_the_season:
        planned.append(
            seed.spawn_child(
                job_type=JOB_SYNC_SEASON_WIDGET,
                entity_type="season",
                entity_id=season_id,
                scope="season",
                params={
                    "widget_kind": "player_of_the_season",
                    "unique_tournament_id": unique_tournament_id,
                    "season_id": season_id,
                },
                priority=2,
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
