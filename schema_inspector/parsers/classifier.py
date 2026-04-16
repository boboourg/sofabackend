"""Snapshot classification for the replayable parser registry."""

from __future__ import annotations

from .base import RawSnapshot


def is_soft_error_payload(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    return isinstance(payload.get("error"), dict)


def classify_snapshot(snapshot: RawSnapshot) -> str:
    pattern = snapshot.endpoint_pattern
    root_keys = snapshot.observed_root_keys

    if pattern == "/api/v1/event/{event_id}" or "event" in root_keys:
        return "event_root"
    if pattern == "/api/v1/event/{event_id}/lineups":
        return "event_lineups"
    if pattern == "/api/v1/event/{event_id}/incidents":
        return "event_incidents"
    if pattern == "/api/v1/event/{event_id}/statistics":
        return "event_statistics"
    if pattern in {
        "/api/v1/team/{team_id}",
        "/api/v1/player/{player_id}",
        "/api/v1/manager/{manager_id}",
    }:
        return "entity_profiles"
    if pattern == "/api/v1/event/{event_id}/point-by-point":
        return "tennis_point_by_point"
    if pattern == "/api/v1/event/{event_id}/tennis-power":
        return "tennis_power"
    if pattern.endswith("/statistics/info"):
        return "season_info"
    if pattern.endswith("/standings/{scope}"):
        return "season_standings"
    if pattern.endswith("/statistics"):
        return "entity_season_statistics"
    return "unknown"
