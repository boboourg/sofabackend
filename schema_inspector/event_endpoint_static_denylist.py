"""Static sport+endpoint deny-list for known-dead event routes.

This is an intentionally manual quick-fix layer backed by a point-in-time
observation snapshot from endpoint_capability_observation as of 2026-04-30.
Update the frozenset in-place when we want to add or remove combinations.
"""

from __future__ import annotations


STATIC_DEAD_EVENT_ENDPOINTS = frozenset(
    {
        ("baseball", "/api/v1/event/{event_id}/graph"),
        ("baseball", "/api/v1/event/{event_id}/managers"),
        ("basketball", "/api/v1/event/{event_id}/comments"),
        ("cricket", "/api/v1/event/{event_id}/comments"),
        ("cricket", "/api/v1/event/{event_id}/graph"),
        ("cricket", "/api/v1/event/{event_id}/managers"),
        ("esports", "/api/v1/event/{event_id}/comments"),
        ("esports", "/api/v1/event/{event_id}/graph"),
        ("esports", "/api/v1/event/{event_id}/managers"),
        ("esports", "/api/v1/event/{event_id}/odds/{provider_id}/all"),
        ("esports", "/api/v1/event/{event_id}/odds/{provider_id}/featured"),
        ("esports", "/api/v1/event/{event_id}/pregame-form"),
        ("esports", "/api/v1/event/{event_id}/provider/{provider_id}/winning-odds"),
        ("futsal", "/api/v1/event/{event_id}/comments"),
        ("futsal", "/api/v1/event/{event_id}/graph"),
        ("futsal", "/api/v1/event/{event_id}/provider/{provider_id}/winning-odds"),
        ("handball", "/api/v1/event/{event_id}/comments"),
        ("ice-hockey", "/api/v1/event/{event_id}/graph"),
        ("rugby", "/api/v1/event/{event_id}/comments"),
        ("rugby", "/api/v1/event/{event_id}/graph"),
        ("table-tennis", "/api/v1/event/{event_id}/comments"),
        ("table-tennis", "/api/v1/event/{event_id}/graph"),
        ("table-tennis", "/api/v1/event/{event_id}/managers"),
        ("table-tennis", "/api/v1/event/{event_id}/pregame-form"),
        ("tennis", "/api/v1/event/{event_id}/comments"),
        ("tennis", "/api/v1/event/{event_id}/managers"),
        ("tennis", "/api/v1/event/{event_id}/pregame-form"),
        ("volleyball", "/api/v1/event/{event_id}/comments"),
        ("volleyball", "/api/v1/event/{event_id}/graph"),
        ("volleyball", "/api/v1/event/{event_id}/managers"),
    }
)


def is_static_dead_event_endpoint(sport_slug: str | None, endpoint_pattern: str | None) -> bool:
    normalized_sport = str(sport_slug or "").strip().lower()
    normalized_pattern = str(endpoint_pattern or "").strip()
    if not normalized_sport or not normalized_pattern:
        return False
    return (normalized_sport, normalized_pattern) in STATIC_DEAD_EVENT_ENDPOINTS


__all__ = ["STATIC_DEAD_EVENT_ENDPOINTS", "is_static_dead_event_endpoint"]
