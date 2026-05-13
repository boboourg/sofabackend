"""Sport-aware endpoint policy for recurring live polling."""

from __future__ import annotations

from .endpoints import (
    EVENT_ESPORTS_GAMES_ENDPOINT,
    EVENT_INNINGS_ENDPOINT,
    EVENT_POINT_BY_POINT_ENDPOINT,
    EVENT_SHOTMAP_ENDPOINT,
    EVENT_TENNIS_POWER_ENDPOINT,
    SofascoreEndpoint,
)


LIVE_DELTA_EDGE_KINDS: dict[str, tuple[str, ...]] = {
    "american-football": ("meta", "incidents", "statistics"),
    "baseball": ("meta", "statistics", "lineups"),
    "basketball": ("meta", "statistics", "lineups", "incidents", "graph"),
    "cricket": ("meta", "lineups", "incidents"),
    "esports": ("meta", "lineups"),
    "football": ("meta", "statistics", "lineups", "incidents", "graph"),
    "futsal": ("meta", "incidents", "statistics"),
    "handball": ("meta", "statistics", "lineups", "incidents", "graph"),
    "ice-hockey": ("meta", "statistics", "lineups", "incidents"),
    "rugby": ("meta", "statistics", "lineups", "incidents"),
    "table-tennis": ("meta", "statistics"),
    "tennis": ("meta", "statistics"),
    "volleyball": ("meta", "statistics", "incidents"),
}

LIVE_DELTA_DETAIL_ENDPOINTS: dict[str, tuple[SofascoreEndpoint, ...]] = {
    # P0.2: /innings moved baseball → cricket (live probe confirmed
    # baseball /innings is 100% soft-error 404). Cricket events get the
    # innings live-delta hook; baseball keeps only at-bats/pitches via
    # event_detail_endpoints.
    "cricket": (EVENT_INNINGS_ENDPOINT,),
    "esports": (EVENT_ESPORTS_GAMES_ENDPOINT,),
    "ice-hockey": (EVENT_SHOTMAP_ENDPOINT,),
    "tennis": (EVENT_POINT_BY_POINT_ENDPOINT, EVENT_TENNIS_POWER_ENDPOINT),
    # X4 (2026-05-13): football has NO entry here — instead falls through to
    # the full matchcenter spec path in ``detail_resource_policy.build_event_detail_request_specs``.
    # That gives parameter-aware fanout (per-provider odds, per-team heatmap)
    # which a flat list of SofascoreEndpoint cannot express, AND ensures
    # the full football_detail_endpoint_allowed gate fires per-spec.
    # Prior to X4 football live_delta was effectively a no-op → "live matches
    # have no data, data only appears after match finishes". Production probe
    # matrix (event_endpoint_availability_log, 7d) confirmed 25 endpoints are
    # 89-100% useful during inprogress phase for non-editor football events.
}


def live_delta_edge_kinds(sport_slug: str | None) -> tuple[str, ...]:
    normalized = _normalize_sport_slug(sport_slug)
    return LIVE_DELTA_EDGE_KINDS.get(normalized, ("meta",))


def live_delta_detail_endpoints(sport_slug: str | None) -> tuple[SofascoreEndpoint, ...]:
    normalized = _normalize_sport_slug(sport_slug)
    return LIVE_DELTA_DETAIL_ENDPOINTS.get(normalized, ())


def live_delta_detail_endpoint_patterns(sport_slug: str | None) -> tuple[str, ...]:
    return tuple(endpoint.path_template for endpoint in live_delta_detail_endpoints(sport_slug))


def _normalize_sport_slug(sport_slug: str | None) -> str:
    return str(sport_slug or "").strip().lower()
