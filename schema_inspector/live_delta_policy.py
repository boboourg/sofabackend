"""Sport-aware endpoint policy for recurring live polling."""

from __future__ import annotations

from .endpoints import (
    EVENT_BASEBALL_INNINGS_ENDPOINT,
    EVENT_ESPORTS_GAMES_ENDPOINT,
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
    "baseball": (EVENT_BASEBALL_INNINGS_ENDPOINT,),
    "esports": (EVENT_ESPORTS_GAMES_ENDPOINT,),
    "ice-hockey": (EVENT_SHOTMAP_ENDPOINT,),
    "tennis": (EVENT_POINT_BY_POINT_ENDPOINT, EVENT_TENNIS_POWER_ENDPOINT),
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
