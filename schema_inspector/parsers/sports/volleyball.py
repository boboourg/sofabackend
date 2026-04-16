"""Volleyball adapter metadata."""

SPORT_ADAPTER = {
    "sport_slug": "volleyball",
    "archetype": "football_like",
    "core_event_edges": ("meta", "statistics", "lineups", "incidents"),
    "live_optional_edges": (),
    "special_families": (),
    "hydrate_entity_profiles": True,
}
