"""Futsal adapter metadata."""

SPORT_ADAPTER = {
    "sport_slug": "futsal",
    "archetype": "football_like",
    "core_event_edges": ("meta", "statistics", "lineups", "incidents"),
    "live_optional_edges": ("graph",),
    "special_families": (),
    "hydrate_entity_profiles": True,
}
