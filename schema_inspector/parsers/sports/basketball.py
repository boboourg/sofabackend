"""Basketball adapter metadata."""

SPORT_ADAPTER = {
    "sport_slug": "basketball",
    "archetype": "regular_season_team",
    "core_event_edges": ("meta", "statistics", "lineups", "incidents"),
    "live_optional_edges": ("graph",),
    "special_families": (),
    "hydrate_entity_profiles": True,
}
