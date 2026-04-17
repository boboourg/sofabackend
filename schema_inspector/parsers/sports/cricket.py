"""Cricket adapter metadata."""

SPORT_ADAPTER = {
    "sport_slug": "cricket",
    "archetype": "thin_special",
    "core_event_edges": ("meta", "lineups", "incidents"),
    "live_optional_edges": (),
    "special_families": (),
    "hydrate_entity_profiles": False,
}
