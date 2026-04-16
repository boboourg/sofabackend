"""Ice hockey adapter metadata."""

SPORT_ADAPTER = {
    "sport_slug": "ice-hockey",
    "archetype": "regular_season_team",
    "core_event_edges": ("meta", "statistics", "lineups", "incidents"),
    "live_optional_edges": (),
    "special_families": ("shotmap",),
    "hydrate_entity_profiles": True,
}
