"""Tennis adapter metadata."""

SPORT_ADAPTER = {
    "sport_slug": "tennis",
    "archetype": "racket_special",
    "core_event_edges": ("meta", "statistics", "lineups", "incidents"),
    "live_optional_edges": (),
    "special_families": ("tennis_point_by_point", "tennis_power"),
    "hydrate_entity_profiles": False,
}
