"""Baseball adapter metadata."""

SPORT_ADAPTER = {
    "sport_slug": "baseball",
    "archetype": "regular_season_team",
    "core_event_edges": ("meta", "statistics", "lineups"),
    "live_optional_edges": (),
    "special_families": ("baseball_innings",),
    "hydrate_entity_profiles": True,
}
