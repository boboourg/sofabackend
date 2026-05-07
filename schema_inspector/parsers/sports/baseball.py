"""Baseball adapter metadata."""

SPORT_ADAPTER = {
    "sport_slug": "baseball",
    "archetype": "regular_season_team",
    "core_event_edges": ("meta", "statistics", "lineups", "incidents"),
    "live_optional_edges": (),
    # P0.2: ``baseball_innings`` removed — /innings is cricket-only on prod.
    # Baseball events still get /at-bats and /atbat/{id}/pitches via
    # EVENT_DETAIL_BASEBALL_ENDPOINTS.
    "special_families": (),
    "hydrate_entity_profiles": True,
}
