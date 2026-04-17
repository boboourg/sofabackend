"""Esports adapter metadata."""

SPORT_ADAPTER = {
    "sport_slug": "esports",
    "archetype": "thin_special",
    "core_event_edges": ("meta", "statistics", "lineups"),
    "live_optional_edges": (),
    "special_families": ("esports_games",),
    "hydrate_entity_profiles": False,
}
