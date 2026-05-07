"""Cricket adapter metadata."""

SPORT_ADAPTER = {
    "sport_slug": "cricket",
    "archetype": "thin_special",
    "core_event_edges": ("meta", "lineups", "incidents"),
    "live_optional_edges": (),
    # P0.2: cricket events get /innings via EVENT_DETAIL_CRICKET_ENDPOINTS
    # and the live-delta hook in LIVE_DELTA_DETAIL_ENDPOINTS["cricket"].
    "special_families": ("cricket_innings",),
    "hydrate_entity_profiles": False,
}
