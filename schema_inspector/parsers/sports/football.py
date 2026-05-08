"""Football adapter metadata."""

SPORT_ADAPTER = {
    "sport_slug": "football",
    "archetype": "football_like",
    "core_event_edges": ("meta", "statistics", "lineups", "incidents"),
    "live_optional_edges": ("graph",),
    "special_families": (),
    "hydrate_entity_profiles": True,
    # F-3 (2026-05-08): re-fetched once at the final sweep so finished
    # matches end up with the canonical post-match versions of these
    # endpoints rather than their bootstrap (notstarted) state.
    # NOT included in live_delta_edge_kinds → not polled during live play.
    "final_only_edges": ("comments", "best_players_summary", "h2h", "pregame_form"),
}
