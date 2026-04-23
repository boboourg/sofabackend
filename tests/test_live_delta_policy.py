from __future__ import annotations

from schema_inspector.live_delta_policy import (
    LIVE_DELTA_EDGE_KINDS,
    live_delta_detail_endpoint_patterns,
    live_delta_edge_kinds,
)


def test_football_live_delta_edges_are_score_stats_lineups_incidents_graph() -> None:
    assert live_delta_edge_kinds("football") == ("meta", "statistics", "lineups", "incidents", "graph")
    assert live_delta_detail_endpoint_patterns("football") == ()


def test_tennis_live_delta_uses_point_feed_and_tennis_power() -> None:
    assert live_delta_edge_kinds("tennis") == ("meta", "statistics")
    assert live_delta_detail_endpoint_patterns("tennis") == (
        "/api/v1/event/{event_id}/point-by-point",
        "/api/v1/event/{event_id}/tennis-power",
    )


def test_baseball_live_delta_includes_innings_detail_endpoint() -> None:
    assert live_delta_edge_kinds("baseball") == ("meta", "statistics", "lineups")
    assert live_delta_detail_endpoint_patterns("baseball") == ("/api/v1/event/{event_id}/innings",)


def test_all_13_sports_have_explicit_policy() -> None:
    assert set(LIVE_DELTA_EDGE_KINDS) == {
        "american-football",
        "baseball",
        "basketball",
        "cricket",
        "esports",
        "football",
        "futsal",
        "handball",
        "ice-hockey",
        "rugby",
        "table-tennis",
        "tennis",
        "volleyball",
    }
