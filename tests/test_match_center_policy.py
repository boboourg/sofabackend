from schema_inspector.match_center_policy import (
    football_detail_endpoint_allowed,
    football_edge_allowed,
    football_highlights_allowed,
    football_special_allowed,
)


def test_football_null_detail_blocks_child_edges() -> None:
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="statistics",
        detail_id=None,
        status_type="inprogress",
        has_xg=True,
    ) is False
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="incidents",
        detail_id=None,
        status_type="finished",
        has_xg=None,
    ) is False


def test_football_notstarted_only_keeps_safe_core_edges() -> None:
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="lineups",
        detail_id=1,
        status_type="scheduled",
        has_xg=True,
    ) is True
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="incidents",
        detail_id=1,
        status_type="notstarted",
        has_xg=True,
    ) is True
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="statistics",
        detail_id=1,
        status_type="scheduled",
        has_xg=True,
    ) is False
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="graph",
        detail_id=1,
        status_type="scheduled",
        has_xg=True,
    ) is False


def test_football_problem_detail_keeps_only_incidents() -> None:
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="incidents",
        detail_id=3,
        status_type="finished",
        has_xg=None,
    ) is True
    assert football_edge_allowed(
        sport_slug="football",
        edge_kind="lineups",
        detail_id=3,
        status_type="finished",
        has_xg=None,
    ) is False


def test_football_tier_one_allows_analytics_with_flags() -> None:
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}/shotmap",
        detail_id=1,
        status_type="inprogress",
        has_xg=True,
        has_event_player_heat_map=False,
        has_event_player_statistics=True,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is True
    assert football_special_allowed(
        sport_slug="football",
        special_kind="event_player_rating_breakdown",
        detail_id=1,
        has_event_player_statistics=False,
        has_event_player_heat_map=False,
        has_xg=False,
    ) is True


def test_football_notstarted_detail_routes_follow_matrix() -> None:
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}/h2h",
        detail_id=1,
        status_type="scheduled",
        has_xg=True,
        has_event_player_heat_map=True,
        has_event_player_statistics=True,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is True
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}/graph",
        detail_id=1,
        status_type="scheduled",
        has_xg=True,
        has_event_player_heat_map=True,
        has_event_player_statistics=True,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is False


def test_football_custom_id_and_team_streak_routes_are_allowed_pre_match() -> None:
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{custom_id}/h2h/events",
        detail_id=1,
        status_type="scheduled",
        has_xg=False,
        has_event_player_heat_map=False,
        has_event_player_statistics=False,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is True
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}/team-streaks",
        detail_id=4,
        status_type="scheduled",
        has_xg=False,
        has_event_player_heat_map=False,
        has_event_player_statistics=False,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is True


def test_football_tier_two_blocks_comments_but_keeps_safe_routes() -> None:
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}/comments",
        detail_id=4,
        status_type="finished",
        has_xg=True,
        has_event_player_heat_map=True,
        has_event_player_statistics=True,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is False
    assert football_detail_endpoint_allowed(
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}/graph",
        detail_id=4,
        status_type="finished",
        has_xg=True,
        has_event_player_heat_map=True,
        has_event_player_statistics=True,
        has_global_highlights=False,
        start_timestamp=1_000,
        now_timestamp=20_000,
    ) is True


def test_football_highlights_wait_until_finished_plus_one_nominal_hour() -> None:
    assert football_highlights_allowed(
        sport_slug="football",
        detail_id=1,
        status_type="finished",
        has_global_highlights=True,
        start_timestamp=1_000,
        now_timestamp=1_000 + 149 * 60,
    ) is False
    assert football_highlights_allowed(
        sport_slug="football",
        detail_id=1,
        status_type="finished",
        has_global_highlights=True,
        start_timestamp=1_000,
        now_timestamp=1_000 + 150 * 60,
    ) is True


def test_highlights_gate_is_football_only() -> None:
    assert football_highlights_allowed(
        sport_slug="tennis",
        detail_id=1,
        status_type="finished",
        has_global_highlights=True,
        start_timestamp=1_000,
        now_timestamp=1_000 + 150 * 60,
    ) is False


def test_football_special_gates_require_matching_capabilities() -> None:
    assert football_special_allowed(
        sport_slug="football",
        special_kind="event_player_heatmap",
        detail_id=1,
        has_event_player_statistics=True,
        has_event_player_heat_map=True,
        has_xg=False,
    ) is True
    assert football_special_allowed(
        sport_slug="football",
        special_kind="event_player_heatmap",
        detail_id=1,
        has_event_player_statistics=True,
        has_event_player_heat_map=False,
        has_xg=False,
    ) is False
    assert football_special_allowed(
        sport_slug="football",
        special_kind="event_player_shotmap",
        detail_id=1,
        has_event_player_statistics=True,
        has_event_player_heat_map=True,
        has_xg=True,
    ) is True
    assert football_special_allowed(
        sport_slug="football",
        special_kind="event_goalkeeper_shotmap",
        detail_id=4,
        has_event_player_statistics=True,
        has_event_player_heat_map=True,
        has_xg=False,
    ) is False
