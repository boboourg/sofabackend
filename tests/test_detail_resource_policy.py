from schema_inspector.detail_resource_policy import build_event_detail_request_specs


def test_football_detail_specs_include_custom_id_and_streak_routes() -> None:
    specs = build_event_detail_request_specs(
        sport_slug="football",
        status_type="scheduled",
        team_ids=(1, 2),
        provider_ids=(1,),
        has_event_player_heat_map=False,
        has_event_player_statistics=False,
        has_global_highlights=False,
        has_xg=False,
        detail_id=1,
        custom_id="uobsILo",
        start_timestamp=1_000,
        now_timestamp=2_000,
        core_only=False,
        hydration_mode="full",
    )
    patterns = {spec.endpoint.pattern for spec in specs}
    assert "/api/v1/event/{custom_id}/h2h/events" in patterns
    assert "/api/v1/event/{event_id}/team-streaks" in patterns
    assert "/api/v1/event/{event_id}/team-streaks/betting-odds/{provider_id}" in patterns


def test_custom_id_path_param_is_preserved_as_string() -> None:
    specs = build_event_detail_request_specs(
        sport_slug="football",
        status_type="scheduled",
        provider_ids=(1,),
        custom_id="uobsILo",
        detail_id=1,
        core_only=False,
        hydration_mode="full",
    )
    custom_spec = next(spec for spec in specs if spec.endpoint.pattern == "/api/v1/event/{custom_id}/h2h/events")
    resolved = custom_spec.resolved_path_params(event_id=15235535)
    assert resolved["custom_id"] == "uobsILo"


def test_football_live_detail_specs_include_average_positions_and_official_tweets() -> None:
    specs = build_event_detail_request_specs(
        sport_slug="football",
        status_type="inprogress",
        team_ids=(11, 22),
        provider_ids=(1,),
        has_event_player_heat_map=True,
        has_event_player_statistics=True,
        has_global_highlights=False,
        has_xg=True,
        detail_id=1,
        custom_id="uobsILo",
        start_timestamp=1_000,
        now_timestamp=20_000,
        core_only=False,
        hydration_mode="full",
    )
    patterns = {spec.endpoint.pattern for spec in specs}
    assert "/api/v1/event/{event_id}/official-tweets" in patterns
    assert "/api/v1/event/{event_id}/average-positions" in patterns
    assert "/api/v1/event/{event_id}/shotmap" in patterns
