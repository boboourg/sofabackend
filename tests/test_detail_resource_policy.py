import unittest

from schema_inspector.detail_resource_policy import build_event_detail_request_specs
from schema_inspector.parsers.sports import resolve_sport_adapter


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


class EventDetailResourcePolicyTests(unittest.TestCase):
    def test_inprogress_football_full_mode_includes_extended_dynamic_routes(self) -> None:
        specs = build_event_detail_request_specs(
            sport_slug="football",
            status_type="inprogress",
            team_ids=(3002, 3001),
            provider_ids=(1,),
            has_event_player_statistics=True,
            has_event_player_heat_map=True,
            has_global_highlights=True,
            has_xg=True,
            detail_id=1,
            start_timestamp=1_000,
            now_timestamp=20_000,
            core_only=False,
        )

        resolved = {(item.endpoint.pattern, tuple(sorted(item.path_params.items()))) for item in specs}

        self.assertIn(("/api/v1/event/{event_id}/managers", ()), resolved)
        self.assertIn(("/api/v1/event/{event_id}/h2h", ()), resolved)
        self.assertIn(("/api/v1/event/{event_id}/pregame-form", ()), resolved)
        self.assertIn(("/api/v1/event/{event_id}/votes", ()), resolved)
        self.assertIn(("/api/v1/event/{event_id}/comments", ()), resolved)
        self.assertIn(("/api/v1/event/{event_id}/best-players/summary", ()), resolved)
        self.assertIn(("/api/v1/event/{event_id}/graph", ()), resolved)
        self.assertNotIn(("/api/v1/event/{event_id}/highlights", ()), resolved)
        self.assertIn(("/api/v1/event/{event_id}/shotmap", ()), resolved)
        self.assertIn(
            ("/api/v1/event/{event_id}/heatmap/{team_id}", (("team_id", 3001),)),
            resolved,
        )
        self.assertIn(
            ("/api/v1/event/{event_id}/heatmap/{team_id}", (("team_id", 3002),)),
            resolved,
        )
        self.assertIn(
            ("/api/v1/event/{event_id}/odds/{provider_id}/all", (("provider_id", 1),)),
            resolved,
        )
        self.assertIn(
            ("/api/v1/event/{event_id}/odds/{provider_id}/featured", (("provider_id", 1),)),
            resolved,
        )
        self.assertIn(
            ("/api/v1/event/{event_id}/provider/{provider_id}/winning-odds", (("provider_id", 1),)),
            resolved,
        )

    def test_notstarted_basketball_skips_live_only_routes(self) -> None:
        specs = build_event_detail_request_specs(
            sport_slug="basketball",
            status_type="notstarted",
            team_ids=(10, 11),
            provider_ids=(1,),
            has_event_player_statistics=True,
            has_event_player_heat_map=True,
            has_global_highlights=True,
            has_xg=True,
            core_only=False,
        )

        patterns = {item.endpoint.pattern for item in specs}

        self.assertIn("/api/v1/event/{event_id}/managers", patterns)
        self.assertIn("/api/v1/event/{event_id}/h2h", patterns)
        self.assertIn("/api/v1/event/{event_id}/votes", patterns)
        self.assertIn("/api/v1/event/{event_id}/odds/{provider_id}/all", patterns)
        self.assertIn("/api/v1/event/{event_id}/highlights", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/comments", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/best-players/summary", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/graph", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/heatmap/{team_id}", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/shotmap", patterns)

    def test_inprogress_table_tennis_skips_dead_match_center_routes(self) -> None:
        specs = build_event_detail_request_specs(
            sport_slug="table-tennis",
            status_type="inprogress",
            provider_ids=(1,),
            has_global_highlights=False,
            core_only=False,
        )

        patterns = {item.endpoint.pattern for item in specs}

        self.assertIn("/api/v1/event/{event_id}/h2h", patterns)
        self.assertIn("/api/v1/event/{event_id}/votes", patterns)
        self.assertIn("/api/v1/event/{event_id}/odds/{provider_id}/all", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/managers", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/pregame-form", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/comments", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/graph", patterns)

    def test_finished_tennis_uses_tennis_specific_match_center_routes(self) -> None:
        specs = build_event_detail_request_specs(
            sport_slug="tennis",
            status_type="finished",
            team_ids=(199527, 199528),
            provider_ids=(1,),
            has_event_player_statistics=False,
            has_event_player_heat_map=False,
            has_global_highlights=True,
            has_xg=False,
            custom_id="vGHbsytkc",
            core_only=False,
        )

        resolved = {(item.endpoint.pattern, tuple(sorted(item.path_params.items()))) for item in specs}
        patterns = {pattern for pattern, _ in resolved}

        self.assertIn(
            ("/api/v1/event/{custom_id}/h2h/events", (("custom_id", "vGHbsytkc"),)),
            resolved,
        )
        self.assertIn("/api/v1/event/{event_id}/odds/{provider_id}/featured", patterns)
        self.assertIn("/api/v1/event/{event_id}/odds/{provider_id}/all", patterns)
        self.assertIn("/api/v1/event/{event_id}/provider/{provider_id}/winning-odds", patterns)
        self.assertIn("/api/v1/event/{event_id}/highlights", patterns)
        self.assertIn("/api/v1/event/{event_id}/team-streaks", patterns)
        self.assertIn("/api/v1/event/{event_id}/point-by-point", patterns)
        self.assertIn("/api/v1/event/{event_id}/tennis-power", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/comments", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/managers", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/pregame-form", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/votes", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/graph", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/heatmap/{team_id}", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/shotmap", patterns)

    def test_notstarted_tennis_uses_prematch_matrix_without_live_point_feeds(self) -> None:
        specs = build_event_detail_request_specs(
            sport_slug="tennis",
            status_type="notstarted",
            provider_ids=(1,),
            has_global_highlights=False,
            custom_id="vGHbsytkc",
            core_only=False,
        )

        resolved = {(item.endpoint.pattern, tuple(sorted(item.path_params.items()))) for item in specs}
        patterns = {pattern for pattern, _ in resolved}

        self.assertIn(
            ("/api/v1/event/{custom_id}/h2h/events", (("custom_id", "vGHbsytkc"),)),
            resolved,
        )
        self.assertIn("/api/v1/event/{event_id}/odds/{provider_id}/featured", patterns)
        self.assertIn("/api/v1/event/{event_id}/odds/{provider_id}/all", patterns)
        self.assertIn("/api/v1/event/{event_id}/provider/{provider_id}/winning-odds", patterns)
        self.assertIn("/api/v1/event/{event_id}/team-streaks", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/point-by-point", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/tennis-power", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/comments", patterns)

    def test_tennis_adapter_core_edges_are_root_and_statistics_only(self) -> None:
        adapter = resolve_sport_adapter("tennis")

        self.assertEqual(adapter.core_event_edges, ("meta", "statistics"))

    def test_core_mode_keeps_lightweight_live_routes_only(self) -> None:
        specs = build_event_detail_request_specs(
            sport_slug="football",
            status_type="inprogress",
            team_ids=(3002, 3001),
            provider_ids=(1,),
            has_event_player_statistics=True,
            has_event_player_heat_map=True,
            has_global_highlights=True,
            has_xg=True,
            detail_id=1,
            core_only=True,
        )

        patterns = {item.endpoint.pattern for item in specs}

        self.assertIn("/api/v1/event/{event_id}/comments", patterns)
        self.assertIn("/api/v1/event/{event_id}/graph", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/best-players/summary", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/highlights", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/managers", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/heatmap/{team_id}", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/shotmap", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/odds/{provider_id}/all", patterns)

    def test_live_delta_football_falls_through_to_full_matchcenter_path(self) -> None:
        """X4 (2026-05-13) — football in live_delta mode must fetch the full
        matchcenter spec set, not the empty live_delta_detail_endpoints tuple.

        Prior to X4 `live_delta_detail_endpoints["football"] = ()` and the
        live_delta branch returned `()` for football → live polling fetched
        only ROOT + 5 core edges → matchcenter populated only at final_sweep
        → user-visible "live matches have no data, appears after match ends".

        Post-X4 football falls through to the regular full-detail spec path
        where per-tier filtering by `football_detail_endpoint_allowed`
        decides what's actually allowed. For tier_1 (detail_id=1), the full
        matchcenter set is allowed during inprogress.
        """
        specs = build_event_detail_request_specs(
            sport_slug="football",
            status_type="inprogress",
            team_ids=(2820, 2836),
            provider_ids=(1,),
            has_event_player_statistics=True,
            has_event_player_heat_map=True,
            has_global_highlights=False,
            has_xg=True,
            detail_id=1,  # tier_1
            custom_id="abc",
            hydration_mode="live_delta",
        )
        patterns = {item.endpoint.pattern for item in specs}
        # Pre-match-style detail bundle MUST be in the spec list for tier_1 inprogress.
        for required in (
            "/api/v1/event/{event_id}/managers",
            "/api/v1/event/{event_id}/h2h",
            "/api/v1/event/{event_id}/pregame-form",
            "/api/v1/event/{event_id}/votes",
            "/api/v1/event/{event_id}/odds/{provider_id}/all",
            "/api/v1/event/{event_id}/odds/{provider_id}/featured",
            "/api/v1/event/{event_id}/provider/{provider_id}/winning-odds",
            "/api/v1/event/{event_id}/team-streaks",
            "/api/v1/event/{event_id}/team-streaks/betting-odds/{provider_id}",
            "/api/v1/event/{custom_id}/h2h/events",
            # Live-detail block:
            "/api/v1/event/{event_id}/comments",
            "/api/v1/event/{event_id}/graph",
            "/api/v1/event/{event_id}/average-positions",
            "/api/v1/event/{event_id}/best-players/summary",
            "/api/v1/event/{event_id}/heatmap/{team_id}",
            "/api/v1/event/{event_id}/shotmap",
        ):
            self.assertIn(required, patterns, f"X4 must include {required!r} in football live_delta tier_1 spec")

    def test_live_delta_football_tier_5_falls_through_with_prematch_only(self) -> None:
        """X4 + X3 — football in live_delta + tier_5 (detail_id=None) still
        only gets the pre-match-style detail set, premium endpoints blocked."""
        specs = build_event_detail_request_specs(
            sport_slug="football",
            status_type="inprogress",
            team_ids=(2820, 2836),
            provider_ids=(1,),
            has_event_player_statistics=None,
            has_event_player_heat_map=None,
            has_global_highlights=None,
            has_xg=None,
            detail_id=None,  # tier_5 — null detailId
            hydration_mode="live_delta",
        )
        patterns = {item.endpoint.pattern for item in specs}
        # Pre-match-style detail bundle allowed for tier_5 (X3 unlock).
        for required in (
            "/api/v1/event/{event_id}/managers",
            "/api/v1/event/{event_id}/h2h",
            "/api/v1/event/{event_id}/pregame-form",
            "/api/v1/event/{event_id}/votes",
            "/api/v1/event/{event_id}/odds/{provider_id}/all",
            "/api/v1/event/{event_id}/team-streaks",
        ):
            self.assertIn(required, patterns, f"tier_5 must include {required!r}")
        # Premium endpoints blocked for tier_5.
        for blocked in (
            "/api/v1/event/{event_id}/comments",
            "/api/v1/event/{event_id}/graph",
            "/api/v1/event/{event_id}/heatmap/{team_id}",
            "/api/v1/event/{event_id}/shotmap",
            "/api/v1/event/{event_id}/best-players/summary",
            "/api/v1/event/{event_id}/average-positions",
        ):
            self.assertNotIn(blocked, patterns, f"tier_5 must block premium {blocked!r}")

    def test_live_delta_football_editor_returns_empty(self) -> None:
        """X3 HARD BAN still fires inside live_delta path for isEditor=true."""
        specs = build_event_detail_request_specs(
            sport_slug="football",
            status_type="inprogress",
            team_ids=(1, 2),
            provider_ids=(1,),
            detail_id=1,
            is_editor=True,
            hydration_mode="live_delta",
        )
        self.assertEqual(specs, ())

    def test_live_delta_football_notstarted_returns_empty(self) -> None:
        """live_delta is only meaningful for inprogress/finished events; for
        notstarted events live_delta must still return () (no live-detail)."""
        specs = build_event_detail_request_specs(
            sport_slug="football",
            status_type="notstarted",
            detail_id=1,
            hydration_mode="live_delta",
        )
        self.assertEqual(specs, ())

    def test_live_delta_tennis_fetches_only_live_point_feeds(self) -> None:
        specs = build_event_detail_request_specs(
            sport_slug="tennis",
            status_type="inprogress",
            team_ids=(199527, 199528),
            provider_ids=(1,),
            has_event_player_statistics=True,
            has_event_player_heat_map=True,
            has_global_highlights=True,
            has_xg=True,
            hydration_mode="live_delta",
        )

        patterns = {item.endpoint.pattern for item in specs}

        self.assertEqual(
            patterns,
            {
                "/api/v1/event/{event_id}/point-by-point",
                "/api/v1/event/{event_id}/tennis-power",
            },
        )

    def test_finished_football_without_capability_flags_skips_highlights_and_best_players_summary(self) -> None:
        specs = build_event_detail_request_specs(
            sport_slug="football",
            status_type="finished",
            team_ids=(3002, 3001),
            provider_ids=(1,),
            has_event_player_statistics=False,
            has_event_player_heat_map=False,
            has_global_highlights=False,
            has_xg=False,
            core_only=False,
        )

        patterns = {item.endpoint.pattern for item in specs}

        self.assertNotIn("/api/v1/event/{event_id}/best-players/summary", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/highlights", patterns)


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
