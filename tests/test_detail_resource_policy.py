from __future__ import annotations

import unittest

from schema_inspector.detail_resource_policy import build_event_detail_request_specs


class EventDetailResourcePolicyTests(unittest.TestCase):
    def test_inprogress_football_full_mode_includes_extended_dynamic_routes(self) -> None:
        specs = build_event_detail_request_specs(
            sport_slug="football",
            status_type="inprogress",
            team_ids=(3002, 3001),
            provider_ids=(1,),
            has_event_player_heat_map=True,
            has_xg=True,
            core_only=False,
        )

        resolved = {(item.endpoint.pattern, tuple(sorted(item.path_params.items()))) for item in specs}

        self.assertIn(("/api/v1/event/{event_id}/managers", ()), resolved)
        self.assertIn(("/api/v1/event/{event_id}/h2h", ()), resolved)
        self.assertIn(("/api/v1/event/{event_id}/pregame-form", ()), resolved)
        self.assertIn(("/api/v1/event/{event_id}/votes", ()), resolved)
        self.assertIn(("/api/v1/event/{event_id}/comments", ()), resolved)
        self.assertIn(("/api/v1/event/{event_id}/graph", ()), resolved)
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
            has_event_player_heat_map=True,
            has_xg=True,
            core_only=False,
        )

        patterns = {item.endpoint.pattern for item in specs}

        self.assertIn("/api/v1/event/{event_id}/managers", patterns)
        self.assertIn("/api/v1/event/{event_id}/h2h", patterns)
        self.assertIn("/api/v1/event/{event_id}/votes", patterns)
        self.assertIn("/api/v1/event/{event_id}/odds/{provider_id}/all", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/comments", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/graph", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/heatmap/{team_id}", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/shotmap", patterns)

    def test_finished_tennis_uses_tennis_specific_live_detail_routes(self) -> None:
        specs = build_event_detail_request_specs(
            sport_slug="tennis",
            status_type="finished",
            team_ids=(199527, 199528),
            provider_ids=(),
            has_event_player_heat_map=False,
            has_xg=False,
            core_only=False,
        )

        patterns = {item.endpoint.pattern for item in specs}

        self.assertIn("/api/v1/event/{event_id}/comments", patterns)
        self.assertIn("/api/v1/event/{event_id}/point-by-point", patterns)
        self.assertIn("/api/v1/event/{event_id}/tennis-power", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/graph", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/heatmap/{team_id}", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/shotmap", patterns)

    def test_core_mode_keeps_lightweight_live_routes_only(self) -> None:
        specs = build_event_detail_request_specs(
            sport_slug="football",
            status_type="inprogress",
            team_ids=(3002, 3001),
            provider_ids=(1,),
            has_event_player_heat_map=True,
            has_xg=True,
            core_only=True,
        )

        patterns = {item.endpoint.pattern for item in specs}

        self.assertIn("/api/v1/event/{event_id}/comments", patterns)
        self.assertIn("/api/v1/event/{event_id}/graph", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/managers", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/heatmap/{team_id}", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/shotmap", patterns)
        self.assertNotIn("/api/v1/event/{event_id}/odds/{provider_id}/all", patterns)

    def test_live_delta_tennis_fetches_only_live_point_feeds(self) -> None:
        specs = build_event_detail_request_specs(
            sport_slug="tennis",
            status_type="inprogress",
            team_ids=(199527, 199528),
            provider_ids=(1,),
            has_event_player_heat_map=True,
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


if __name__ == "__main__":
    unittest.main()
