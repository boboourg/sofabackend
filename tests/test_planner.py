from __future__ import annotations

import unittest

from schema_inspector.jobs.envelope import JobEnvelope
from schema_inspector.endpoints import unique_tournament_top_players_endpoint
from schema_inspector.jobs.types import (
    JOB_FINALIZE_EVENT,
    JOB_HYDRATE_EVENT_EDGE,
    JOB_HYDRATE_EVENT_ROOT,
    JOB_HYDRATE_SPECIAL_ROUTE,
    JOB_SYNC_SEASON_WIDGET,
    JOB_TRACK_LIVE_EVENT,
)
from schema_inspector.parsers.base import ParseResult
from schema_inspector.planner.planner import Planner


class PlannerTests(unittest.TestCase):
    def test_event_root_expands_to_core_edges_and_live_tracking(self) -> None:
        planner = Planner(
            capability_rollup={
                "/api/v1/event/{event_id}/graph": "supported",
            }
        )
        job = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug="football",
            entity_type="event",
            entity_id=14083191,
            scope="live",
            params={"status_type": "inprogress"},
            priority=1,
            trace_id="trace-1",
        )

        planned = planner.expand(job)
        kinds = {(item.job_type, item.params.get("edge_kind")) for item in planned}

        self.assertIn((JOB_HYDRATE_EVENT_EDGE, "meta"), kinds)
        self.assertIn((JOB_HYDRATE_EVENT_EDGE, "statistics"), kinds)
        self.assertIn((JOB_HYDRATE_EVENT_EDGE, "lineups"), kinds)
        self.assertIn((JOB_HYDRATE_EVENT_EDGE, "incidents"), kinds)
        self.assertIn((JOB_HYDRATE_EVENT_EDGE, "graph"), kinds)
        self.assertTrue(any(item.job_type == JOB_TRACK_LIVE_EVENT for item in planned))

    def test_event_root_skips_unsupported_optional_edges(self) -> None:
        planner = Planner(
            capability_rollup={
                "/api/v1/event/{event_id}/graph": "unsupported",
            }
        )
        job = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug="handball",
            entity_type="event",
            entity_id=1,
            scope="live",
            params={"status_type": "inprogress"},
            priority=1,
            trace_id="trace-1",
        )

        planned = planner.expand(job)
        graph_jobs = [item for item in planned if item.params.get("edge_kind") == "graph"]
        self.assertEqual(graph_jobs, [])

    def test_event_root_keeps_core_edges_even_if_rollup_is_unsupported(self) -> None:
        planner = Planner(
            capability_rollup={
                "/api/v1/event/{event_id}/incidents": "unsupported",
                "/api/v1/event/{event_id}/statistics": "unsupported",
                "/api/v1/event/{event_id}/lineups": "unsupported",
            }
        )
        job = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug="football",
            entity_type="event",
            entity_id=14083191,
            scope="live",
            params={"status_type": "inprogress"},
            priority=1,
            trace_id="trace-1",
        )

        planned = planner.expand(job)
        kinds = {(item.job_type, item.params.get("edge_kind")) for item in planned}

        self.assertIn((JOB_HYDRATE_EVENT_EDGE, "statistics"), kinds)
        self.assertIn((JOB_HYDRATE_EVENT_EDGE, "lineups"), kinds)
        self.assertIn((JOB_HYDRATE_EVENT_EDGE, "incidents"), kinds)

    def test_finished_event_schedules_finalize(self) -> None:
        planner = Planner()
        job = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug="basketball",
            entity_type="event",
            entity_id=14439306,
            scope="postgame",
            params={"status_type": "finished"},
            priority=1,
            trace_id="trace-1",
        )

        planned = planner.expand(job)

        self.assertTrue(any(item.job_type == JOB_FINALIZE_EVENT for item in planned))

    def test_baseball_uses_adapter_specific_core_edges(self) -> None:
        planner = Planner()
        job = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug="baseball",
            entity_type="event",
            entity_id=15507996,
            scope="live",
            params={"status_type": "inprogress"},
            priority=1,
            trace_id="trace-1",
        )

        planned = planner.expand(job)
        kinds = {item.params.get("edge_kind") for item in planned if item.job_type == JOB_HYDRATE_EVENT_EDGE}

        self.assertIn("statistics", kinds)
        self.assertIn("lineups", kinds)
        self.assertIn("incidents", kinds)

    def test_live_delta_football_edges_are_only_live_delta_policy(self) -> None:
        planner = Planner()
        job = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug="football",
            entity_type="event",
            entity_id=1,
            scope="live",
            params={"status_type": "inprogress", "hydration_mode": "live_delta"},
            priority=0,
            trace_id=None,
        )

        planned = planner.expand(job)
        edge_kinds = tuple(item.params["edge_kind"] for item in planned if item.job_type == JOB_HYDRATE_EVENT_EDGE)

        self.assertEqual(edge_kinds, ("meta", "statistics", "lineups", "incidents", "graph"))

    def test_live_delta_tennis_edges_drop_lineups_and_incidents(self) -> None:
        planner = Planner()
        job = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug="tennis",
            entity_type="event",
            entity_id=1,
            scope="live",
            params={"status_type": "inprogress", "hydration_mode": "live_delta"},
            priority=0,
            trace_id=None,
        )

        planned = planner.expand(job)
        edge_kinds = tuple(item.params["edge_kind"] for item in planned if item.job_type == JOB_HYDRATE_EVENT_EDGE)

        self.assertEqual(edge_kinds, ("meta", "statistics"))

    def test_event_root_skips_dead_optional_graph_for_futsal(self) -> None:
        planner = Planner(
            capability_rollup={
                "/api/v1/event/{event_id}/graph": "supported",
            }
        )
        job = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug="futsal",
            entity_type="event",
            entity_id=1,
            scope="live",
            params={"status_type": "inprogress"},
            priority=1,
            trace_id="trace-1",
        )

        planned = planner.expand(job)
        edge_kinds = {item.params["edge_kind"] for item in planned if item.job_type == JOB_HYDRATE_EVENT_EDGE}

        self.assertNotIn("graph", edge_kinds)

    def test_season_widgets_follow_sport_profile(self) -> None:
        planner = Planner()

        basketball = planner.plan_season_widgets("basketball", unique_tournament_id=132, season_id=84695)
        tennis = planner.plan_season_widgets("tennis", unique_tournament_id=2361, season_id=90001)

        basketball_pairs = {(item.params["widget_kind"], item.params.get("suffix")) for item in basketball}

        self.assertIn(("top_players", "regularSeason"), basketball_pairs)
        self.assertIn(("top_players_per_game", "all/regularSeason"), basketball_pairs)
        self.assertIn(("top_teams", "regularSeason"), basketball_pairs)
        self.assertIn(("player_of_the_season", None), basketball_pairs)
        self.assertEqual(tennis, ())

    def test_season_widgets_support_coarse_suppression_by_endpoint_pattern(self) -> None:
        planner = Planner()
        blocked_pattern = unique_tournament_top_players_endpoint("regularSeason").pattern

        basketball = planner.plan_season_widgets(
            "basketball",
            unique_tournament_id=132,
            season_id=84695,
            blocked_endpoint_patterns=(blocked_pattern,),
        )

        basketball_pairs = {(item.params["widget_kind"], item.params.get("suffix")) for item in basketball}

        self.assertNotIn(("top_players", "regularSeason"), basketball_pairs)
        self.assertIn(("top_players_per_game", "all/regularSeason"), basketball_pairs)
        self.assertIn(("top_teams", "regularSeason"), basketball_pairs)

    def test_lineups_schedule_player_analytics_followups_for_football_starters(self) -> None:
        planner = Planner()
        root_job = JobEnvelope.create(
            job_type=JOB_HYDRATE_EVENT_ROOT,
            sport_slug="football",
            entity_type="event",
            entity_id=14083191,
            scope="pilot",
            params={"status_type": "inprogress"},
            priority=0,
            trace_id="trace-1",
        )
        lineups_job = root_job.spawn_child(
            job_type=JOB_HYDRATE_EVENT_EDGE,
            entity_type="event",
            entity_id=14083191,
            scope="pilot",
            params={"edge_kind": "lineups"},
            priority=1,
        )
        parse_result = ParseResult(
            snapshot_id=1,
            parser_family="event_lineups",
            parser_version="v1",
            status="parsed",
            relation_upserts={
                "event_lineup_player": (
                    {"event_id": 14083191, "player_id": 700, "substitute": False},
                    {"event_id": 14083191, "player_id": 701, "substitute": True},
                    {"event_id": 14083191, "player_id": 702, "substitute": False},
                )
            },
        )

        planned = planner.plan_lineup_followups(lineups_job, parse_result)

        self.assertEqual(sum(item.job_type == JOB_HYDRATE_SPECIAL_ROUTE for item in planned), 6)
        special_kinds = [item.params["special_kind"] for item in planned]
        self.assertEqual(special_kinds.count("event_player_statistics"), 2)
        self.assertEqual(special_kinds.count("event_player_heatmap"), 2)
        self.assertEqual(special_kinds.count("event_player_rating_breakdown"), 2)


if __name__ == "__main__":
    unittest.main()
