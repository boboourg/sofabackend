from __future__ import annotations

import unittest

from schema_inspector.jobs.envelope import JobEnvelope
from schema_inspector.jobs.types import (
    JOB_FINALIZE_EVENT,
    JOB_HYDRATE_EVENT_EDGE,
    JOB_HYDRATE_EVENT_ROOT,
    JOB_SYNC_SEASON_WIDGET,
    JOB_TRACK_LIVE_EVENT,
)
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


if __name__ == "__main__":
    unittest.main()
