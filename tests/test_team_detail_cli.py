from __future__ import annotations

import unittest

from schema_inspector.team_detail_cli import TeamFetchJob, build_jobs
from schema_inspector.endpoints import (
    TEAM_PLAYERS_ENDPOINT,
    team_last_events_endpoint,
    team_next_events_endpoint,
)


class BuildJobsTests(unittest.TestCase):
    def test_default_includes_only_team_players(self) -> None:
        jobs = build_jobs(42, include_events=False, pages=1)
        self.assertEqual(len(jobs), 1)
        self.assertIs(jobs[0].endpoint, TEAM_PLAYERS_ENDPOINT)
        self.assertEqual(jobs[0].path_params, {"team_id": 42})
        self.assertEqual(jobs[0].context_entity_type, "team")
        self.assertEqual(jobs[0].context_entity_id, 42)

    def test_include_events_with_one_page(self) -> None:
        jobs = build_jobs(2672, include_events=True, pages=1)
        # Expect: team/players + (events/last/0, events/next/0).
        self.assertEqual(len(jobs), 3)
        endpoints = [j.endpoint.path_template for j in jobs]
        self.assertIn("/api/v1/team/{team_id}/players", endpoints)
        self.assertIn("/api/v1/team/{team_id}/events/last/{page}", endpoints)
        self.assertIn("/api/v1/team/{team_id}/events/next/{page}", endpoints)
        # All event jobs must carry the same page=0.
        for job in jobs:
            if "events" in job.endpoint.path_template:
                self.assertEqual(job.path_params.get("page"), 0)
                self.assertEqual(job.path_params.get("team_id"), 2672)
                self.assertEqual(job.context_entity_type, "team")
                self.assertEqual(job.context_entity_id, 2672)

    def test_include_events_with_multiple_pages_does_both_directions(self) -> None:
        jobs = build_jobs(42, include_events=True, pages=3)
        # Expect: team/players + 3 pages * 2 directions = 7 jobs.
        self.assertEqual(len(jobs), 7)
        last_pages = sorted(
            j.path_params["page"]
            for j in jobs
            if j.endpoint.path_template == "/api/v1/team/{team_id}/events/last/{page}"
        )
        next_pages = sorted(
            j.path_params["page"]
            for j in jobs
            if j.endpoint.path_template == "/api/v1/team/{team_id}/events/next/{page}"
        )
        self.assertEqual(last_pages, [0, 1, 2])
        self.assertEqual(next_pages, [0, 1, 2])

    def test_zero_pages_with_include_events_emits_only_players(self) -> None:
        jobs = build_jobs(42, include_events=True, pages=0)
        self.assertEqual(len(jobs), 1)
        self.assertIs(jobs[0].endpoint, TEAM_PLAYERS_ENDPOINT)

    def test_negative_pages_rejected(self) -> None:
        with self.assertRaises(ValueError):
            build_jobs(42, include_events=True, pages=-1)

    def test_team_last_and_next_endpoints_are_distinct(self) -> None:
        # Defensive: ensure factory functions return separate endpoints; otherwise
        # build_jobs would silently fetch the same URL twice for each page.
        last = team_last_events_endpoint()
        nxt = team_next_events_endpoint()
        self.assertNotEqual(last.path_template, nxt.path_template)


if __name__ == "__main__":
    unittest.main()
