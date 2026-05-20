"""TDD tests for sub-tournament priority filtering in
``/round/{N}/slug/{slug}`` synth.

Bug discovered 2026-05-20: UCL 22/23 had two events matching
(UT=7, season=41897, round=29, slug='final'):
* event 11289024 Manchester City vs Inter — sub_tournament
  "UEFA Champions League, Knockout stage" (priority=716)
* event 10400733 Inter Club d'Escaldes vs Vikingur Reykjavik —
  sub_tournament "UEFA Champions League, Preliminary Round"
  (priority=488)

Upstream Sofascore returns ONLY the higher-priority sub-tournament's
final (Knockout stage). Our synth was returning both. Fix: filter
on tournament.priority = MAX(...) across the matching set.
"""

from __future__ import annotations

import re
import unittest


class FetchRoundSlugEventsHighestPriorityTests(unittest.IsolatedAsyncioTestCase):
    async def test_query_filters_by_highest_priority_sub_tournament(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_round_slug_events_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_round_slug_events_rows(
            _StubConn(),
            unique_tournament_id=7,
            season_id=41897,
            round_number=29,
            slug="final",
        )
        query = str(captured["query"])
        # Outer query must include the priority filter referencing
        # an inner MAX over the matching set.
        self.assertRegex(
            query,
            re.compile(
                r"COALESCE\(t\.priority,\s*0\)\s*=\s*\(\s*SELECT\s+MAX\(",
                re.DOTALL,
            ),
        )

    async def test_query_still_filters_by_round_number_and_slug(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_round_slug_events_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_round_slug_events_rows(
            _StubConn(),
            unique_tournament_id=7,
            season_id=41897,
            round_number=29,
            slug="final",
        )
        query = str(captured["query"])
        self.assertIn("eri.round_number = $3", query)
        self.assertIn("eri.slug = $4", query)


if __name__ == "__main__":
    unittest.main()
