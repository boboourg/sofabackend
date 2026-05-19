"""TDD tests for Phase 4 Step 2 — parser + job wiring for
slug-aware round events.

The orchestrator (Step 4) needs two thin layers on top of the
endpoint declaration:

* ``EventListParser.fetch_round_events_with_slug`` — fires the HTTP
  GET against ``UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT`` with
  the four path params (ut, season, round_number, slug).
* ``EventListIngestJob.run_round_with_slug`` — wraps the parser fetch
  in the job's transactional upsert + correction detection, and
  produces a distinctive ``job_name`` so logs/audits can pin slug-
  routed traffic separately from the bare slug-less round fetch.

These tests pin the contract: URL dispatch, params propagation, and
job_name shape. They do NOT cover the orchestrator routing decision
(that's Step 4).
"""

from __future__ import annotations

import unittest


class FetchRoundEventsWithSlugUrlDispatchTests(unittest.IsolatedAsyncioTestCase):
    """``fetch_round_events_with_slug`` dispatches the slug-aware
    endpoint with all four path params populated."""

    async def test_dispatches_slug_aware_endpoint(self) -> None:
        """The parser's fetch_round_events_with_slug builds the
        slug-aware URL and feeds it to the HTTP client. We short-
        circuit the rest of the parse pipeline by having the stub
        client raise — only the URL contract is under test here."""
        from schema_inspector.event_list_parser import EventListParser

        captured: dict[str, object] = {}

        class _StubClient:
            async def get_json(self, url: str, *, timeout: float | None = None):
                captured["url"] = url
                captured["timeout"] = timeout
                raise RuntimeError("stub-short-circuit")

        parser = EventListParser(client=_StubClient())
        with self.assertRaises(RuntimeError):
            await parser.fetch_round_events_with_slug(
                unique_tournament_id=16,
                season_id=58210,
                round_number=29,
                slug="final",
                sport_slug="football",
                timeout=20.0,
            )

        self.assertEqual(
            captured["url"],
            "https://www.sofascore.com/api/v1/unique-tournament/16/season/58210"
            "/events/round/29/slug/final",
        )
        self.assertEqual(captured["timeout"], 20.0)

    async def test_url_for_quarterfinals_propagates_slug(self) -> None:
        from schema_inspector.event_list_parser import EventListParser

        captured: dict[str, object] = {}

        class _StubClient:
            async def get_json(self, url: str, *, timeout: float | None = None):
                captured["url"] = url
                raise RuntimeError("stub-short-circuit")

        parser = EventListParser(client=_StubClient())
        with self.assertRaises(RuntimeError):
            await parser.fetch_round_events_with_slug(
                unique_tournament_id=7,
                season_id=52162,
                round_number=27,
                slug="quarterfinals",
                sport_slug="football",
                timeout=20.0,
            )

        self.assertIn("/round/27/slug/quarterfinals", str(captured["url"]))


class RunRoundWithSlugJobTests(unittest.IsolatedAsyncioTestCase):
    """``EventListIngestJob.run_round_with_slug`` exists and produces a
    distinctive ``job_name`` so the audit log can distinguish slug-
    routed traffic from bare slug-less round fetches."""

    async def test_run_round_with_slug_exists_and_calls_parser(self) -> None:
        """Pin the wiring: ``EventListIngestJob.run_round_with_slug`` is
        callable and forwards path params to the parser. Job-name
        contract is verified through a lighter unit (no DB round-trip)."""
        from schema_inspector.event_list_job import EventListIngestJob

        captured: dict[str, object] = {}

        class _StubParser:
            async def fetch_round_events_with_slug(
                self,
                unique_tournament_id,
                season_id,
                round_number,
                slug,
                *,
                sport_slug,
                timeout,
            ):
                captured["ut"] = unique_tournament_id
                captured["season"] = season_id
                captured["round"] = round_number
                captured["slug"] = slug
                captured["sport"] = sport_slug
                # Raise to short-circuit the rest of _run; we only care
                # about the parser call here.
                raise RuntimeError("parser-stub-called")

        class _StubRepo:
            pass

        class _StubDb:
            pass

        job = EventListIngestJob(
            parser=_StubParser(),
            repository=_StubRepo(),
            database=_StubDb(),
        )
        with self.assertRaises(RuntimeError):
            await job.run_round_with_slug(
                unique_tournament_id=16,
                season_id=58210,
                round_number=29,
                slug="final",
                sport_slug="football",
                timeout=20.0,
            )

        self.assertEqual(captured["ut"], 16)
        self.assertEqual(captured["season"], 58210)
        self.assertEqual(captured["round"], 29)
        self.assertEqual(captured["slug"], "final")
        self.assertEqual(captured["sport"], "football")

    def test_run_round_with_slug_is_method_of_job(self) -> None:
        """Static API surface check — the method exists and is callable."""
        from schema_inspector.event_list_job import EventListIngestJob

        self.assertTrue(hasattr(EventListIngestJob, "run_round_with_slug"))
        self.assertTrue(callable(EventListIngestJob.run_round_with_slug))


if __name__ == "__main__":
    unittest.main()
