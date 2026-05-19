"""TDD tests for Phase 5.2 — synthesize
``/season/{s}/events/round/{N}/slug/{slug}``.

After Phase 4 lands the correct ``event_round_info`` rows (proper
``round_number`` + ``slug`` per event), serving the slug-aware URL
becomes a 1-row filter — no upstream call needed.

Sofascore envelope is identical to the slug-less variant:
``{"events": [<full event object>, ...]}``. We reuse
``build_payload`` and the existing ``_FETCH_SELECT_AND_JOINS``
template; the only delta is a WHERE clause adding two new params
(``round_number`` + ``slug``).

After this lands, ``/events/round/29/slug/final`` for FIFA WC 2022
returns the 1 event (Argentina vs France) from our DB without
hitting Sofascore — same wire format the frontend already expects.
"""

from __future__ import annotations

import unittest


class FetchRoundSlugEventsRowsContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_passes_all_four_args_in_order(self) -> None:
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
            unique_tournament_id=16,
            season_id=41087,
            round_number=29,
            slug="final",
        )

        self.assertEqual(captured["args"], (16, 41087, 29, "final"))

    async def test_query_filters_event_by_ut_and_season(self) -> None:
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
            unique_tournament_id=1,
            season_id=1,
            round_number=1,
            slug="final",
        )
        query = str(captured["query"])
        self.assertIn("unique_tournament_id = $1", query)
        self.assertIn("season_id = $2", query)

    async def test_query_joins_event_round_info_and_filters(self) -> None:
        """The slug filter lives on ``event_round_info`` (joined into
        the existing ``_FETCH_SELECT_AND_JOINS`` template). Both
        ``round_number`` and ``slug`` are required filters — the
        knockout final and the group stage matchday 1 can share
        ``round_number`` for some sport profiles, so slug
        disambiguates."""
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
            unique_tournament_id=16,
            season_id=41087,
            round_number=29,
            slug="final",
        )
        query = str(captured["query"])
        self.assertIn("event_round_info", query)
        self.assertIn("round_number = $3", query)
        # The slug parameter is bound to the round-info row's slug.
        self.assertIn("$4", query)

    async def test_query_orders_results_consistently(self) -> None:
        """Multi-event rounds (e.g. round-of-16 with 8 matches) must
        come back in a stable order so the synthesized envelope diffs
        cleanly against the upstream snapshot when both exist."""
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
            unique_tournament_id=16,
            season_id=41087,
            round_number=5,
            slug="round-of-16",
        )
        query = str(captured["query"])
        self.assertIn("ORDER BY", query)
        # Stable secondary key — event_id — protects against same-
        # timestamp ties (rare but real for paired knockout fixtures).
        self.assertIn("start_timestamp", query)


class LocalApiRoundSlugEventsRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_method_returns_synthesized_envelope(self) -> None:
        """The dispatcher passes the four path params to the
        synthesizer and wraps the rows in the ``{"events": [...]}``
        envelope via the existing ``build_payload``. We assert on the
        SQL call args + envelope shape; full per-row content is
        covered by ``test_scheduled_events_synthesizer`` tests."""
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        class _StubConn:
            def __init__(self) -> None:
                self.closed = False
                self.captured_args: tuple[object, ...] | None = None

            async def fetch(self, query: str, *args: object):
                self.captured_args = args
                return []  # empty — pin contract not payload content

            async def close(self) -> None:
                self.closed = True

        stub = _StubConn()

        async def _connect():
            return stub

        application._connect = _connect

        result = await application._fetch_round_slug_events_payload(
            unique_tournament_id=16,
            season_id=41087,
            round_number=29,
            slug="final",
        )
        self.assertEqual(result, {"events": []})
        self.assertEqual(stub.captured_args, (16, 41087, 29, "final"))
        self.assertTrue(stub.closed)

    async def test_method_returns_empty_envelope_when_db_unreachable(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _connect():
            raise RuntimeError("db unreachable")

        application._connect = _connect

        result = await application._fetch_round_slug_events_payload(
            unique_tournament_id=1,
            season_id=1,
            round_number=1,
            slug="final",
        )
        self.assertEqual(result, {"events": []})

    async def test_method_returns_empty_envelope_when_fetch_raises(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        class _StubConn:
            def __init__(self) -> None:
                self.closed = False

            async def fetch(self, query: str, *args: object):
                raise RuntimeError("query exploded")

            async def close(self) -> None:
                self.closed = True

        stub = _StubConn()

        async def _connect():
            return stub

        application._connect = _connect

        result = await application._fetch_round_slug_events_payload(
            unique_tournament_id=1,
            season_id=1,
            round_number=1,
            slug="final",
        )
        self.assertEqual(result, {"events": []})
        self.assertTrue(stub.closed)


class RoundSlugEndpointOriginPromotedTests(unittest.TestCase):
    """Phase 5.2 promotes the slug-aware endpoint from UPSTREAM to
    FEDERATED. Upstream fetch (Phase 4) lands the canonical round
    structure into ``event_round_info``; from then on the local API
    serves the route from DB. Future cursor walks can skip the
    re-fetch (Phase 5.3) once we wire the skip-when-populated check.
    """

    def test_endpoint_origin_promoted_to_federated(self) -> None:
        from schema_inspector.endpoints import (
            EndpointOrigin,
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT,
        )

        self.assertEqual(
            UNIQUE_TOURNAMENT_ROUND_EVENTS_SLUG_ENDPOINT.origin,
            EndpointOrigin.FEDERATED,
        )


if __name__ == "__main__":
    unittest.main()
