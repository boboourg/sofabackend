"""TDD tests for Phase 2.4 — synthesize ``/event/{custom_id}/h2h/events``.

The endpoint takes a Sofascore-style ``custom_id`` (string token per
event) and returns the historical head-to-head fixture list between the
two teams that played in the anchor event. The data is entirely in our
``event`` table — we just need a 2-step query:

  1. Resolve ``custom_id`` → ``(home_team_id, away_team_id)`` from the
     anchor event.
  2. Filter ``event`` for both directions of that pair, ordered most
     recent first.

Output envelope: ``{"events": [<full event objects>]}`` — same shape as
``build_payload`` already emits for the other event-list synthesizers.
"""

from __future__ import annotations

import unittest


class FetchH2HEventsRowsContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_passes_custom_id_as_only_arg(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_h2h_events_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_h2h_events_rows(_StubConn(), custom_id="IKxhsOLaj")
        # Default limit applied — implementation chooses 20 (matches Sofascore).
        self.assertEqual(captured["args"][0], "IKxhsOLaj")

    async def test_query_resolves_custom_id_via_anchor_event(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_h2h_events_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_h2h_events_rows(_StubConn(), custom_id="ZZZ")
        query = str(captured["query"])
        # The query MUST start from the anchor event to resolve the pair.
        # We accept either CTE form or subquery — pin the lookup column:
        self.assertIn("custom_id = $1", query)

    async def test_query_includes_both_directions_of_team_pair(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_h2h_events_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_h2h_events_rows(_StubConn(), custom_id="ZZZ")
        query = str(captured["query"])
        # Must filter on BOTH directions (home vs away can swap between
        # encounters). Pin OR / both team_id refs:
        self.assertIn("home_team_id", query)
        self.assertIn("away_team_id", query)
        # And cover the swap explicitly (the anchor pair AND the reverse).
        # Accept either "OR" or a flexible form — count team_id refs >= 4.
        self.assertGreaterEqual(query.count("team_id"), 4)

    async def test_query_orders_by_start_timestamp_desc(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_h2h_events_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_h2h_events_rows(_StubConn(), custom_id="ZZZ")
        query = str(captured["query"])
        # Most-recent-first ordering matches Sofascore's behavior.
        self.assertRegex(query, r"ORDER BY.*start_timestamp.*DESC")

    async def test_query_respects_limit_param(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_h2h_events_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_h2h_events_rows(_StubConn(), custom_id="ZZZ", limit=5)
        # The limit value must be parameterized (not interpolated),
        # so it shows up in args. The exact position depends on the
        # implementation but it MUST be passed.
        self.assertIn(5, captured["args"])


class LocalApiH2HEventsRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_method_returns_envelope_with_events(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        class _StubConn:
            def __init__(self) -> None:
                self.closed = False
                self.captured_custom_id: str | None = None

            async def fetch(self, query: str, *args: object):
                self.captured_custom_id = str(args[0])
                # No matching events — empty fixture history.
                return []

            async def close(self) -> None:
                self.closed = True

        stub = _StubConn()

        async def _connect():
            return stub

        application._connect = _connect

        result = await application._fetch_h2h_events_payload(
            custom_id="IKxhsOLaj",
        )

        self.assertEqual(result, {"events": []})
        self.assertEqual(stub.captured_custom_id, "IKxhsOLaj")
        self.assertTrue(stub.closed)

    async def test_method_returns_empty_when_db_unreachable(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _connect():
            raise RuntimeError("db down")

        application._connect = _connect

        result = await application._fetch_h2h_events_payload(custom_id="X")
        self.assertEqual(result, {"events": []})

    async def test_method_returns_empty_when_fetch_raises(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        class _StubConn:
            def __init__(self) -> None:
                self.closed = False

            async def fetch(self, query: str, *args: object):
                raise RuntimeError("query died")

            async def close(self) -> None:
                self.closed = True

        stub = _StubConn()

        async def _connect():
            return stub

        application._connect = _connect

        result = await application._fetch_h2h_events_payload(custom_id="X")
        self.assertEqual(result, {"events": []})
        self.assertTrue(stub.closed)


class H2HEventsEndpointRefreshDisabledTests(unittest.TestCase):
    """Phase 2.4 — the H2H events endpoint is synthesized locally, so the
    resource refresh loop must stop publishing custom_id targets for it.
    Disabling the scope_kind cuts ~1 HTTP per finished football event per
    week without breaking on-demand access (the constant + path_template
    stay for the dispatcher / Swagger)."""

    def test_h2h_events_endpoint_has_no_scope_kind(self) -> None:
        from schema_inspector.endpoints import EVENT_H2H_EVENTS_ENDPOINT

        self.assertIsNone(
            EVENT_H2H_EVENTS_ENDPOINT.scope_kind,
            "h2h/events is synthesized from event history — drop "
            "scope_kind so CustomIdOfRegistryEventsResolver stops "
            "feeding the resource refresh loop.",
        )

    def test_h2h_events_endpoint_has_no_refresh_interval(self) -> None:
        from schema_inspector.endpoints import EVENT_H2H_EVENTS_ENDPOINT

        self.assertIsNone(EVENT_H2H_EVENTS_ENDPOINT.refresh_interval_seconds)

    def test_h2h_events_endpoint_keeps_path_template(self) -> None:
        from schema_inspector.endpoints import EVENT_H2H_EVENTS_ENDPOINT

        self.assertEqual(
            EVENT_H2H_EVENTS_ENDPOINT.path_template,
            "/api/v1/event/{custom_id}/h2h/events",
        )


if __name__ == "__main__":
    unittest.main()
