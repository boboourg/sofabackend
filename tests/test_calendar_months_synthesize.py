"""TDD tests for Phase 2.1 — synthesize ``/calendar/.../months-with-events``.

This endpoint returns the list of YYYY-MM month strings that have at least
one event for a given (unique_tournament, season). It is currently a
phantom in ``endpoints.py`` (declared but never fetched, never served).
Phase 2.1 builds it from ``event.start_timestamp`` directly — one SQL
query, no upstream traffic.

Contract pinned:
  * SQL: ``SELECT DISTINCT TO_CHAR(...)`` over ``event``, filtered by
    ``unique_tournament_id`` + ``season_id``.
  * Output: ``{"months": ["YYYY-MM", "YYYY-MM", ...]}`` sorted
    chronologically.
  * Empty season → ``{"months": []}``.
"""

from __future__ import annotations

import unittest


class FetchCalendarMonthsRowsContractTests(unittest.IsolatedAsyncioTestCase):
    """``fetch_calendar_months_rows`` runs the canonical DISTINCT query."""

    async def test_passes_ut_and_season_in_order(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_calendar_months_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_calendar_months_rows(
            _StubConn(),
            unique_tournament_id=17,
            season_id=76986,
        )
        self.assertEqual(captured["args"], (17, 76986))

    async def test_query_filters_by_event_table(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_calendar_months_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_calendar_months_rows(
            _StubConn(), unique_tournament_id=1, season_id=1,
        )
        query = str(captured["query"])
        # Source is the canonical event table — start_timestamp is the only
        # field we need.
        self.assertIn("FROM event", query)
        self.assertIn("unique_tournament_id = $1", query)
        self.assertIn("season_id = $2", query)

    async def test_query_distinct_and_orders_chronologically(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_calendar_months_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_calendar_months_rows(
            _StubConn(), unique_tournament_id=1, season_id=1,
        )
        query = str(captured["query"])
        self.assertIn("DISTINCT", query)
        # Sofascore exposes months ascending chronologically. Sort on the
        # underlying date, not on the YYYY-MM string (cheap to do but pin
        # the ORDER BY direction so we cannot regress to DESC).
        self.assertIn("ORDER BY", query)
        # No DESC keyword — must be ASC (default).
        self.assertNotRegex(query, r"ORDER BY.*DESC")

    async def test_returns_month_strings_unwrapped(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_calendar_months_rows,
        )

        # Row shape: asyncpg Record with one column. Wrap in a dict-like.
        class _Row(dict):
            pass

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                return [_Row(month="2024-08"), _Row(month="2024-09"), _Row(month="2025-02")]

        rows = await fetch_calendar_months_rows(
            _StubConn(), unique_tournament_id=1, season_id=1,
        )
        # Fetcher returns raw rows; the payload builder flattens to strings.
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0]["month"], "2024-08")


class BuildCalendarMonthsPayloadTests(unittest.TestCase):
    """``build_calendar_months_payload`` wraps the fetched rows in the
    Sofascore wire format ``{"months": [...]}``."""

    def test_empty_rows_returns_empty_envelope(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_calendar_months_payload,
        )

        self.assertEqual(build_calendar_months_payload([]), {"months": []})

    def test_extracts_month_field_into_string_list(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_calendar_months_payload,
        )

        rows = [
            {"month": "2024-08"},
            {"month": "2024-09"},
            {"month": "2024-10"},
            {"month": "2024-11"},
            {"month": "2024-12"},
            {"month": "2025-01"},
            {"month": "2025-02"},
        ]
        payload = build_calendar_months_payload(rows)
        self.assertEqual(
            payload,
            {
                "months": [
                    "2024-08",
                    "2024-09",
                    "2024-10",
                    "2024-11",
                    "2024-12",
                    "2025-01",
                    "2025-02",
                ]
            },
        )

    def test_dedupes_and_preserves_order(self) -> None:
        """If a duplicate slips through (shouldn't, but pin behavior),
        the builder preserves first occurrence and drops repeats."""
        from schema_inspector.scheduled_events_synthesizer import (
            build_calendar_months_payload,
        )

        rows = [
            {"month": "2024-08"},
            {"month": "2024-09"},
            {"month": "2024-08"},  # duplicate
            {"month": "2024-10"},
        ]
        payload = build_calendar_months_payload(rows)
        self.assertEqual(payload["months"], ["2024-08", "2024-09", "2024-10"])

    def test_drops_rows_with_null_or_empty_month(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_calendar_months_payload,
        )

        rows = [
            {"month": "2024-08"},
            {"month": None},  # event with NULL start_timestamp
            {"month": ""},
            {"month": "2024-09"},
        ]
        payload = build_calendar_months_payload(rows)
        self.assertEqual(payload["months"], ["2024-08", "2024-09"])


class LocalApiCalendarMonthsRouteTests(unittest.IsolatedAsyncioTestCase):
    """``LocalApiApplication._fetch_calendar_months_with_events_payload``
    must call the synthesizer with the route's path params and return
    the wrapped envelope.

    These are unit-level tests that wire the fake connection into
    ``_connect`` so we don't depend on a real DB. They pin the contract
    between the dispatcher and the synthesizer.
    """

    async def test_method_returns_synthesized_envelope(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        class _StubConn:
            def __init__(self) -> None:
                self.closed = False
                self.captured_args: tuple[object, ...] | None = None

            async def fetch(self, query: str, *args: object):
                self.captured_args = args
                return [
                    {"month": "2024-08"},
                    {"month": "2024-09"},
                    {"month": "2024-10"},
                ]

            async def close(self) -> None:
                self.closed = True

        stub = _StubConn()

        async def _connect():
            return stub

        application._connect = _connect

        result = await application._fetch_calendar_months_with_events_payload(
            unique_tournament_id=17,
            season_id=76986,
        )

        self.assertEqual(result, {"months": ["2024-08", "2024-09", "2024-10"]})
        self.assertEqual(stub.captured_args, (17, 76986))
        self.assertTrue(stub.closed)

    async def test_method_returns_empty_when_db_unreachable(self) -> None:
        """Connection failure must not propagate — fallback to
        ``{"months": []}`` so the HTTP dispatcher never throws 500."""
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _connect():
            raise RuntimeError("db unreachable")

        application._connect = _connect

        result = await application._fetch_calendar_months_with_events_payload(
            unique_tournament_id=1,
            season_id=1,
        )
        self.assertEqual(result, {"months": []})

    async def test_method_returns_empty_when_fetch_raises(self) -> None:
        """An error after the connection is open (mid-query) must also
        fall back to the empty envelope and close the connection."""
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

        result = await application._fetch_calendar_months_with_events_payload(
            unique_tournament_id=1,
            season_id=1,
        )
        self.assertEqual(result, {"months": []})
        self.assertTrue(stub.closed)


class CalendarMonthsEndpointRegisteredTests(unittest.TestCase):
    """The endpoint must show up in ``local_api_endpoints()`` so the
    route dispatcher can match the path. Without this, requests to the
    calendar URL would 404 even though the synthesizer works."""

    def test_calendar_months_endpoint_in_local_api_registry(self) -> None:
        from schema_inspector.endpoints import local_api_endpoints

        endpoints = local_api_endpoints()
        patterns = [ep.path_template for ep in endpoints]
        self.assertIn(
            "/api/v1/calendar/unique-tournament/{unique_tournament_id}"
            "/season/{season_id}/months-with-events",
            patterns,
        )


if __name__ == "__main__":
    unittest.main()
