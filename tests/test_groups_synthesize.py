"""TDD tests for Phase 5.1 — synthesize ``/unique-tournament/{ut}/season/{s}/groups``.

The Sofascore ``/groups`` endpoint returns the list of group-stage
sub-tournaments under a cup-style unique tournament (FIFA WC, classic
UCL until 2023/24, etc.). Each entry is ``{groupName, tournamentId}``
— the sub-tournament id of the group, e.g.:

    {"groups": [
        {"groupName": "Group A", "tournamentId": 3954},
        {"groupName": "Group B", "tournamentId": 3955},
        ...
    ]}

We already have all this data in our DB: each group event references
its sub-tournament via ``event.tournament_id``, and the sub-tournament
sits in the ``tournament`` table with a name like
``"FIFA World Cup, Group A"`` or ``"UEFA Champions League, Group A"``.
The umbrella ``unique_tournament_id`` links them all.

So instead of fetching ``/groups`` (which the resource refresh loop
only does for ±60 day window via leaderboards) we synthesize the
response from the ``tournament`` rows we already discovered through
the event-list flow. This unlocks group pages for ALL historical
cup-style competitions without upstream traffic.

Phase 5.1 pure-function + DB-contract + dispatcher tests below.
"""

from __future__ import annotations

import unittest


class FetchGroupsRowsContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_filters_by_unique_tournament_and_season(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import fetch_groups_rows

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_groups_rows(
            _StubConn(),
            unique_tournament_id=16,
            season_id=41087,
        )

        self.assertEqual(captured["args"], (16, 41087))
        query = str(captured["query"])
        self.assertIn("unique_tournament_id = $1", query)
        self.assertIn("season_id = $2", query)

    async def test_query_joins_event_and_tournament_tables(self) -> None:
        """Source of truth for sub-tournament discovery is ``event``
        (which knows about both ``unique_tournament_id`` and
        ``tournament_id``) JOIN ``tournament`` (for the human-readable
        name we parse the group letter out of)."""
        from schema_inspector.scheduled_events_synthesizer import fetch_groups_rows

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_groups_rows(
            _StubConn(),
            unique_tournament_id=1,
            season_id=1,
        )
        query = str(captured["query"])
        self.assertIn("FROM event", query)
        self.assertIn("tournament", query)

    async def test_query_distinct_and_orders_by_name(self) -> None:
        """``Group A`` before ``Group B`` etc. — stable wire order so
        the synthesised envelope matches Sofascore's alphabetical
        listing."""
        from schema_inspector.scheduled_events_synthesizer import fetch_groups_rows

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_groups_rows(
            _StubConn(),
            unique_tournament_id=1,
            season_id=1,
        )
        query = str(captured["query"])
        self.assertIn("DISTINCT", query)
        self.assertIn("ORDER BY", query)
        # Default ASC — no DESC keyword.
        self.assertNotRegex(query, r"ORDER BY.*DESC")

    async def test_returns_tournament_rows_with_id_and_name(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import fetch_groups_rows

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                return [
                    {"tournament_id": 3954, "tournament_name": "FIFA World Cup, Group A"},
                    {"tournament_id": 3955, "tournament_name": "FIFA World Cup, Group B"},
                ]

        rows = await fetch_groups_rows(
            _StubConn(),
            unique_tournament_id=16,
            season_id=41087,
        )

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["tournament_id"], 3954)
        self.assertEqual(rows[0]["tournament_name"], "FIFA World Cup, Group A")


class BuildGroupsPayloadTests(unittest.TestCase):
    """``build_groups_payload`` produces the Sofascore wire format
    ``{"groups": [{groupName, tournamentId}, ...]}``."""

    def test_empty_rows_returns_empty_envelope(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import build_groups_payload

        self.assertEqual(build_groups_payload([]), {"groups": []})

    def test_wc_2022_8_groups(self) -> None:
        """FIFA WC 2022 has 8 groups A-H — full pyramid pinned here."""
        from schema_inspector.scheduled_events_synthesizer import build_groups_payload

        rows = [
            {"tournament_id": 3954, "tournament_name": "FIFA World Cup, Group A"},
            {"tournament_id": 3955, "tournament_name": "FIFA World Cup, Group B"},
            {"tournament_id": 3956, "tournament_name": "FIFA World Cup, Group C"},
            {"tournament_id": 3957, "tournament_name": "FIFA World Cup, Group D"},
            {"tournament_id": 3958, "tournament_name": "FIFA World Cup, Group E"},
            {"tournament_id": 3959, "tournament_name": "FIFA World Cup, Group F"},
            {"tournament_id": 3960, "tournament_name": "FIFA World Cup, Group G"},
            {"tournament_id": 3961, "tournament_name": "FIFA World Cup, Group H"},
        ]
        payload = build_groups_payload(rows)
        self.assertEqual(
            payload,
            {
                "groups": [
                    {"groupName": "Group A", "tournamentId": 3954},
                    {"groupName": "Group B", "tournamentId": 3955},
                    {"groupName": "Group C", "tournamentId": 3956},
                    {"groupName": "Group D", "tournamentId": 3957},
                    {"groupName": "Group E", "tournamentId": 3958},
                    {"groupName": "Group F", "tournamentId": 3959},
                    {"groupName": "Group G", "tournamentId": 3960},
                    {"groupName": "Group H", "tournamentId": 3961},
                ]
            },
        )

    def test_extracts_group_name_from_combined_string(self) -> None:
        """Sofascore tournament names look like ``"<Cup>, Group X"`` —
        the synthesizer slices off the part after the last comma."""
        from schema_inspector.scheduled_events_synthesizer import build_groups_payload

        rows = [
            {
                "tournament_id": 1462,
                "tournament_name": "UEFA Champions League, Group A",
            },
        ]
        payload = build_groups_payload(rows)
        self.assertEqual(
            payload, {"groups": [{"groupName": "Group A", "tournamentId": 1462}]}
        )

    def test_skips_rows_without_group_in_name(self) -> None:
        """Sub-tournaments that are NOT group-stage (e.g. ``Knockout``,
        ``Qualification``, ``Playoff Round``) must be filtered out —
        ``/groups`` is groups-only."""
        from schema_inspector.scheduled_events_synthesizer import build_groups_payload

        rows = [
            {"tournament_id": 3954, "tournament_name": "FIFA World Cup, Group A"},
            {"tournament_id": 3948, "tournament_name": "FIFA World Cup, Knockout"},
            {"tournament_id": 1339, "tournament_name": "UEFA Champions League, Qualification"},
            {"tournament_id": 3955, "tournament_name": "FIFA World Cup, Group B"},
        ]
        payload = build_groups_payload(rows)
        # Only Group A + Group B survive.
        self.assertEqual(
            payload,
            {
                "groups": [
                    {"groupName": "Group A", "tournamentId": 3954},
                    {"groupName": "Group B", "tournamentId": 3955},
                ]
            },
        )

    def test_swiss_format_returns_empty(self) -> None:
        """UCL 2024/25+ uses a single Swiss-format league phase with
        zero groups. Sub-tournaments are ``"UEFA Champions League"``,
        ``"…, Knockout Phase"``, ``"…, Playoff Round"``,
        ``"…, Qualification"`` — none match the group pattern.
        Synthesizer correctly returns ``{"groups": []}``."""
        from schema_inspector.scheduled_events_synthesizer import build_groups_payload

        rows = [
            {"tournament_id": 138314, "tournament_name": "UEFA Champions League"},
            {"tournament_id": 145109, "tournament_name": "UEFA Champions League, Knockout Phase"},
            {"tournament_id": 119880, "tournament_name": "UEFA Champions League, Playoff Round"},
            {"tournament_id": 1339, "tournament_name": "UEFA Champions League, Qualification"},
        ]
        payload = build_groups_payload(rows)
        self.assertEqual(payload, {"groups": []})


class LocalApiGroupsRouteTests(unittest.IsolatedAsyncioTestCase):
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
                    {"tournament_id": 3954, "tournament_name": "FIFA World Cup, Group A"},
                    {"tournament_id": 3955, "tournament_name": "FIFA World Cup, Group B"},
                ]

            async def close(self) -> None:
                self.closed = True

        stub = _StubConn()

        async def _connect():
            return stub

        application._connect = _connect

        result = await application._fetch_groups_payload(
            unique_tournament_id=16,
            season_id=41087,
        )
        self.assertEqual(
            result,
            {
                "groups": [
                    {"groupName": "Group A", "tournamentId": 3954},
                    {"groupName": "Group B", "tournamentId": 3955},
                ]
            },
        )
        self.assertEqual(stub.captured_args, (16, 41087))
        self.assertTrue(stub.closed)

    async def test_method_returns_empty_when_db_unreachable(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _connect():
            raise RuntimeError("db unreachable")

        application._connect = _connect

        result = await application._fetch_groups_payload(
            unique_tournament_id=1,
            season_id=1,
        )
        self.assertEqual(result, {"groups": []})


class GroupsEndpointMarkedSyntheticTests(unittest.TestCase):
    """Phase 5.1: existing ``UNIQUE_TOURNAMENT_GROUPS_ENDPOINT`` gets
    promoted from UPSTREAM to FEDERATED (snapshot primary + synthesize
    fallback) so callers see a Sofascore-stable envelope even when
    leaderboards never fetched the snapshot."""

    def test_groups_endpoint_origin_promoted_to_federated(self) -> None:
        from schema_inspector.endpoints import (
            EndpointOrigin,
            UNIQUE_TOURNAMENT_GROUPS_ENDPOINT,
        )

        # FEDERATED = snapshot primary + synth fallback. We keep the
        # ``scope_kind`` so leaderboards-driven snapshot capture
        # continues (gives 1:1 when available); when missing we now
        # synthesize from ``tournament`` rows.
        self.assertEqual(
            UNIQUE_TOURNAMENT_GROUPS_ENDPOINT.origin,
            EndpointOrigin.FEDERATED,
        )


if __name__ == "__main__":
    unittest.main()
