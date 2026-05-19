"""TDD tests for Phase 2.3 (Item 2) — hybrid synthesizer for
``/team/{team_id}/transfers``.

Hybrid model
------------
* **Primary**: existing raw passthrough from ``api_payload_snapshot``.
  When fresh, the local API serves the canonical Sofascore wire
  envelope including deeply nested fields (fieldTranslations, sport,
  country, teamColors, …) we don't normalize.
* **Fallback** (Phase 2.3): when no snapshot exists for the team, the
  synthesizer assembles a *minimal* Sofascore-shape envelope from
  ``player_transfer_history`` JOIN ``player`` JOIN ``team``. The
  output covers the fields frontend lists actually use (player id,
  name, slug; transfer fee/date; from/to team ids+names) and
  omits the rich editorial nesting that's snapshot-only.

This is the explicit trade-off the user accepted when approving
Item 2: 1:1 fidelity stays available when we have the snapshot;
when missing, we produce a usable-but-trimmed response instead of
404.

Schema
------
``player_transfer_history`` columns: id, player_id,
transfer_from_team_id, transfer_to_team_id, from_team_name,
to_team_name, transfer_date_timestamp, transfer_fee,
transfer_fee_description, transfer_fee_raw (JSONB), type.

Direction split:
  transfer_to_team_id   = X  → goes into ``transfersIn``
  transfer_from_team_id = X  → goes into ``transfersOut``
"""

from __future__ import annotations

import unittest


class FetchTeamTransfersRowsContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_passes_team_id_as_only_arg(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_transfers_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_team_transfers_rows(_StubConn(), team_id=42)
        self.assertEqual(captured["args"], (42,))

    async def test_query_joins_transfer_history_with_player_and_team(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_transfers_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_team_transfers_rows(_StubConn(), team_id=42)
        query = str(captured["query"])
        self.assertIn("player_transfer_history", query)
        self.assertIn("player", query)
        # Both direction filters present so a single query covers IN + OUT.
        self.assertIn("transfer_to_team_id", query)
        self.assertIn("transfer_from_team_id", query)

    async def test_query_orders_by_transfer_date_desc(self) -> None:
        """Sofascore presents most recent transfers first — pin DESC
        order so the synthesised envelope matches when both paths land."""
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_transfers_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_team_transfers_rows(_StubConn(), team_id=42)
        query = str(captured["query"])
        self.assertRegex(query, r"ORDER BY.*transfer_date_timestamp.*DESC")


class BuildTeamTransfersPayloadTests(unittest.TestCase):
    def test_empty_rows_returns_empty_envelope(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_transfers_payload,
        )

        self.assertEqual(
            build_team_transfers_payload([], team_id=42),
            {"transfersIn": [], "transfersOut": []},
        )

    def test_transfer_in_when_to_team_matches(self) -> None:
        """A row whose ``transfer_to_team_id == team_id`` goes into
        ``transfersIn``. Direction split is the only structural logic
        in the builder."""
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_transfers_payload,
        )

        rows = [
            {
                "id": 2719436,
                "type": 2,
                "player_id": 287643,
                "player_name": "Oleksandr Zinchenko",
                "player_slug": "oleksandr-zinchenko",
                "transfer_from_team_id": 1644,
                "from_team_name": "Manchester City",
                "transfer_to_team_id": 42,
                "to_team_name": "Arsenal",
                "transfer_date_timestamp": 1660003200,
                "transfer_fee": 35_000_000,
                "transfer_fee_description": "€35M",
            },
        ]
        payload = build_team_transfers_payload(rows, team_id=42)
        self.assertEqual(len(payload["transfersIn"]), 1)
        self.assertEqual(payload["transfersOut"], [])
        entry = payload["transfersIn"][0]
        self.assertEqual(entry["id"], 2719436)
        # Minimal player object — id/name/slug only (rich fields are
        # snapshot-only).
        self.assertEqual(entry["player"]["id"], 287643)
        self.assertEqual(entry["player"]["name"], "Oleksandr Zinchenko")
        self.assertEqual(entry["player"]["slug"], "oleksandr-zinchenko")
        self.assertEqual(entry["fromTeamName"], "Manchester City")
        self.assertEqual(entry["toTeamName"], "Arsenal")
        self.assertEqual(entry["transferDateTimestamp"], 1660003200)
        self.assertEqual(entry["transferFee"], 35_000_000)
        self.assertEqual(entry["transferFeeDescription"], "€35M")

    def test_transfer_out_when_from_team_matches(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_transfers_payload,
        )

        rows = [
            {
                "id": 2900100,
                "type": 1,
                "player_id": 580001,
                "player_name": "John Doe",
                "player_slug": "john-doe",
                "transfer_from_team_id": 42,
                "from_team_name": "Arsenal",
                "transfer_to_team_id": 6,
                "to_team_name": "Burnley",
                "transfer_date_timestamp": 1700000000,
                "transfer_fee": 0,
                "transfer_fee_description": "Free transfer",
            },
        ]
        payload = build_team_transfers_payload(rows, team_id=42)
        self.assertEqual(payload["transfersIn"], [])
        self.assertEqual(len(payload["transfersOut"]), 1)
        self.assertEqual(payload["transfersOut"][0]["fromTeamName"], "Arsenal")
        self.assertEqual(payload["transfersOut"][0]["toTeamName"], "Burnley")

    def test_mixed_direction_rows(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_transfers_payload,
        )

        rows = [
            {
                "id": 1,
                "type": 2,
                "player_id": 100,
                "player_name": "Player A",
                "player_slug": "player-a",
                "transfer_from_team_id": 99,
                "from_team_name": "Other",
                "transfer_to_team_id": 42,
                "to_team_name": "Arsenal",
                "transfer_date_timestamp": 1700000000,
                "transfer_fee": 1,
                "transfer_fee_description": "—",
            },
            {
                "id": 2,
                "type": 1,
                "player_id": 200,
                "player_name": "Player B",
                "player_slug": "player-b",
                "transfer_from_team_id": 42,
                "from_team_name": "Arsenal",
                "transfer_to_team_id": 99,
                "to_team_name": "Other",
                "transfer_date_timestamp": 1700000001,
                "transfer_fee": 2,
                "transfer_fee_description": "—",
            },
        ]
        payload = build_team_transfers_payload(rows, team_id=42)
        self.assertEqual(len(payload["transfersIn"]), 1)
        self.assertEqual(len(payload["transfersOut"]), 1)
        self.assertEqual(payload["transfersIn"][0]["player"]["id"], 100)
        self.assertEqual(payload["transfersOut"][0]["player"]["id"], 200)

    def test_rows_with_neither_direction_match_dropped(self) -> None:
        """Defensive: a row that has team_id in neither direction
        column (data integrity anomaly — shouldn't happen given the
        SQL filter but pin the behavior)."""
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_transfers_payload,
        )

        rows = [
            {
                "id": 999,
                "type": 0,
                "player_id": 1,
                "player_name": "X",
                "player_slug": "x",
                "transfer_from_team_id": 100,
                "from_team_name": "A",
                "transfer_to_team_id": 200,
                "to_team_name": "B",
                "transfer_date_timestamp": 0,
                "transfer_fee": 0,
                "transfer_fee_description": "",
            },
        ]
        payload = build_team_transfers_payload(rows, team_id=42)
        self.assertEqual(payload, {"transfersIn": [], "transfersOut": []})


class LocalApiTeamTransfersHybridTests(unittest.IsolatedAsyncioTestCase):
    """The dispatcher tries raw passthrough first; on miss falls back
    to the synthesizer. Both paths produce a Sofascore-shape envelope
    with the same top-level keys."""

    async def test_returns_synthesized_envelope_when_no_snapshot(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _no_snapshot(**kwargs):
            return None  # no raw passthrough available

        application._fetch_latest_entity_passthrough = _no_snapshot

        class _StubConn:
            def __init__(self) -> None:
                self.closed = False

            async def fetch(self, query: str, *args: object):
                return [
                    {
                        "id": 1,
                        "type": 2,
                        "player_id": 100,
                        "player_name": "Synth Player",
                        "player_slug": "synth-player",
                        "transfer_from_team_id": 99,
                        "from_team_name": "Other",
                        "transfer_to_team_id": 42,
                        "to_team_name": "Arsenal",
                        "transfer_date_timestamp": 1700000000,
                        "transfer_fee": 1,
                        "transfer_fee_description": "—",
                    }
                ]

            async def close(self) -> None:
                self.closed = True

        async def _connect():
            return _StubConn()

        application._connect = _connect

        result = await application._fetch_team_transfers_payload(42)
        self.assertIsNotNone(result)
        # Envelope keys are Sofascore-canonical.
        self.assertIn("transfersIn", result)
        self.assertIn("transfersOut", result)
        self.assertEqual(len(result["transfersIn"]), 1)

    async def test_returns_snapshot_when_available(self) -> None:
        """When a raw snapshot exists, the dispatcher returns it
        as-is (1:1 wire fidelity with all the editorial nesting)."""
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _snapshot(**kwargs):
            return {
                "transfersIn": [{"id": 999, "rich": "nested-data"}],
                "transfersOut": [],
            }

        application._fetch_latest_entity_passthrough = _snapshot

        result = await application._fetch_team_transfers_payload(42)
        self.assertEqual(result["transfersIn"][0]["id"], 999)
        # Rich nested key only present in snapshot path — synthesizer
        # wouldn't emit it.
        self.assertEqual(result["transfersIn"][0]["rich"], "nested-data")

    async def test_returns_empty_envelope_when_db_unreachable(self) -> None:
        """No snapshot + no DB connection = ``{"transfersIn": [],
        "transfersOut": []}`` not 500."""
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _no_snapshot(**kwargs):
            return None

        application._fetch_latest_entity_passthrough = _no_snapshot

        async def _connect():
            raise RuntimeError("db down")

        application._connect = _connect

        result = await application._fetch_team_transfers_payload(42)
        self.assertEqual(result, {"transfersIn": [], "transfersOut": []})


if __name__ == "__main__":
    unittest.main()
