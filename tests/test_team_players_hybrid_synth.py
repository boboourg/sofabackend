"""TDD tests for Phase 2.2 (Item 2) — hybrid synthesizer for
``/team/{team_id}/players``.

Same hybrid model as Phase 2.3 (transfers): snapshot primary +
minimal synthesizer fallback. Snapshot serves the full Sofascore
envelope ``{"players": [...], "foreignPlayers": [...],
"nationalPlayers": [...]}``; the fallback emits ``{"players": [
{"player": {id, name, slug, position, jerseyNumber}}, ...]}``
from ``event_lineup_player`` rows seen in this team's recent
matches.

The fallback intentionally drops the ``foreignPlayers`` /
``nationalPlayers`` partitions: those are editorial categories
keyed on player.country == team.country and the partition logic
isn't simple "filter by country" (Sofascore considers naturalised
players, dual-nationality cases, etc.). When the snapshot is fresh,
all three partitions are served 1:1; when synthesizing, only the
basic ``players`` list is meaningful.

Data source for the fallback: ``event_lineup_player.team_id`` JOIN
``player`` filtered to lineup entries from the last 60 days (cutoff
keeps the squad list current — players who left long ago drop off).
"""

from __future__ import annotations

import unittest


class FetchTeamSquadRowsContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_passes_team_id_as_only_positional_arg(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_squad_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_team_squad_rows(_StubConn(), team_id=42)
        # team_id is the first arg; lookback cutoff is the second
        # (computed from NOW() - 60d in seconds-since-epoch).
        self.assertEqual(captured["args"][0], 42)
        self.assertIsInstance(captured["args"][1], int)
        # Cutoff is positive (a timestamp in seconds, well past 0).
        self.assertGreater(int(captured["args"][1]), 1_000_000_000)

    async def test_query_joins_event_lineup_player_with_player(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_squad_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_team_squad_rows(_StubConn(), team_id=42)
        query = str(captured["query"])
        self.assertIn("event_lineup_player", query)
        self.assertIn("player", query)
        self.assertIn("event", query)

    async def test_query_filters_by_team_and_recent_lineup_cutoff(self) -> None:
        """The squad list reflects recent activity — players seen in
        lineups within the last 60 days. Pre-60-day cutoff filters
        out players who left the club months ago."""
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_squad_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_team_squad_rows(_StubConn(), team_id=42)
        query = str(captured["query"])
        self.assertIn("team_id = $1", query)
        self.assertIn("start_timestamp", query)


class BuildTeamPlayersPayloadTests(unittest.TestCase):
    def test_empty_rows_returns_empty_envelope(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_players_payload,
        )

        self.assertEqual(build_team_players_payload([]), {"players": []})

    def test_player_object_carries_minimal_fields(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_players_payload,
        )

        rows = [
            {
                "player_id": 287643,
                "player_name": "Oleksandr Zinchenko",
                "player_slug": "oleksandr-zinchenko",
                "position": "D",
                "jersey_number": "47",
            },
        ]
        payload = build_team_players_payload(rows)
        self.assertEqual(len(payload["players"]), 1)
        entry = payload["players"][0]
        # Wire format wraps the player in a ``player`` key (matches
        # Sofascore's ``{"players": [{"player": {...}}]}`` shape).
        self.assertIn("player", entry)
        self.assertEqual(entry["player"]["id"], 287643)
        self.assertEqual(entry["player"]["name"], "Oleksandr Zinchenko")
        self.assertEqual(entry["player"]["slug"], "oleksandr-zinchenko")
        self.assertEqual(entry["player"]["position"], "D")
        self.assertEqual(entry["player"]["jerseyNumber"], "47")

    def test_dedupes_by_player_id(self) -> None:
        """The SQL query already DISTINCTs on player_id but the
        builder repeats the guard so a hand-constructed input doesn't
        emit duplicates."""
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_players_payload,
        )

        rows = [
            {
                "player_id": 1,
                "player_name": "Alice",
                "player_slug": "alice",
                "position": "M",
                "jersey_number": "10",
            },
            {
                "player_id": 1,
                "player_name": "Alice",
                "player_slug": "alice",
                "position": "M",
                "jersey_number": "10",
            },
            {
                "player_id": 2,
                "player_name": "Bob",
                "player_slug": "bob",
                "position": "F",
                "jersey_number": "9",
            },
        ]
        payload = build_team_players_payload(rows)
        ids = sorted(entry["player"]["id"] for entry in payload["players"])
        self.assertEqual(ids, [1, 2])

    def test_skips_rows_without_player_id(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_players_payload,
        )

        rows = [
            {
                "player_id": None,
                "player_name": "Ghost",
                "player_slug": "ghost",
                "position": "G",
                "jersey_number": "1",
            },
            {
                "player_id": 5,
                "player_name": "Real",
                "player_slug": "real",
                "position": "G",
                "jersey_number": "1",
            },
        ]
        payload = build_team_players_payload(rows)
        self.assertEqual(len(payload["players"]), 1)
        self.assertEqual(payload["players"][0]["player"]["id"], 5)


class LocalApiTeamPlayersHybridTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_snapshot_when_available(self) -> None:
        """Snapshot path is unchanged from pre-Item-2 behaviour — the
        nested ``foreignPlayers`` / ``nationalPlayers`` partitions
        ride through untouched."""
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _snapshot(**kwargs):
            return {
                "players": [{"player": {"id": 1, "name": "X"}}],
                "foreignPlayers": [{"player": {"id": 1, "name": "X"}}],
                "nationalPlayers": [],
            }

        application._fetch_latest_entity_passthrough = _snapshot

        result = await application._fetch_team_players_payload(42)
        self.assertIn("foreignPlayers", result)
        self.assertIn("nationalPlayers", result)

    async def test_returns_synthesized_envelope_when_no_snapshot(self) -> None:
        """Phase 2.2 fallback: minimal ``{"players": [...]}`` envelope
        — frontend list still renders, partitions are absent (logged
        trade-off)."""
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _no_snapshot(**kwargs):
            return None

        application._fetch_latest_entity_passthrough = _no_snapshot

        class _StubConn:
            def __init__(self) -> None:
                self.closed = False

            async def fetch(self, query: str, *args: object):
                return [
                    {
                        "player_id": 287643,
                        "player_name": "Oleksandr Zinchenko",
                        "player_slug": "oleksandr-zinchenko",
                        "position": "D",
                        "jersey_number": "47",
                    },
                ]

            async def close(self) -> None:
                self.closed = True

        async def _connect():
            return _StubConn()

        application._connect = _connect

        result = await application._fetch_team_players_payload(42)
        self.assertEqual(len(result["players"]), 1)
        self.assertEqual(result["players"][0]["player"]["id"], 287643)
        # Synth path doesn't include the editorial partitions.
        self.assertNotIn("foreignPlayers", result)
        self.assertNotIn("nationalPlayers", result)

    async def test_returns_empty_envelope_when_db_unreachable(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _no_snapshot(**kwargs):
            return None

        application._fetch_latest_entity_passthrough = _no_snapshot

        async def _connect():
            raise RuntimeError("db down")

        application._connect = _connect

        result = await application._fetch_team_players_payload(42)
        self.assertEqual(result, {"players": []})


if __name__ == "__main__":
    unittest.main()
