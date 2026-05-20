"""TDD tests for hybrid synthesizer of
``/unique-tournament/{ut}/season/{s}/team-of-the-week/{period_id}``.

Same hybrid model as Item 2 (team/players, transfers): snapshot primary
+ synthesizer fallback over normalized tables. The pre-fix generic
dispatcher was returning only the bare ``team_of_the_week`` row
wrapped in an array (``{teamOfTheWeeks: [{formation, periodId}]}``)
because the implicit envelope assembly stopped at the parent table
and never JOIN-ed ``team_of_the_week_player`` for the actual lineup.

The synthesizer here emits the flat Sofascore wire shape
``{formation, players: [{player, team, rating}, ...]}`` (without the
``teamOfTheWeeks`` outer wrapper). Snapshot path is preserved 1:1
with upstream (which carries rich nested editorial like
``team.fieldTranslations``, ``player.country``, ``event``, etc.).

Schema involved:
  team_of_the_week        : period_id (PK), formation
  team_of_the_week_player : period_id, entry_id, player_id, team_id,
                            order_value, rating
  player, team            : nested envelope fields
"""

from __future__ import annotations

import unittest


class FetchTeamOfTheWeekRowsContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_passes_period_id_as_keyword_arg(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_of_the_week_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_team_of_the_week_rows(_StubConn(), period_id=19131)
        self.assertEqual(captured["args"][0], 19131)

    async def test_query_joins_totw_player_team(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_of_the_week_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_team_of_the_week_rows(_StubConn(), period_id=19131)
        query = str(captured["query"])
        self.assertIn("team_of_the_week", query)
        self.assertIn("team_of_the_week_player", query)
        self.assertIn("player", query)
        self.assertIn("team", query)

    async def test_query_orders_by_order_value(self) -> None:
        """Sofascore returns players in their formation slot order
        (1..11). The synth must preserve this for layout-aware UI."""
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_of_the_week_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_team_of_the_week_rows(_StubConn(), period_id=19131)
        query = str(captured["query"])
        self.assertRegex(query, r"ORDER BY.*order_value")


class BuildTeamOfTheWeekPayloadTests(unittest.TestCase):
    def test_empty_rows_returns_empty_envelope(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_of_the_week_payload,
        )

        # Empty envelope is ``{"players": []}`` (without formation) —
        # consistent with the DB-down fallback in the dispatcher.
        self.assertEqual(build_team_of_the_week_payload([]), {"players": []})

    def test_formation_extracted_from_first_row(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_of_the_week_payload,
        )

        rows = [
            {
                "formation": "4-2-3-1",
                "entry_id": 1,
                "order_value": 1,
                "rating": "8.5",
                "player_id": 100,
                "player_name": "P1",
                "player_slug": "p1",
                "team_id": 200,
                "team_name": "T1",
                "team_slug": "t1",
                "team_short_name": "T1",
                "team_name_code": "TT1",
                "team_gender": "M",
            },
        ]
        result = build_team_of_the_week_payload(rows)
        self.assertEqual(result["formation"], "4-2-3-1")

    def test_player_object_carries_nested_player_and_team(self) -> None:
        """Flat Sofascore wire shape: each entry has ``player`` +
        ``team`` + ``rating`` (no outer ``teamOfTheWeeks`` wrapper).
        """
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_of_the_week_payload,
        )

        rows = [
            {
                "formation": "4-3-3",
                "entry_id": 1,
                "order_value": 1,
                "rating": "8.7",
                "player_id": 287643,
                "player_name": "Oleksandr Zinchenko",
                "player_slug": "oleksandr-zinchenko",
                "team_id": 42,
                "team_name": "Arsenal",
                "team_slug": "arsenal",
                "team_short_name": "Arsenal",
                "team_name_code": "ARS",
                "team_gender": "M",
            },
        ]
        result = build_team_of_the_week_payload(rows)
        self.assertEqual(len(result["players"]), 1)
        entry = result["players"][0]
        self.assertIn("player", entry)
        self.assertIn("team", entry)
        self.assertEqual(entry["player"]["id"], 287643)
        self.assertEqual(entry["player"]["name"], "Oleksandr Zinchenko")
        self.assertEqual(entry["player"]["slug"], "oleksandr-zinchenko")
        self.assertEqual(entry["team"]["id"], 42)
        self.assertEqual(entry["team"]["name"], "Arsenal")
        self.assertEqual(entry["team"]["slug"], "arsenal")
        self.assertEqual(entry["team"]["shortName"], "Arsenal")
        self.assertEqual(entry["team"]["nameCode"], "ARS")
        self.assertEqual(entry["team"]["gender"], "M")
        self.assertEqual(entry["rating"], "8.7")

    def test_players_sorted_by_order_value(self) -> None:
        """The SQL ORDER BY already sorts, but builder repeats the
        sort so a hand-constructed input still yields stable output."""
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_of_the_week_payload,
        )

        rows = [
            {
                "formation": "4-3-3", "entry_id": 2, "order_value": 2,
                "rating": "7.5", "player_id": 200, "player_name": "B",
                "player_slug": "b", "team_id": 1, "team_name": "T",
                "team_slug": "t", "team_short_name": "T",
                "team_name_code": "T", "team_gender": "M",
            },
            {
                "formation": "4-3-3", "entry_id": 1, "order_value": 1,
                "rating": "8.5", "player_id": 100, "player_name": "A",
                "player_slug": "a", "team_id": 1, "team_name": "T",
                "team_slug": "t", "team_short_name": "T",
                "team_name_code": "T", "team_gender": "M",
            },
        ]
        result = build_team_of_the_week_payload(rows)
        ids = [p["player"]["id"] for p in result["players"]]
        self.assertEqual(ids, [100, 200])

    def test_skips_rows_with_missing_player_id(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_of_the_week_payload,
        )

        rows = [
            {
                "formation": "4-3-3", "entry_id": 1, "order_value": 1,
                "rating": "8.5", "player_id": None, "player_name": "Ghost",
                "player_slug": "ghost", "team_id": 1, "team_name": "T",
                "team_slug": "t", "team_short_name": "T",
                "team_name_code": "T", "team_gender": "M",
            },
            {
                "formation": "4-3-3", "entry_id": 2, "order_value": 2,
                "rating": "9.0", "player_id": 5, "player_name": "Real",
                "player_slug": "real", "team_id": 1, "team_name": "T",
                "team_slug": "t", "team_short_name": "T",
                "team_name_code": "T", "team_gender": "M",
            },
        ]
        result = build_team_of_the_week_payload(rows)
        self.assertEqual(len(result["players"]), 1)
        self.assertEqual(result["players"][0]["player"]["id"], 5)


class LocalApiTeamOfTheWeekHybridTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_snapshot_when_available(self) -> None:
        """Snapshot path is preserved unchanged — when fresh, the
        rich Sofascore wire payload (with editorial fieldTranslations,
        event, teamColors, country, etc.) is served as-is."""
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _snapshot(**kwargs):
            return {
                "formation": "4-2-3-1",
                "players": [
                    {
                        "player": {"id": 1, "name": "X"},
                        "team": {"id": 2, "name": "Y", "teamColors": {"primary": "#000"}},
                        "event": {"id": 333},
                        "rating": "8.0",
                    }
                ],
            }

        application._fetch_latest_entity_passthrough = _snapshot

        result = await application._fetch_team_of_the_week_payload(7, 61644, 19131)
        self.assertEqual(result["formation"], "4-2-3-1")
        self.assertEqual(len(result["players"]), 1)
        # Editorial fields preserved by snapshot path.
        self.assertIn("event", result["players"][0])
        self.assertIn("teamColors", result["players"][0]["team"])

    async def test_returns_synthesized_when_no_snapshot(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _no_snapshot(**kwargs):
            return None

        application._fetch_latest_entity_passthrough = _no_snapshot

        class _StubConn:
            def __init__(self) -> None:
                self.closed = False

            async def fetch(self, query, *args):
                return [
                    {
                        "formation": "4-2-3-1",
                        "entry_id": 1,
                        "order_value": 1,
                        "rating": "8.7",
                        "player_id": 287643,
                        "player_name": "Oleksandr Zinchenko",
                        "player_slug": "oleksandr-zinchenko",
                        "team_id": 42,
                        "team_name": "Arsenal",
                        "team_slug": "arsenal",
                        "team_short_name": "Arsenal",
                        "team_name_code": "ARS",
                        "team_gender": "M",
                    },
                ]

            async def close(self) -> None:
                self.closed = True

        async def _connect():
            return _StubConn()

        application._connect = _connect

        result = await application._fetch_team_of_the_week_payload(7, 61644, 19131)
        self.assertEqual(result["formation"], "4-2-3-1")
        self.assertEqual(len(result["players"]), 1)
        self.assertEqual(result["players"][0]["player"]["id"], 287643)
        # Synth path does not include event / editorial fields.
        self.assertNotIn("event", result["players"][0])

    async def test_returns_empty_envelope_when_db_unreachable(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _no_snapshot(**kwargs):
            return None

        application._fetch_latest_entity_passthrough = _no_snapshot

        async def _connect():
            raise RuntimeError("db down")

        application._connect = _connect

        result = await application._fetch_team_of_the_week_payload(7, 61644, 19131)
        self.assertEqual(result, {"players": []})


if __name__ == "__main__":
    unittest.main()
