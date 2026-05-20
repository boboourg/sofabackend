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


class BuildTeamOfTheWeekPayloadRichFieldsTests(unittest.TestCase):
    """B2 (2026-05-20): synth payload extended to match upstream
    Sofascore wire shape for player + team nested fields.

    Upstream entry shape (verified via InspectorTransport):
        {
            "id": <entry_id>,         ← top-level (NEW)
            "order": <int>,            ← top-level (NEW)
            "type": "rated",           ← top-level literal (NEW)
            "rating": "...",
            "player": {id, name, slug, shortName, userCount, position,
                       jerseyNumber, gender, sofascoreId, fieldTranslations},
            "team": {id, name, slug, shortName, nameCode, gender,
                     userCount, type, national, disabled, teamColors,
                     fieldTranslations, sport: {id, name, slug}},
        }

    ``event`` nested NOT covered in B2 (requires schema migration —
    see B3). Snapshot path preserves event 1:1 when present.
    """

    def test_entry_carries_top_level_id_order_and_rated_type(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_of_the_week_payload,
        )

        rows = [
            {
                "formation": "4-3-3", "entry_id": 99, "order_value": 1,
                "rating": "8.7", "player_id": 1, "player_name": "X",
                "player_slug": "x", "team_id": 2, "team_name": "T",
                "team_slug": "t", "team_short_name": "T",
                "team_name_code": "TT", "team_gender": "M",
            },
        ]
        payload = build_team_of_the_week_payload(rows)
        entry = payload["players"][0]
        self.assertEqual(entry["id"], 99)
        self.assertEqual(entry["order"], 1)
        # Upstream returns literal "rated" for every player in ToTW.
        self.assertEqual(entry["type"], "rated")

    def test_player_nested_carries_rich_fields(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_of_the_week_payload,
        )

        rows = [
            {
                "formation": "4-3-3", "entry_id": 1, "order_value": 1,
                "rating": "8.7", "player_id": 287643,
                "player_name": "Oleksandr Zinchenko",
                "player_slug": "oleksandr-zinchenko",
                "player_short_name": "O. Zinchenko",
                "player_user_count": 12345,
                "player_position": "D",
                "player_jersey_number": "47",
                "player_gender": "M",
                "player_sofascore_id": "abc123",
                "player_field_translations": {
                    "nameTranslation": {"ru": "Зинченко"}
                },
                "team_id": 42, "team_name": "Arsenal", "team_slug": "arsenal",
                "team_short_name": "Arsenal", "team_name_code": "ARS",
                "team_gender": "M",
            },
        ]
        payload = build_team_of_the_week_payload(rows)
        p = payload["players"][0]["player"]
        self.assertEqual(p["shortName"], "O. Zinchenko")
        self.assertEqual(p["userCount"], 12345)
        self.assertEqual(p["position"], "D")
        self.assertEqual(p["jerseyNumber"], "47")
        self.assertEqual(p["gender"], "M")
        self.assertEqual(p["sofascoreId"], "abc123")
        self.assertEqual(
            p["fieldTranslations"],
            {"nameTranslation": {"ru": "Зинченко"}},
        )

    def test_team_nested_carries_rich_fields_and_sport(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_of_the_week_payload,
        )

        rows = [
            {
                "formation": "4-3-3", "entry_id": 1, "order_value": 1,
                "rating": "8.7", "player_id": 100,
                "player_name": "A", "player_slug": "a",
                "team_id": 2697, "team_name": "Inter", "team_slug": "inter",
                "team_short_name": "Inter", "team_name_code": "INT",
                "team_gender": "M",
                "team_user_count": 1973685,
                "team_type": 0,
                "team_national": False,
                "team_disabled": False,
                "team_colors": {
                    "primary": "#1a57cc",
                    "secondary": "#000000",
                    "text": "#000000",
                },
                "team_field_translations": {
                    "nameTranslation": {"ru": "Интер"}
                },
                "sport_id": 1, "sport_name": "Football", "sport_slug": "football",
            },
        ]
        payload = build_team_of_the_week_payload(rows)
        t = payload["players"][0]["team"]
        self.assertEqual(t["userCount"], 1973685)
        self.assertEqual(t["type"], 0)
        self.assertEqual(t["national"], False)
        self.assertEqual(t["disabled"], False)
        self.assertEqual(
            t["teamColors"],
            {"primary": "#1a57cc", "secondary": "#000000", "text": "#000000"},
        )
        self.assertEqual(
            t["fieldTranslations"],
            {"nameTranslation": {"ru": "Интер"}},
        )
        self.assertEqual(
            t["sport"],
            {"id": 1, "name": "Football", "slug": "football"},
        )

    def test_jsonb_strings_are_decoded(self) -> None:
        """asyncpg can return JSONB as already-decoded dicts OR as raw
        strings depending on codec settings. Builder should handle both."""
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_of_the_week_payload,
        )

        rows = [
            {
                "formation": "4-3-3", "entry_id": 1, "order_value": 1,
                "rating": "8.5", "player_id": 1,
                "player_name": "A", "player_slug": "a",
                # JSONB as raw string (some envs)
                "player_field_translations": '{"nameTranslation": {"ru": "А"}}',
                "team_id": 2, "team_name": "T", "team_slug": "t",
                "team_short_name": "T", "team_name_code": "T",
                "team_gender": "M",
                "team_colors": '{"primary": "#000000"}',
            },
        ]
        payload = build_team_of_the_week_payload(rows)
        p = payload["players"][0]["player"]
        t = payload["players"][0]["team"]
        # Strings should be decoded to dicts.
        self.assertEqual(p["fieldTranslations"], {"nameTranslation": {"ru": "А"}})
        self.assertEqual(t["teamColors"], {"primary": "#000000"})


class FetchTeamOfTheWeekRichSqlContractTests(unittest.IsolatedAsyncioTestCase):
    """Pin the SQL JOIN graph for the rich fetch — must include
    player rich columns + team rich columns + sport JOIN."""

    async def test_query_selects_player_rich_columns(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_of_the_week_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_team_of_the_week_rows(_StubConn(), period_id=19131)
        q = str(captured["query"])
        self.assertIn("p.short_name", q)
        self.assertIn("p.user_count", q)
        self.assertIn("p.position", q)
        self.assertIn("p.jersey_number", q)
        self.assertIn("p.gender", q)
        self.assertIn("p.sofascore_id", q)
        self.assertIn("p.field_translations", q)

    async def test_query_selects_team_rich_columns(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_of_the_week_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_team_of_the_week_rows(_StubConn(), period_id=19131)
        q = str(captured["query"])
        self.assertIn("t.user_count", q)
        self.assertIn("t.type", q)
        self.assertIn("t.national", q)
        self.assertIn("t.disabled", q)
        self.assertIn("t.team_colors", q)
        self.assertIn("t.field_translations", q)

    async def test_query_joins_sport_via_team_sport_id(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_of_the_week_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_team_of_the_week_rows(_StubConn(), period_id=19131)
        q = str(captured["query"])
        self.assertRegex(q, r"sport\s+\w+\s+ON\s+\w+\.id\s*=\s*t\.sport_id")
        self.assertIn(".name", q)


class BuildTeamOfTheWeekEventNestedTests(unittest.TestCase):
    """B3 (2026-05-20): synth now emits ``event`` nested per player.

    Requires the migration adding team_of_the_week_player.event_id
    and the parser update extracting item.event.id. SQL JOIN-s event
    + event_status + event_score + team (for home/away). Builder
    emits the event nested only when event_id is non-NULL.
    """

    def test_event_nested_emitted_when_event_id_present(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_of_the_week_payload,
        )

        rows = [
            {
                "formation": "4-2-3-1", "entry_id": 1, "order_value": 1,
                "rating": "8.7", "player_id": 100, "player_name": "X",
                "player_slug": "x", "team_id": 200, "team_name": "T",
                "team_slug": "t", "team_short_name": "T",
                "team_name_code": "T", "team_gender": "M",
                "event_id": 12345,
                "event_slug": "psg-inter",
                "event_custom_id": "abc",
                "event_start_timestamp": 1748707200,
                "event_has_xg": True,
                "event_final_result_only": False,
                "event_home_team_id": 1644,
                "event_home_team_name": "Paris Saint-Germain",
                "event_home_team_slug": "paris-saint-germain",
                "event_home_team_short_name": "PSG",
                "event_home_team_name_code": "PSG",
                "event_away_team_id": 2697,
                "event_away_team_name": "Inter",
                "event_away_team_slug": "inter",
                "event_away_team_short_name": "Inter",
                "event_away_team_name_code": "INT",
                "event_status_code": 100,
                "event_status_type": "finished",
                "event_status_description": "Ended",
                "event_home_score_current": 5,
                "event_home_score_display": 5,
                "event_home_score_normaltime": 5,
                "event_home_score_period1": 3,
                "event_home_score_period2": 2,
                "event_away_score_current": 0,
                "event_away_score_display": 0,
                "event_away_score_normaltime": 0,
                "event_away_score_period1": 0,
                "event_away_score_period2": 0,
            },
        ]
        payload = build_team_of_the_week_payload(rows)
        entry = payload["players"][0]
        self.assertIn("event", entry)
        ev = entry["event"]
        self.assertEqual(ev["id"], 12345)
        self.assertEqual(ev["slug"], "psg-inter")
        self.assertEqual(ev["customId"], "abc")
        self.assertEqual(ev["startTimestamp"], 1748707200)
        self.assertEqual(ev["hasXg"], True)
        self.assertEqual(ev["status"]["type"], "finished")
        self.assertEqual(ev["status"]["description"], "Ended")
        self.assertEqual(ev["homeTeam"]["name"], "Paris Saint-Germain")
        self.assertEqual(ev["homeTeam"]["nameCode"], "PSG")
        self.assertEqual(ev["awayTeam"]["name"], "Inter")
        self.assertEqual(ev["homeScore"]["current"], 5)
        self.assertEqual(ev["awayScore"]["current"], 0)
        # eventState is an empty dict in upstream (live flags consolidated
        # there but absent for finished events).
        self.assertEqual(ev["eventState"], {})

    def test_event_nested_omitted_when_event_id_null(self) -> None:
        """Backward compat: legacy rows pre-migration have event_id=NULL.
        Builder must NOT emit an empty ``event`` for those."""
        from schema_inspector.scheduled_events_synthesizer import (
            build_team_of_the_week_payload,
        )

        rows = [
            {
                "formation": "4-3-3", "entry_id": 1, "order_value": 1,
                "rating": "8.5", "player_id": 1, "player_name": "A",
                "player_slug": "a", "team_id": 2, "team_name": "T",
                "team_slug": "t", "team_short_name": "T",
                "team_name_code": "T", "team_gender": "M",
                "event_id": None,
            },
        ]
        payload = build_team_of_the_week_payload(rows)
        self.assertNotIn("event", payload["players"][0])


class FetchTeamOfTheWeekEventSqlContractTests(unittest.IsolatedAsyncioTestCase):
    """SQL must JOIN event + event_status + event_score + team for
    nested event hydration."""

    async def test_query_joins_event_via_event_id(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_of_the_week_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_team_of_the_week_rows(_StubConn(), period_id=19131)
        q = str(captured["query"])
        # event + status + score + home/away team JOINs
        self.assertRegex(q, r"event\s+ev\s+ON\s+ev\.id\s*=\s*totwp\.event_id")
        self.assertIn("event_status", q)
        self.assertIn("event_score", q)
        self.assertIn("totwp.event_id", q)

    async def test_query_selects_event_columns(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_team_of_the_week_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_team_of_the_week_rows(_StubConn(), period_id=19131)
        q = str(captured["query"])
        self.assertIn("ev.slug", q)
        self.assertIn("ev.custom_id", q)
        self.assertIn("ev.start_timestamp", q)
        self.assertIn("ev.home_team_id", q)
        self.assertIn("ev.away_team_id", q)


if __name__ == "__main__":
    unittest.main()
