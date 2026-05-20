"""TDD tests for hybrid synthesizer of
``/unique-tournament/{ut}/season/{s}/cuptrees``.

Phase B (теория ре-аудита 2026-05-20): the upstream ``/cuptrees``
fetcher already lands bracket structure in the 4 normalized tables
(``season_cup_tree``, ``_round``, ``_block``, ``_participant``) via
Item 4 (2026-05-19) pre-fetch scope. But local API has no dedicated
handler — endpoint relies on raw snapshot passthrough only, which
returns 404 when no snapshot exists (e.g. archived seasons whose
snapshot was retention-purged).

This synth assembles the flat Sofascore wire shape
``{"cupTrees": [{"id", "name", "currentRound", "rounds": [{"order",
"type", "description", "blocks": [{"id", "blockId", "order",
"finished", "matchesInRound", "result", "homeTeamScore",
"awayTeamScore", "hasNextRoundLink", "seriesStartDateTimestamp",
"automaticProgression", "events": [{"id": E}], "participants":
[{"id", "order", "winner", "team": {id, name, slug, shortName,
nameCode, gender}}]}]}]}]}`` from the normalized tables.

Snapshot path still serves rich 1:1 (tournament nested, teamColors,
fieldTranslations, sport, country, etc.). Synth omits those —
minimal but renderable bracket UI.

Schema involved:
  season_cup_tree            : cup_tree_id, ut_id, season_id, tournament_id, name, current_round
  season_cup_tree_round      : cup_tree_id, round_order, round_type, description
  season_cup_tree_block      : entry_id (PK), cup_tree_id, round_order, block_id, block_order,
                               finished, matches_in_round, result,
                               home_team_score, away_team_score, has_next_round_link,
                               series_start_date_timestamp, automatic_progression,
                               event_ids_json (jsonb)
  season_cup_tree_participant: participant_id (PK), entry_id, team_id, order_value, winner
  team                       : nested envelope fields
"""

from __future__ import annotations

import unittest


class FetchCuptreesRowsContractTests(unittest.IsolatedAsyncioTestCase):
    async def test_passes_ut_and_season_as_keyword_args(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_cuptrees_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                captured["args"] = args
                return []

        await fetch_cuptrees_rows(
            _StubConn(), unique_tournament_id=7, season_id=61644
        )
        self.assertEqual(captured["args"][0], 7)
        self.assertEqual(captured["args"][1], 61644)

    async def test_query_joins_all_four_tables_and_team(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            fetch_cuptrees_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_cuptrees_rows(
            _StubConn(), unique_tournament_id=7, season_id=61644
        )
        query = str(captured["query"])
        self.assertIn("season_cup_tree", query)
        self.assertIn("season_cup_tree_round", query)
        self.assertIn("season_cup_tree_block", query)
        self.assertIn("season_cup_tree_participant", query)
        # team JOIN for nested envelope (id, name, slug, shortName, ...).
        self.assertIn("team", query)

    async def test_query_orders_for_stable_nesting(self) -> None:
        """Builder collects rows into nested envelope; SQL must order
        so consecutive rows for the same cup_tree/round/block are
        adjacent (otherwise the builder would emit duplicate entries)."""
        import re

        from schema_inspector.scheduled_events_synthesizer import (
            fetch_cuptrees_rows,
        )

        captured: dict[str, object] = {}

        class _StubConn:
            async def fetch(self, query: str, *args: object):
                captured["query"] = query
                return []

        await fetch_cuptrees_rows(
            _StubConn(), unique_tournament_id=7, season_id=61644
        )
        query = str(captured["query"])
        # ORDER BY block spans newlines — use DOTALL.
        self.assertRegex(
            query,
            re.compile(
                r"ORDER BY.*cup_tree_id.*round_order.*block_order",
                re.DOTALL,
            ),
        )


class BuildCuptreesPayloadTests(unittest.TestCase):
    def test_empty_rows_returns_empty_envelope(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_cuptrees_payload,
        )

        self.assertEqual(build_cuptrees_payload([]), {"cupTrees": []})

    def test_single_block_with_single_participant(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_cuptrees_payload,
        )

        rows = [
            {
                # tree
                "cup_tree_id": 1234,
                "cup_tree_name": "Knockout",
                "current_round": 2,
                # round
                "round_order": 1,
                "round_type": 1,
                "round_description": "Round of 16",
                # block
                "entry_id": 9999,
                "block_id": 555,
                "block_order": 1,
                "block_finished": True,
                "matches_in_round": 1,
                "result": "2:1",
                "home_team_score": "2",
                "away_team_score": "1",
                "has_next_round_link": True,
                "series_start_date_timestamp": 1700000000,
                "automatic_progression": False,
                "event_ids_json": [4001, 4002],
                # participant
                "participant_id": 7001,
                "participant_order": 1,
                "winner": True,
                # team
                "team_id": 2697,
                "team_name": "Inter",
                "team_slug": "inter",
                "team_short_name": "Inter",
                "team_name_code": "INT",
                "team_gender": "M",
            },
        ]
        payload = build_cuptrees_payload(rows)
        self.assertEqual(len(payload["cupTrees"]), 1)
        tree = payload["cupTrees"][0]
        self.assertEqual(tree["id"], 1234)
        self.assertEqual(tree["name"], "Knockout")
        self.assertEqual(tree["currentRound"], 2)
        self.assertEqual(len(tree["rounds"]), 1)

        round_ = tree["rounds"][0]
        self.assertEqual(round_["order"], 1)
        self.assertEqual(round_["type"], 1)
        self.assertEqual(round_["description"], "Round of 16")
        self.assertEqual(len(round_["blocks"]), 1)

        block = round_["blocks"][0]
        self.assertEqual(block["id"], 9999)
        self.assertEqual(block["blockId"], 555)
        self.assertEqual(block["order"], 1)
        self.assertEqual(block["finished"], True)
        self.assertEqual(block["matchesInRound"], 1)
        self.assertEqual(block["result"], "2:1")
        self.assertEqual(block["homeTeamScore"], "2")
        self.assertEqual(block["awayTeamScore"], "1")
        self.assertEqual(block["hasNextRoundLink"], True)
        self.assertEqual(block["seriesStartDateTimestamp"], 1700000000)
        self.assertEqual(block["automaticProgression"], False)
        # event_ids → events: [{id: E}, ...]
        self.assertEqual(block["events"], [{"id": 4001}, {"id": 4002}])

        self.assertEqual(len(block["participants"]), 1)
        participant = block["participants"][0]
        self.assertEqual(participant["id"], 7001)
        self.assertEqual(participant["order"], 1)
        self.assertEqual(participant["winner"], True)
        self.assertEqual(participant["team"]["id"], 2697)
        self.assertEqual(participant["team"]["name"], "Inter")
        self.assertEqual(participant["team"]["slug"], "inter")
        self.assertEqual(participant["team"]["shortName"], "Inter")
        self.assertEqual(participant["team"]["nameCode"], "INT")
        self.assertEqual(participant["team"]["gender"], "M")

    def test_multiple_participants_per_block(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_cuptrees_payload,
        )

        base = {
            "cup_tree_id": 1,
            "cup_tree_name": "K",
            "current_round": 1,
            "round_order": 1,
            "round_type": None,
            "round_description": None,
            "entry_id": 100,
            "block_id": 10,
            "block_order": 1,
            "block_finished": True,
            "matches_in_round": 1,
            "result": None,
            "home_team_score": None,
            "away_team_score": None,
            "has_next_round_link": None,
            "series_start_date_timestamp": None,
            "automatic_progression": None,
            "event_ids_json": [],
            "team_short_name": None,
            "team_name_code": None,
            "team_gender": None,
        }
        rows = [
            {**base, "participant_id": 1, "participant_order": 1, "winner": True,
             "team_id": 1, "team_name": "A", "team_slug": "a"},
            {**base, "participant_id": 2, "participant_order": 2, "winner": False,
             "team_id": 2, "team_name": "B", "team_slug": "b"},
        ]
        payload = build_cuptrees_payload(rows)
        block = payload["cupTrees"][0]["rounds"][0]["blocks"][0]
        self.assertEqual(len(block["participants"]), 2)
        self.assertEqual(block["participants"][0]["team"]["name"], "A")
        self.assertEqual(block["participants"][1]["team"]["name"], "B")

    def test_multiple_blocks_per_round(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_cuptrees_payload,
        )

        base = {
            "cup_tree_id": 1, "cup_tree_name": "K", "current_round": 1,
            "round_order": 1, "round_type": None, "round_description": None,
            "block_finished": True, "matches_in_round": 1, "result": None,
            "home_team_score": None, "away_team_score": None,
            "has_next_round_link": None, "series_start_date_timestamp": None,
            "automatic_progression": None, "event_ids_json": [],
            "team_short_name": None, "team_name_code": None, "team_gender": None,
        }
        rows = [
            {**base, "entry_id": 100, "block_id": 10, "block_order": 1,
             "participant_id": 1, "participant_order": 1, "winner": True,
             "team_id": 1, "team_name": "A", "team_slug": "a"},
            {**base, "entry_id": 101, "block_id": 11, "block_order": 2,
             "participant_id": 2, "participant_order": 1, "winner": False,
             "team_id": 2, "team_name": "B", "team_slug": "b"},
        ]
        payload = build_cuptrees_payload(rows)
        round_ = payload["cupTrees"][0]["rounds"][0]
        self.assertEqual(len(round_["blocks"]), 2)
        self.assertEqual(round_["blocks"][0]["id"], 100)
        self.assertEqual(round_["blocks"][1]["id"], 101)

    def test_multiple_rounds_per_tree(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_cuptrees_payload,
        )

        base = {
            "cup_tree_id": 1, "cup_tree_name": "K", "current_round": 2,
            "round_type": None, "round_description": None,
            "block_finished": True, "matches_in_round": 1, "result": None,
            "home_team_score": None, "away_team_score": None,
            "has_next_round_link": None, "series_start_date_timestamp": None,
            "automatic_progression": None, "event_ids_json": [],
            "team_short_name": None, "team_name_code": None, "team_gender": None,
        }
        rows = [
            {**base, "round_order": 1, "entry_id": 100, "block_id": 10,
             "block_order": 1, "participant_id": 1, "participant_order": 1,
             "winner": True, "team_id": 1, "team_name": "A", "team_slug": "a"},
            {**base, "round_order": 2, "entry_id": 200, "block_id": 20,
             "block_order": 1, "participant_id": 2, "participant_order": 1,
             "winner": False, "team_id": 2, "team_name": "B", "team_slug": "b"},
        ]
        payload = build_cuptrees_payload(rows)
        tree = payload["cupTrees"][0]
        self.assertEqual(len(tree["rounds"]), 2)
        self.assertEqual(tree["rounds"][0]["order"], 1)
        self.assertEqual(tree["rounds"][1]["order"], 2)

    def test_skip_rows_with_null_cup_tree_id(self) -> None:
        from schema_inspector.scheduled_events_synthesizer import (
            build_cuptrees_payload,
        )

        rows = [
            {
                "cup_tree_id": None,
                "cup_tree_name": "Ghost", "current_round": None,
                "round_order": None, "round_type": None,
                "round_description": None, "entry_id": None,
                "block_id": None, "block_order": None,
                "block_finished": None, "matches_in_round": None,
                "result": None, "home_team_score": None,
                "away_team_score": None, "has_next_round_link": None,
                "series_start_date_timestamp": None,
                "automatic_progression": None, "event_ids_json": None,
                "participant_id": None, "participant_order": None,
                "winner": None, "team_id": None, "team_name": None,
                "team_slug": None, "team_short_name": None,
                "team_name_code": None, "team_gender": None,
            },
        ]
        self.assertEqual(build_cuptrees_payload(rows), {"cupTrees": []})

    def test_block_with_no_participants(self) -> None:
        """A block can exist without participants (TBD pairings,
        bye blocks, etc.). Builder must still emit the block."""
        from schema_inspector.scheduled_events_synthesizer import (
            build_cuptrees_payload,
        )

        rows = [
            {
                "cup_tree_id": 1, "cup_tree_name": "K", "current_round": 1,
                "round_order": 1, "round_type": None,
                "round_description": None,
                "entry_id": 100, "block_id": 10, "block_order": 1,
                "block_finished": False, "matches_in_round": 1,
                "result": None, "home_team_score": None,
                "away_team_score": None, "has_next_round_link": None,
                "series_start_date_timestamp": None,
                "automatic_progression": None, "event_ids_json": [],
                "participant_id": None, "participant_order": None,
                "winner": None, "team_id": None, "team_name": None,
                "team_slug": None, "team_short_name": None,
                "team_name_code": None, "team_gender": None,
            },
        ]
        payload = build_cuptrees_payload(rows)
        block = payload["cupTrees"][0]["rounds"][0]["blocks"][0]
        self.assertEqual(block["id"], 100)
        self.assertEqual(block["participants"], [])

    def test_event_ids_emitted_as_objects(self) -> None:
        """``event_ids_json`` JSONB column stores bare ints; wire
        format wraps each in ``{"id": E}``."""
        from schema_inspector.scheduled_events_synthesizer import (
            build_cuptrees_payload,
        )

        rows = [
            {
                "cup_tree_id": 1, "cup_tree_name": "K", "current_round": 1,
                "round_order": 1, "round_type": None,
                "round_description": None,
                "entry_id": 100, "block_id": 10, "block_order": 1,
                "block_finished": True, "matches_in_round": 1,
                "result": None, "home_team_score": None,
                "away_team_score": None, "has_next_round_link": None,
                "series_start_date_timestamp": None,
                "automatic_progression": None,
                "event_ids_json": [12345, 67890],
                "participant_id": 1, "participant_order": 1, "winner": True,
                "team_id": 1, "team_name": "A", "team_slug": "a",
                "team_short_name": None, "team_name_code": None,
                "team_gender": None,
            },
        ]
        payload = build_cuptrees_payload(rows)
        block = payload["cupTrees"][0]["rounds"][0]["blocks"][0]
        self.assertEqual(block["events"], [{"id": 12345}, {"id": 67890}])


class LocalApiCuptreesHybridTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_snapshot_when_available(self) -> None:
        """When snapshot exists, rich upstream wire is served 1:1
        (with editorial nesting like tournament, teamColors,
        fieldTranslations)."""
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _snapshot(**kwargs):
            return {
                "cupTrees": [
                    {
                        "id": 1,
                        "name": "Knockout",
                        "currentRound": 2,
                        "tournament": {"id": 7, "name": "UCL"},
                        "rounds": [
                            {
                                "order": 1, "type": 1, "description": "R16",
                                "blocks": [
                                    {
                                        "id": 100, "blockId": 10, "order": 1,
                                        "finished": True,
                                        "homeTeamScore": "2",
                                        "awayTeamScore": "1",
                                        "events": [{"id": 4001}],
                                        "participants": [
                                            {
                                                "id": 7001, "order": 1,
                                                "winner": True,
                                                "team": {
                                                    "id": 2697,
                                                    "name": "Inter",
                                                    "teamColors": {"primary": "#000"},
                                                },
                                            },
                                        ],
                                    },
                                ],
                            },
                        ],
                    },
                ],
            }

        application._fetch_latest_entity_passthrough = _snapshot

        result = await application._fetch_cuptrees_payload(7, 61644)
        self.assertEqual(len(result["cupTrees"]), 1)
        tree = result["cupTrees"][0]
        # Editorial preserved.
        self.assertIn("tournament", tree)
        self.assertIn("teamColors", tree["rounds"][0]["blocks"][0]["participants"][0]["team"])

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
                        "cup_tree_id": 1234,
                        "cup_tree_name": "Knockout",
                        "current_round": 2,
                        "round_order": 1,
                        "round_type": 1,
                        "round_description": "R16",
                        "entry_id": 9999,
                        "block_id": 555,
                        "block_order": 1,
                        "block_finished": True,
                        "matches_in_round": 1,
                        "result": "2:1",
                        "home_team_score": "2",
                        "away_team_score": "1",
                        "has_next_round_link": True,
                        "series_start_date_timestamp": 1700000000,
                        "automatic_progression": False,
                        "event_ids_json": [4001],
                        "participant_id": 7001,
                        "participant_order": 1,
                        "winner": True,
                        "team_id": 2697,
                        "team_name": "Inter",
                        "team_slug": "inter",
                        "team_short_name": "Inter",
                        "team_name_code": "INT",
                        "team_gender": "M",
                    },
                ]

            async def close(self) -> None:
                self.closed = True

        async def _connect():
            return _StubConn()

        application._connect = _connect

        result = await application._fetch_cuptrees_payload(7, 61644)
        self.assertEqual(len(result["cupTrees"]), 1)
        tree = result["cupTrees"][0]
        self.assertEqual(tree["id"], 1234)
        # Synth path omits editorial.
        self.assertNotIn("tournament", tree)
        self.assertNotIn(
            "teamColors",
            tree["rounds"][0]["blocks"][0]["participants"][0]["team"],
        )

    async def test_returns_empty_envelope_when_db_unreachable(self) -> None:
        from schema_inspector.local_api_server import LocalApiApplication

        application = LocalApiApplication.__new__(LocalApiApplication)

        async def _no_snapshot(**kwargs):
            return None

        application._fetch_latest_entity_passthrough = _no_snapshot

        async def _connect():
            raise RuntimeError("db down")

        application._connect = _connect

        result = await application._fetch_cuptrees_payload(7, 61644)
        self.assertEqual(result, {"cupTrees": []})


if __name__ == "__main__":
    unittest.main()
