from __future__ import annotations

import unittest

from schema_inspector.normalizers.worker import NormalizeWorker
from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.registry import ParserRegistry


class ParserRegistryTests(unittest.TestCase):
    def test_registry_dispatches_event_root_snapshot(self) -> None:
        registry = ParserRegistry.default()
        snapshot = RawSnapshot(
            snapshot_id=101,
            endpoint_pattern="/api/v1/event/{event_id}",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/14083191",
            resolved_url="https://www.sofascore.com/api/v1/event/14083191",
            envelope_key="event",
            http_status=200,
            payload={
                "event": {
                    "id": 14083191,
                    "slug": "arsenal-chelsea",
                    "tournament": {"id": 100, "name": "Premier League", "slug": "premier-league"},
                    "season": {"id": 76986, "name": "Premier League 25/26"},
                    "status": {"type": "inprogress"},
                    "homeTeam": {"id": 42, "name": "Arsenal", "slug": "arsenal"},
                    "awayTeam": {"id": 43, "name": "Chelsea", "slug": "chelsea"},
                }
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=14083191,
            context_event_id=14083191,
        )

        result = registry.parse(snapshot)

        self.assertEqual(result.parser_family, "event_root")
        self.assertEqual(result.status, "parsed")
        self.assertEqual(result.entity_upserts["event"][0]["id"], 14083191)
        self.assertEqual({item["id"] for item in result.entity_upserts["team"]}, {42, 43})

    def test_registry_marks_soft_error_payload_without_parsing_family(self) -> None:
        registry = ParserRegistry.default()
        snapshot = RawSnapshot(
            snapshot_id=102,
            endpoint_pattern="/api/v1/event/{event_id}/statistics",
            sport_slug="handball",
            source_url="https://www.sofascore.com/api/v1/event/1/statistics",
            resolved_url="https://www.sofascore.com/api/v1/event/1/statistics",
            envelope_key="statistics",
            http_status=200,
            payload={"error": {"code": 404, "message": "Not found"}},
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=1,
            context_event_id=1,
        )

        result = registry.parse(snapshot)

        self.assertEqual(result.status, "soft_error_payload")
        self.assertEqual(result.parser_family, "soft_error")
        self.assertEqual(result.entity_upserts, {})

    def test_normalize_worker_uses_registry_contract(self) -> None:
        registry = ParserRegistry.default()
        worker = NormalizeWorker(registry)
        snapshot = RawSnapshot(
            snapshot_id=103,
            endpoint_pattern="/api/v1/event/{event_id}/lineups",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/14083191/lineups",
            resolved_url="https://www.sofascore.com/api/v1/event/14083191/lineups",
            envelope_key="home,away",
            http_status=200,
            payload={
                "home": {
                    "players": [
                        {
                            "position": "F",
                            "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"},
                            "teamId": 42,
                        }
                    ]
                },
                "away": {
                    "players": [
                        {
                            "position": "F",
                            "player": {"id": 701, "slug": "palmer", "name": "Cole Palmer"},
                            "teamId": 43,
                        }
                    ]
                },
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=14083191,
            context_event_id=14083191,
        )

        result = worker.handle(snapshot)

        self.assertEqual(result.parser_family, "event_lineups")
        self.assertEqual(result.status, "parsed")
        self.assertEqual(len(result.relation_upserts["event_lineup_player"]), 2)

    def test_registry_dispatches_esports_games_special_snapshot(self) -> None:
        registry = ParserRegistry.default()
        snapshot = RawSnapshot(
            snapshot_id=104,
            endpoint_pattern="/api/v1/event/{event_id}/esports-games",
            sport_slug="esports",
            source_url="https://www.sofascore.com/api/v1/event/20001/esports-games",
            resolved_url="https://www.sofascore.com/api/v1/event/20001/esports-games",
            envelope_key="games",
            http_status=200,
            payload={"games": [{"id": 1, "status": "finished", "mapName": "Dust2"}]},
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=20001,
            context_event_id=20001,
        )

        result = registry.parse(snapshot)

        self.assertEqual(result.parser_family, "esports_games")
        self.assertEqual(result.status, "parsed")

    def test_registry_dispatches_event_player_statistics_snapshot(self) -> None:
        registry = ParserRegistry.default()
        snapshot = RawSnapshot(
            snapshot_id=105,
            endpoint_pattern="/api/v1/event/{event_id}/player/{player_id}/statistics",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/14023987/player/804508/statistics",
            resolved_url="https://www.sofascore.com/api/v1/event/14023987/player/804508/statistics",
            envelope_key="player,team,position,statistics,extra",
            http_status=200,
            payload={
                "player": {"id": 804508, "slug": "player-a", "name": "Player A"},
                "team": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                "position": "F",
                "statistics": {"goals": 1, "rating": 7.8},
                "extra": None,
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="player",
            context_entity_id=804508,
            context_event_id=14023987,
        )

        result = registry.parse(snapshot)

        self.assertEqual(result.parser_family, "event_player_statistics")
        self.assertEqual(result.status, "parsed")

    def test_registry_dispatches_event_comments_snapshot(self) -> None:
        registry = ParserRegistry.default()
        snapshot = RawSnapshot(
            snapshot_id=106,
            endpoint_pattern="/api/v1/event/{event_id}/comments",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/14083191/comments",
            resolved_url="https://www.sofascore.com/api/v1/event/14083191/comments",
            envelope_key="comments,home,away",
            http_status=200,
            payload={
                "comments": [
                    {
                        "id": 37184719,
                        "isHome": False,
                        "periodName": "2ND",
                        "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"},
                        "sequence": 0,
                        "text": "Second Half begins.",
                        "time": 46,
                        "type": "matchStarted",
                    }
                ],
                "home": {"playerColor": {"primary": "#ffffff"}},
                "away": {"playerColor": {"primary": "#0000ff"}},
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=14083191,
            context_event_id=14083191,
        )

        result = registry.parse(snapshot)

        self.assertEqual(result.parser_family, "event_comments")
        self.assertEqual(result.status, "parsed")
        self.assertEqual(result.metric_rows["event_comment"][0]["comment_id"], 37184719)

    def test_registry_dispatches_extended_event_detail_families(self) -> None:
        registry = ParserRegistry.default()

        managers_result = registry.parse(
            RawSnapshot(
                snapshot_id=107,
                endpoint_pattern="/api/v1/event/{event_id}/managers",
                sport_slug="football",
                source_url="https://www.sofascore.com/api/v1/event/15868599/managers",
                resolved_url="https://www.sofascore.com/api/v1/event/15868599/managers",
                envelope_key="homeManager,awayManager",
                http_status=200,
                payload={
                    "homeManager": {"id": 500, "slug": "martin", "name": "Manager Home"},
                    "awayManager": {"id": 501, "slug": "borges", "name": "Manager Away"},
                },
                fetched_at="2026-04-23T00:00:00+00:00",
                context_entity_type="event",
                context_entity_id=15868599,
                context_event_id=15868599,
            )
        )
        self.assertEqual(managers_result.parser_family, "event_managers")
        self.assertEqual(
            {(row["side"], row["manager_id"]) for row in managers_result.metric_rows["event_manager_assignment"]},
            {("home", 500), ("away", 501)},
        )

        heatmap_result = registry.parse(
            RawSnapshot(
                snapshot_id=108,
                endpoint_pattern="/api/v1/event/{event_id}/heatmap/{team_id}",
                sport_slug="football",
                source_url="https://www.sofascore.com/api/v1/event/15868599/heatmap/3002",
                resolved_url="https://www.sofascore.com/api/v1/event/15868599/heatmap/3002",
                envelope_key="playerPoints,goalkeeperPoints",
                http_status=200,
                payload={
                    "playerPoints": [{"x": 0.1, "y": 0.2}],
                    "goalkeeperPoints": [{"x": 0.3, "y": 0.4}],
                },
                fetched_at="2026-04-23T00:00:00+00:00",
                context_entity_type="event",
                context_entity_id=15868599,
                context_event_id=15868599,
            )
        )
        self.assertEqual(heatmap_result.parser_family, "event_team_heatmap")
        self.assertEqual(heatmap_result.metric_rows["event_team_heatmap"][0]["team_id"], 3002)
        self.assertEqual(len(heatmap_result.metric_rows["event_team_heatmap_point"]), 2)

        odds_result = registry.parse(
            RawSnapshot(
                snapshot_id=109,
                endpoint_pattern="/api/v1/event/{event_id}/odds/{provider_id}/all",
                sport_slug="football",
                source_url="https://www.sofascore.com/api/v1/event/15868599/odds/1/all",
                resolved_url="https://www.sofascore.com/api/v1/event/15868599/odds/1/all",
                envelope_key="markets",
                http_status=200,
                payload={
                    "provider": {"id": 1, "name": "Provider One"},
                    "providerConfiguration": {"id": 77, "providerId": 1, "type": "main"},
                    "markets": [
                        {
                            "id": 900,
                            "fid": 44,
                            "sourceId": 555,
                            "marketId": 2,
                            "marketGroup": "Match",
                            "marketName": "1X2",
                            "marketPeriod": "ALL",
                            "structureType": 1,
                            "isLive": True,
                            "suspended": False,
                            "choices": [
                                {
                                    "sourceId": 6001,
                                    "name": "Home",
                                    "change": 0,
                                    "fractionalValue": "2/1",
                                    "initialFractionalValue": "2/1",
                                }
                            ],
                        }
                    ],
                },
                fetched_at="2026-04-23T00:00:00+00:00",
                context_entity_type="event",
                context_entity_id=15868599,
                context_event_id=15868599,
            )
        )
        self.assertEqual(odds_result.parser_family, "event_odds")
        self.assertEqual(odds_result.metric_rows["provider"][0]["id"], 1)
        self.assertEqual(odds_result.metric_rows["event_market"][0]["id"], 900)
        self.assertEqual(odds_result.metric_rows["event_market_choice"][0]["source_id"], 6001)

        winning_odds_result = registry.parse(
            RawSnapshot(
                snapshot_id=110,
                endpoint_pattern="/api/v1/event/{event_id}/provider/{provider_id}/winning-odds",
                sport_slug="football",
                source_url="https://www.sofascore.com/api/v1/event/15868599/provider/1/winning-odds",
                resolved_url="https://www.sofascore.com/api/v1/event/15868599/provider/1/winning-odds",
                envelope_key="home,away",
                http_status=200,
                payload={
                    "home": {"id": 10, "actual": 52, "expected": 48, "fractionalValue": "1/2"},
                    "away": {"id": 11, "actual": 48, "expected": 52, "fractionalValue": "7/4"},
                },
                fetched_at="2026-04-23T00:00:00+00:00",
                context_entity_type="event",
                context_entity_id=15868599,
                context_event_id=15868599,
            )
        )
        self.assertEqual(winning_odds_result.parser_family, "event_winning_odds")
        self.assertEqual(len(winning_odds_result.metric_rows["event_winning_odds"]), 2)

    def test_registry_dispatches_season_rounds_snapshot(self) -> None:
        registry = ParserRegistry.default()
        snapshot = RawSnapshot(
            snapshot_id=111,
            endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/rounds",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/unique-tournament/8/season/77559/rounds",
            resolved_url="https://www.sofascore.com/api/v1/unique-tournament/8/season/77559/rounds",
            envelope_key="rounds",
            http_status=200,
            payload={
                "currentRound": {"round": 33, "name": "Round 33", "slug": "round-33"},
                "rounds": [
                    {"round": 32, "name": "Round 32", "slug": "round-32"},
                    {"round": 33, "name": "Round 33", "slug": "round-33"},
                ],
            },
            fetched_at="2026-04-23T00:00:00+00:00",
            context_entity_type="season",
            context_entity_id=77559,
            context_unique_tournament_id=8,
            context_season_id=77559,
        )

        result = registry.parse(snapshot)

        self.assertEqual(result.parser_family, "season_rounds")
        self.assertEqual(result.status, "parsed")
        round_rows = sorted(result.metric_rows["season_round"], key=lambda row: row["round_number"])
        self.assertEqual(round_rows[0]["round_number"], 32)
        self.assertFalse(round_rows[0]["is_current"])
        self.assertEqual(round_rows[1]["round_number"], 33)
        self.assertTrue(round_rows[1]["is_current"])

    def test_registry_dispatches_season_cuptrees_snapshot(self) -> None:
        registry = ParserRegistry.default()
        snapshot = RawSnapshot(
            snapshot_id=112,
            endpoint_pattern="/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/cuptrees",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/unique-tournament/336/season/80287/cuptrees",
            resolved_url="https://www.sofascore.com/api/v1/unique-tournament/336/season/80287/cuptrees",
            envelope_key="cupTrees",
            http_status=200,
            payload={
                "cupTrees": [
                    {
                        "id": 10845780,
                        "name": "Taca de Portugal 25/26",
                        "currentRound": 7,
                        "tournament": {
                            "id": 207,
                            "slug": "taca-de-portugal",
                            "name": "Taca de Portugal",
                            "category": {
                                "id": 44,
                                "slug": "portugal",
                                "name": "Portugal",
                                "sport": {"id": 1, "slug": "football", "name": "Football"},
                            },
                            "uniqueTournament": {"id": 336, "slug": "taca-de-portugal", "name": "Taca de Portugal"},
                        },
                        "rounds": [
                            {
                                "order": 1,
                                "type": 101,
                                "description": "Round 1",
                                "blocks": [
                                    {
                                        "id": 2873386,
                                        "blockId": 2421533,
                                        "finished": True,
                                        "matchesInRound": 1,
                                        "order": 1,
                                        "result": "7:1",
                                        "homeTeamScore": "7",
                                        "awayTeamScore": "1",
                                        "events": [14410747],
                                        "hasNextRoundLink": True,
                                        "seriesStartDateTimestamp": 1756656000,
                                        "automaticProgression": False,
                                        "participants": [
                                            {
                                                "id": 5276248,
                                                "order": 1,
                                                "winner": True,
                                                "team": {"id": 190324, "slug": "ad-ovarense", "name": "AD Ovarense"},
                                            },
                                            {
                                                "id": 5276249,
                                                "order": 2,
                                                "winner": False,
                                                "team": {"id": 1116505, "slug": "sc-celoricense", "name": "SC Celoricense"},
                                            },
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ]
            },
            fetched_at="2026-04-23T00:00:00+00:00",
            context_entity_type="season",
            context_entity_id=80287,
            context_unique_tournament_id=336,
            context_season_id=80287,
        )

        result = registry.parse(snapshot)

        self.assertEqual(result.parser_family, "season_cuptrees")
        self.assertEqual(result.status, "parsed")
        self.assertEqual(result.metric_rows["season_cup_tree"][0]["cup_tree_id"], 10845780)
        self.assertEqual(result.metric_rows["season_cup_tree_round"][0]["round_order"], 1)
        self.assertEqual(result.metric_rows["season_cup_tree_block"][0]["entry_id"], 2873386)
        self.assertEqual(result.metric_rows["season_cup_tree_block"][0]["event_ids_json"], [14410747])
        self.assertEqual(result.metric_rows["season_cup_tree_participant"][0]["team_id"], 190324)


if __name__ == "__main__":
    unittest.main()
