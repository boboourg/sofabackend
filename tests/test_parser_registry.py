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


if __name__ == "__main__":
    unittest.main()
