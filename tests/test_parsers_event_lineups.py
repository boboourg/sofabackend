from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.families.event_lineups import EventLineupsParser


class EventLineupsParserTests(unittest.TestCase):
    def test_event_lineups_parser_extracts_players_and_lineup_rows(self) -> None:
        parser = EventLineupsParser()
        snapshot = RawSnapshot(
            snapshot_id=401,
            endpoint_pattern="/api/v1/event/{event_id}/lineups",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/14083191/lineups",
            resolved_url="https://www.sofascore.com/api/v1/event/14083191/lineups",
            envelope_key="home,away",
            http_status=200,
            payload={
                "home": {
                    "formation": "4-3-3",
                    "players": [
                        {
                            "position": "F",
                            "jerseyNumber": "7",
                            "substitute": False,
                            "teamId": 42,
                            "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"},
                        }
                    ],
                },
                "away": {
                    "formation": "4-2-3-1",
                    "players": [
                        {
                            "position": "M",
                            "jerseyNumber": "20",
                            "substitute": False,
                            "teamId": 43,
                            "player": {"id": 701, "slug": "palmer", "name": "Cole Palmer"},
                        }
                    ],
                },
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=14083191,
            context_event_id=14083191,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual({item["id"] for item in result.entity_upserts["player"]}, {700, 701})
        self.assertEqual(len(result.metric_rows["event_lineup_side"]), 2)
        self.assertEqual(len(result.relation_upserts["event_lineup_player"]), 2)


if __name__ == "__main__":
    unittest.main()
