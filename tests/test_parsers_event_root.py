from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.families.event_root import EventRootParser


class EventRootParserTests(unittest.TestCase):
    def test_event_root_parser_extracts_core_entities_and_relations(self) -> None:
        parser = EventRootParser()
        snapshot = RawSnapshot(
            snapshot_id=201,
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
                    "tournament": {
                        "id": 100,
                        "slug": "premier-league",
                        "name": "Premier League",
                        "category": {
                            "id": 1,
                            "slug": "england",
                            "name": "England",
                            "sport": {"id": 1, "slug": "football", "name": "Football"},
                        },
                        "uniqueTournament": {"id": 17, "slug": "premier-league", "name": "Premier League"},
                    },
                    "season": {"id": 76986, "name": "Premier League 25/26", "year": "25/26"},
                    "venue": {"id": 55, "slug": "emirates-stadium", "name": "Emirates Stadium"},
                    "homeTeam": {
                        "id": 42,
                        "slug": "arsenal",
                        "name": "Arsenal",
                        "manager": {"id": 500, "slug": "arteta", "name": "Mikel Arteta"},
                    },
                    "awayTeam": {
                        "id": 43,
                        "slug": "chelsea",
                        "name": "Chelsea",
                        "manager": {"id": 501, "slug": "maresca", "name": "Enzo Maresca"},
                    },
                    "status": {"code": 100, "description": "1st half", "type": "inprogress"},
                    "startTimestamp": 1775779200,
                }
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=14083191,
            context_event_id=14083191,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual(result.entity_upserts["sport"][0]["slug"], "football")
        self.assertEqual(result.entity_upserts["category"][0]["id"], 1)
        self.assertEqual(result.entity_upserts["unique_tournament"][0]["id"], 17)
        self.assertEqual(result.entity_upserts["season"][0]["id"], 76986)
        self.assertEqual(result.entity_upserts["venue"][0]["id"], 55)
        self.assertEqual({item["id"] for item in result.entity_upserts["manager"]}, {500, 501})
        self.assertEqual(len(result.relation_upserts["event_team"]), 2)


if __name__ == "__main__":
    unittest.main()
