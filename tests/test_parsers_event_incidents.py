from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.families.event_incidents import EventIncidentsParser


class EventIncidentsParserTests(unittest.TestCase):
    def test_event_incidents_parser_extracts_incident_rows(self) -> None:
        parser = EventIncidentsParser()
        snapshot = RawSnapshot(
            snapshot_id=601,
            endpoint_pattern="/api/v1/event/{event_id}/incidents",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/14083191/incidents",
            resolved_url="https://www.sofascore.com/api/v1/event/14083191/incidents",
            envelope_key="incidents",
            http_status=200,
            payload={
                "incidents": [
                    {
                        "id": 1,
                        "incidentType": "goal",
                        "time": 17,
                        "player": {"id": 700, "slug": "saka", "name": "Bukayo Saka"},
                        "team": {"id": 42, "slug": "arsenal", "name": "Arsenal"},
                        "homeScore": 1,
                        "awayScore": 0,
                    }
                ]
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=14083191,
            context_event_id=14083191,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual(len(result.metric_rows["event_incident"]), 1)
        self.assertEqual(result.metric_rows["event_incident"][0]["incident_type"], "goal")
        self.assertEqual({item["id"] for item in result.entity_upserts["player"]}, {700})


if __name__ == "__main__":
    unittest.main()
