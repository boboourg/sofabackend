from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.special.tennis_point_by_point import TennisPointByPointParser
from schema_inspector.parsers.special.tennis_power import TennisPowerParser


class TennisSpecialParserTests(unittest.TestCase):
    def test_point_by_point_parser_extracts_progression_rows(self) -> None:
        parser = TennisPointByPointParser()
        snapshot = RawSnapshot(
            snapshot_id=501,
            endpoint_pattern="/api/v1/event/{event_id}/point-by-point",
            sport_slug="tennis",
            source_url="https://www.sofascore.com/api/v1/event/15921219/point-by-point",
            resolved_url="https://www.sofascore.com/api/v1/event/15921219/point-by-point",
            envelope_key="root",
            http_status=200,
            payload={
                "pointByPoint": [
                    {"id": 1, "set": 1, "game": 1, "server": "home", "score": {"home": 15, "away": 0}},
                    {"id": 2, "set": 1, "game": 1, "server": "away", "score": {"home": 15, "away": 15}},
                ]
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=15921219,
            context_event_id=15921219,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual(len(result.metric_rows["tennis_point_by_point"]), 2)
        self.assertEqual(result.metric_rows["tennis_point_by_point"][0]["server"], "home")

    def test_tennis_power_parser_extracts_ranking_rows(self) -> None:
        parser = TennisPowerParser()
        snapshot = RawSnapshot(
            snapshot_id=502,
            endpoint_pattern="/api/v1/event/{event_id}/tennis-power",
            sport_slug="tennis",
            source_url="https://www.sofascore.com/api/v1/event/15921219/tennis-power",
            resolved_url="https://www.sofascore.com/api/v1/event/15921219/tennis-power",
            envelope_key="tennisPowerRankings",
            http_status=200,
            payload={
                "tennisPowerRankings": {
                    "home": {"current": 0.61, "delta": 0.12},
                    "away": {"current": 0.39, "delta": -0.12},
                }
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=15921219,
            context_event_id=15921219,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual(len(result.metric_rows["tennis_power"]), 2)
        self.assertEqual({item["side"] for item in result.metric_rows["tennis_power"]}, {"home", "away"})


if __name__ == "__main__":
    unittest.main()
