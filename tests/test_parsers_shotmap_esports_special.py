from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.special.esports_games import EsportsGamesParser
from schema_inspector.parsers.special.shotmap import ShotmapParser


class ShotmapAndEsportsSpecialParserTests(unittest.TestCase):
    def test_shotmap_parser_extracts_points(self) -> None:
        parser = ShotmapParser()
        snapshot = RawSnapshot(
            snapshot_id=901,
            endpoint_pattern="/api/v1/event/{event_id}/shotmap",
            sport_slug="ice-hockey",
            source_url="https://www.sofascore.com/api/v1/event/14201603/shotmap",
            resolved_url="https://www.sofascore.com/api/v1/event/14201603/shotmap",
            envelope_key="shotmap",
            http_status=200,
            payload={"shotmap": [{"x": 12.5, "y": 30.0, "shotType": "wrist"}]},
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=14201603,
            context_event_id=14201603,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual(result.metric_rows["shotmap_point"][0]["shot_type"], "wrist")

    def test_esports_games_parser_extracts_games(self) -> None:
        parser = EsportsGamesParser()
        snapshot = RawSnapshot(
            snapshot_id=902,
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

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual(result.metric_rows["esports_game"][0]["map_name"], "Dust2")


if __name__ == "__main__":
    unittest.main()
