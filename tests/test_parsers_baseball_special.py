from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.special.baseball_innings import BaseballInningsParser
from schema_inspector.parsers.special.baseball_pitches import BaseballPitchesParser


class BaseballSpecialParserTests(unittest.TestCase):
    def test_baseball_innings_parser_extracts_inning_rows(self) -> None:
        parser = BaseballInningsParser()
        snapshot = RawSnapshot(
            snapshot_id=801,
            endpoint_pattern="/api/v1/event/{event_id}/innings",
            sport_slug="baseball",
            source_url="https://www.sofascore.com/api/v1/event/15507996/innings",
            resolved_url="https://www.sofascore.com/api/v1/event/15507996/innings",
            envelope_key="innings",
            http_status=200,
            payload={"innings": [{"inning": 1, "homeScore": 0, "awayScore": 1}]},
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=15507996,
            context_event_id=15507996,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual(result.metric_rows["baseball_inning"][0]["inning"], 1)

    def test_baseball_pitches_parser_extracts_pitch_rows(self) -> None:
        parser = BaseballPitchesParser()
        snapshot = RawSnapshot(
            snapshot_id=802,
            endpoint_pattern="/api/v1/event/{event_id}/atbat/{at_bat_id}/pitches",
            sport_slug="baseball",
            source_url="https://www.sofascore.com/api/v1/event/15507996/atbat/44/pitches",
            resolved_url="https://www.sofascore.com/api/v1/event/15507996/atbat/44/pitches",
            envelope_key="pitches",
            http_status=200,
            payload={"pitches": [{"id": 1, "outcome": "strike", "pitchType": "FF", "speed": 96}]},
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=15507996,
            context_event_id=15507996,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual(result.metric_rows["baseball_pitch"][0]["pitch_type"], "FF")


if __name__ == "__main__":
    unittest.main()
