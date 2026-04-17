from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.families.event_statistics import EventStatisticsParser


class EventStatisticsParserTests(unittest.TestCase):
    def test_event_statistics_parser_extracts_rows(self) -> None:
        parser = EventStatisticsParser()
        snapshot = RawSnapshot(
            snapshot_id=301,
            endpoint_pattern="/api/v1/event/{event_id}/statistics",
            sport_slug="handball",
            source_url="https://www.sofascore.com/api/v1/event/1/statistics",
            resolved_url="https://www.sofascore.com/api/v1/event/1/statistics",
            envelope_key="statistics",
            http_status=200,
            payload={
                "statistics": [
                    {
                        "period": "ALL",
                        "groups": [
                            {
                                "groupName": "Match overview",
                                "statisticsItems": [
                                    {"name": "Possession", "home": "55%", "away": "45%"},
                                    {"name": "Fastbreak goals", "home": "7", "away": "4"},
                                ],
                            }
                        ],
                    }
                ],
                "homeTeam": {"id": 42, "slug": "veszprem", "name": "Veszprem"},
                "awayTeam": {"id": 43, "slug": "szeged", "name": "Szeged"},
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=1,
            context_event_id=1,
        )

        result = parser.parse(snapshot)

        self.assertEqual(result.status, "parsed")
        self.assertEqual({item["id"] for item in result.entity_upserts["team"]}, {42, 43})
        self.assertEqual(len(result.metric_rows["event_statistic"]), 2)
        self.assertEqual(result.metric_rows["event_statistic"][0]["period"], "ALL")
        self.assertEqual(result.metric_rows["event_statistic"][1]["name"], "Fastbreak goals")


if __name__ == "__main__":
    unittest.main()
