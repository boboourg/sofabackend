from __future__ import annotations

import unittest

from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.registry import ParserRegistry


class EventGraphParserTests(unittest.TestCase):
    def test_registry_dispatches_event_graph_snapshot(self) -> None:
        registry = ParserRegistry.default()
        snapshot = RawSnapshot(
            snapshot_id=201,
            endpoint_pattern="/api/v1/event/{event_id}/graph",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/14083191/graph",
            resolved_url="https://www.sofascore.com/api/v1/event/14083191/graph",
            envelope_key="graphPoints",
            http_status=200,
            payload={
                "periodTime": 74,
                "periodCount": 2,
                "overtimeLength": 0,
                "graphPoints": [
                    {"minute": 17, "value": 1},
                    {"minute": 62, "value": -1},
                ],
            },
            fetched_at="2026-04-17T10:00:00+00:00",
            context_entity_type="event",
            context_entity_id=14083191,
            context_event_id=14083191,
        )

        result = registry.parse(snapshot)

        self.assertEqual(result.parser_family, "event_graph")
        self.assertEqual(result.status, "parsed")
        self.assertEqual(result.metric_rows["event_graph"][0]["event_id"], 14083191)
        self.assertEqual(len(result.metric_rows["event_graph_point"]), 2)


if __name__ == "__main__":
    unittest.main()
