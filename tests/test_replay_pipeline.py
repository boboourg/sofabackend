from __future__ import annotations

import unittest

from schema_inspector.normalizers.worker import NormalizeWorker
from schema_inspector.parsers.base import RawSnapshot
from schema_inspector.parsers.registry import ParserRegistry
from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator
from schema_inspector.planner.planner import Planner


class _SnapshotStore:
    def __init__(self, snapshot: RawSnapshot) -> None:
        self.snapshot = snapshot

    def load_snapshot(self, snapshot_id: int) -> RawSnapshot:
        if snapshot_id != self.snapshot.snapshot_id:
            raise KeyError(snapshot_id)
        return self.snapshot


class ReplayPipelineTests(unittest.TestCase):
    def test_replay_snapshot_uses_normalize_worker_without_network(self) -> None:
        snapshot = RawSnapshot(
            snapshot_id=900,
            endpoint_pattern="/api/v1/event/{event_id}/statistics",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/1/statistics",
            resolved_url="https://www.sofascore.com/api/v1/event/1/statistics",
            envelope_key="statistics",
            http_status=200,
            payload={
                "statistics": [
                    {
                        "period": "ALL",
                        "groups": [{"groupName": "Overview", "statisticsItems": [{"name": "Shots", "home": "5", "away": "3"}]}],
                    }
                ]
            },
            fetched_at="2026-04-16T12:00:00+00:00",
            context_entity_type="event",
            context_entity_id=1,
            context_event_id=1,
        )
        orchestrator = PilotOrchestrator(
            fetch_executor=None,
            snapshot_store=_SnapshotStore(snapshot),
            normalize_worker=NormalizeWorker(ParserRegistry.default()),
            planner=Planner(),
            capability_repository=None,
            sql_executor=None,
        )

        result = orchestrator.replay_snapshot(900)

        self.assertEqual(result.parser_family, "event_statistics")
        self.assertEqual(result.status, "parsed")


if __name__ == "__main__":
    unittest.main()
