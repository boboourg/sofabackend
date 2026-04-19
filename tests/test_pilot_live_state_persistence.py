from __future__ import annotations

import json
import unittest

from schema_inspector.fetch_executor import FetchExecutor
from schema_inspector.normalizers.worker import NormalizeWorker
from schema_inspector.parsers.registry import ParserRegistry
from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator
from schema_inspector.planner.planner import Planner
from schema_inspector.runtime import TransportAttempt, TransportResult
from schema_inspector.workers.live_worker import LiveWorker


class _FakeTransport:
    def __init__(self, responses: dict[str, TransportResult]) -> None:
        self.responses = responses

    async def fetch(self, url: str, *, headers=None, timeout: float = 20.0) -> TransportResult:
        del headers, timeout
        return self.responses[url]


class _FakeRawSnapshotStore:
    def __init__(self) -> None:
        self.snapshots_by_id = {}
        self._next_id = 1

    async def insert_request_log(self, executor, record) -> None:
        del executor, record

    async def insert_payload_snapshot_returning_id(self, executor, record) -> int:
        del executor
        snapshot_id = self._next_id
        self._next_id += 1
        self.snapshots_by_id[snapshot_id] = record
        return snapshot_id

    async def insert_payload_snapshot_if_missing_returning_id(self, executor, record) -> int:
        return await self.insert_payload_snapshot_returning_id(executor, record)

    async def upsert_snapshot_head(self, executor, record) -> None:
        del executor, record

    def load_snapshot(self, snapshot_id: int):
        from schema_inspector.parsers.base import RawSnapshot

        record = self.snapshots_by_id[snapshot_id]
        return RawSnapshot(
            snapshot_id=snapshot_id,
            endpoint_pattern=record.endpoint_pattern,
            sport_slug=record.sport_slug,
            source_url=record.source_url,
            resolved_url=record.resolved_url,
            envelope_key=record.envelope_key,
            http_status=record.http_status,
            payload=record.payload,
            fetched_at=record.fetched_at,
            context_entity_type=record.context_entity_type,
            context_entity_id=record.context_entity_id,
            context_unique_tournament_id=record.context_unique_tournament_id,
            context_season_id=record.context_season_id,
            context_event_id=record.context_event_id,
        )


class _FakeCapabilityRepository:
    async def insert_observation(self, executor, record) -> None:
        del executor, record

    async def upsert_rollup(self, executor, record) -> None:
        del executor, record


class _FakeLiveStateRepository:
    def __init__(self) -> None:
        self.live_history = []
        self.terminal_states = []

    async def insert_live_state_history(self, executor, record) -> None:
        del executor
        self.live_history.append(record)

    async def upsert_terminal_state(self, executor, record) -> None:
        del executor
        self.terminal_states.append(record)


class PilotLiveStatePersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_and_terminal_states_are_persisted(self) -> None:
        event_id = 15921221
        event_url = f"https://www.sofascore.com/api/v1/event/{event_id}"
        statistics_url = f"https://www.sofascore.com/api/v1/event/{event_id}/statistics"
        lineups_url = f"https://www.sofascore.com/api/v1/event/{event_id}/lineups"
        incidents_url = f"https://www.sofascore.com/api/v1/event/{event_id}/incidents"
        point_by_point_url = f"https://www.sofascore.com/api/v1/event/{event_id}/point-by-point"
        tennis_power_url = f"https://www.sofascore.com/api/v1/event/{event_id}/tennis-power"

        live_state_repository = _FakeLiveStateRepository()
        raw_store = _FakeRawSnapshotStore()
        orchestrator = PilotOrchestrator(
            fetch_executor=FetchExecutor(
                transport=_FakeTransport(
                    {
                        event_url: _json_result(
                            event_url,
                            {
                                "event": {
                                    "id": event_id,
                                    "slug": "sinner-alcaraz",
                                    "tournament": {
                                        "id": 300,
                                        "slug": "wimbledon",
                                        "name": "Wimbledon",
                                        "uniqueTournament": {"id": 2361, "slug": "wimbledon", "name": "Wimbledon"},
                                    },
                                    "season": {"id": 90001, "name": "Wimbledon 2026", "year": "2026"},
                                    "status": {"type": "finished"},
                                    "startTimestamp": 1_799_999_000,
                                    "homeTeam": {"id": 101, "slug": "sinner", "name": "Jannik Sinner"},
                                    "awayTeam": {"id": 102, "slug": "alcaraz", "name": "Carlos Alcaraz"},
                                }
                            },
                        ),
                        statistics_url: _json_result(statistics_url, {"statistics": []}),
                        lineups_url: _json_result(lineups_url, {"home": {"players": []}, "away": {"players": []}}),
                        incidents_url: _json_result(incidents_url, {"incidents": []}),
                        point_by_point_url: _json_result(point_by_point_url, {"pointByPoint": []}),
                        tennis_power_url: _json_result(
                            tennis_power_url,
                            {"tennisPowerRankings": {"home": {"current": 0.51}, "away": {"current": 0.49}}},
                        ),
                    }
                ),
                raw_repository=raw_store,
                sql_executor=object(),
            ),
            snapshot_store=raw_store,
            normalize_worker=NormalizeWorker(ParserRegistry.default()),
            planner=Planner(
                capability_rollup={
                    "/api/v1/event/{event_id}/graph": "unsupported",
                    "/api/v1/event/{event_id}/incidents": "supported",
                }
            ),
            capability_repository=_FakeCapabilityRepository(),
            live_state_repository=live_state_repository,
            sql_executor=object(),
            live_worker=LiveWorker(),
            now_ms_factory=lambda: 1_800_000_000_000,
        )

        await orchestrator.run_event(event_id=event_id, sport_slug="tennis")

        self.assertEqual(len(live_state_repository.live_history), 1)
        self.assertEqual(live_state_repository.live_history[0].poll_profile, "terminal")
        self.assertEqual(len(live_state_repository.terminal_states), 1)
        self.assertEqual(live_state_repository.terminal_states[0].terminal_status, "finished")
        self.assertIsNotNone(live_state_repository.terminal_states[0].final_snapshot_id)


def _json_result(url: str, payload: object, *, status_code: int = 200) -> TransportResult:
    return TransportResult(
        resolved_url=url,
        status_code=status_code,
        headers={"Content-Type": "application/json"},
        body_bytes=json.dumps(payload).encode("utf-8"),
        attempts=(TransportAttempt(1, "proxy_1", status_code, None, None),),
        final_proxy_name="proxy_1",
        challenge_reason=None,
    )


if __name__ == "__main__":
    unittest.main()
