from __future__ import annotations

import json
import unittest

from schema_inspector.fetch_executor import FetchExecutor
from schema_inspector.normalizers.worker import NormalizeWorker
from schema_inspector.parsers.registry import ParserRegistry
from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator
from schema_inspector.planner.planner import Planner
from schema_inspector.runtime import TransportAttempt, TransportResult


class _FakeTransport:
    def __init__(self, responses: dict[str, TransportResult]) -> None:
        self.responses = responses
        self.seen_urls: list[str] = []

    async def fetch(self, url: str, *, headers=None, timeout: float = 20.0) -> TransportResult:
        del headers, timeout
        self.seen_urls.append(url)
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
    def __init__(self) -> None:
        self.observations = []

    async def insert_observation(self, executor, record) -> None:
        del executor
        self.observations.append(record)

    async def upsert_rollup(self, executor, record) -> None:
        del executor, record


class TennisHybridPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_tennis_pipeline_fetches_special_routes(self) -> None:
        event_url = "https://www.sofascore.com/api/v1/event/15921219"
        statistics_url = "https://www.sofascore.com/api/v1/event/15921219/statistics"
        lineups_url = "https://www.sofascore.com/api/v1/event/15921219/lineups"
        point_by_point_url = "https://www.sofascore.com/api/v1/event/15921219/point-by-point"
        tennis_power_url = "https://www.sofascore.com/api/v1/event/15921219/tennis-power"

        transport = _FakeTransport(
            {
                event_url: _json_result(
                    event_url,
                    {
                        "event": {
                            "id": 15921219,
                            "slug": "sinner-alcaraz",
                            "tournament": {"id": 300, "slug": "wimbledon", "name": "Wimbledon", "uniqueTournament": {"id": 2361, "slug": "wimbledon", "name": "Wimbledon"}},
                            "season": {"id": 90001, "name": "Wimbledon 2026", "year": "2026"},
                            "status": {"type": "inprogress"},
                            "homeTeam": {"id": 101, "slug": "sinner", "name": "Jannik Sinner"},
                            "awayTeam": {"id": 102, "slug": "alcaraz", "name": "Carlos Alcaraz"},
                        }
                    },
                ),
                statistics_url: _json_result(statistics_url, {"statistics": []}),
                lineups_url: _json_result(lineups_url, {"home": {"players": []}, "away": {"players": []}}),
                point_by_point_url: _json_result(point_by_point_url, {"pointByPoint": [{"id": 1, "set": 1, "game": 1, "server": "home", "score": {"home": 15, "away": 0}}]}),
                tennis_power_url: _json_result(tennis_power_url, {"tennisPowerRankings": {"home": {"current": 0.61}, "away": {"current": 0.39}}}),
            }
        )
        raw_store = _FakeRawSnapshotStore()
        orchestrator = PilotOrchestrator(
            fetch_executor=FetchExecutor(transport=transport, raw_repository=raw_store, sql_executor=object()),
            snapshot_store=raw_store,
            normalize_worker=NormalizeWorker(ParserRegistry.default()),
            planner=Planner(
                capability_rollup={
                    "/api/v1/event/{event_id}/graph": "unsupported",
                    "/api/v1/event/{event_id}/incidents": "unsupported",
                }
            ),
            capability_repository=_FakeCapabilityRepository(),
            sql_executor=object(),
        )

        report = await orchestrator.run_event(event_id=15921219, sport_slug="tennis")

        self.assertIn(point_by_point_url, transport.seen_urls)
        self.assertIn(tennis_power_url, transport.seen_urls)
        self.assertIn("tennis_point_by_point", {item.parser_family for item in report.parse_results})
        self.assertIn("tennis_power", {item.parser_family for item in report.parse_results})


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
