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
    async def insert_observation(self, executor, record) -> None:
        del executor, record

    async def upsert_rollup(self, executor, record) -> None:
        del executor, record


class EsportsHybridPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_esports_core_pipeline_still_fetches_games_special(self) -> None:
        event_url = "https://www.sofascore.com/api/v1/event/20001"
        statistics_url = "https://www.sofascore.com/api/v1/event/20001/statistics"
        lineups_url = "https://www.sofascore.com/api/v1/event/20001/lineups"
        esports_games_url = "https://www.sofascore.com/api/v1/event/20001/esports-games"

        transport = _FakeTransport(
            {
                event_url: _json_result(
                    event_url,
                    {
                        "event": {
                            "id": 20001,
                            "slug": "spirit-vitality",
                            "tournament": {
                                "id": 501,
                                "slug": "esl-pro-league",
                                "name": "ESL Pro League",
                                "uniqueTournament": {"id": 18501, "slug": "esl-pro-league", "name": "ESL Pro League"},
                            },
                            "season": {"id": 91001, "name": "EPL 2026", "year": "2026"},
                            "status": {"type": "notstarted"},
                            "homeTeam": {"id": 9001, "slug": "spirit", "name": "Spirit"},
                            "awayTeam": {"id": 9002, "slug": "vitality", "name": "Vitality"},
                        }
                    },
                ),
                statistics_url: _json_result(statistics_url, {"statistics": []}),
                lineups_url: _json_result(lineups_url, {"home": {"players": []}, "away": {"players": []}}),
                esports_games_url: _json_result(esports_games_url, {"games": [{"id": 1, "status": "finished", "mapName": "Dust2"}]}),
            }
        )
        raw_store = _FakeRawSnapshotStore()
        orchestrator = PilotOrchestrator(
            fetch_executor=FetchExecutor(transport=transport, raw_repository=raw_store, sql_executor=object()),
            snapshot_store=raw_store,
            normalize_worker=NormalizeWorker(ParserRegistry.default()),
            planner=Planner(capability_rollup={}),
            capability_repository=_FakeCapabilityRepository(),
            sql_executor=object(),
        )

        await orchestrator.run_event(event_id=20001, sport_slug="esports", hydration_mode="core")

        self.assertIn(esports_games_url, transport.seen_urls)

    async def test_esports_pipeline_uses_thin_special_adapter(self) -> None:
        event_url = "https://www.sofascore.com/api/v1/event/20001"
        statistics_url = "https://www.sofascore.com/api/v1/event/20001/statistics"
        lineups_url = "https://www.sofascore.com/api/v1/event/20001/lineups"
        esports_games_url = "https://www.sofascore.com/api/v1/event/20001/esports-games"
        team_url = "https://www.sofascore.com/api/v1/team/9001"

        transport = _FakeTransport(
            {
                event_url: _json_result(
                    event_url,
                    {
                        "event": {
                            "id": 20001,
                            "slug": "spirit-vitality",
                            "tournament": {
                                "id": 501,
                                "slug": "esl-pro-league",
                                "name": "ESL Pro League",
                                "uniqueTournament": {"id": 18501, "slug": "esl-pro-league", "name": "ESL Pro League"},
                            },
                            "season": {"id": 91001, "name": "EPL 2026", "year": "2026"},
                            "status": {"type": "inprogress"},
                            "homeTeam": {"id": 9001, "slug": "spirit", "name": "Spirit"},
                            "awayTeam": {"id": 9002, "slug": "vitality", "name": "Vitality"},
                        }
                    },
                ),
                statistics_url: _json_result(statistics_url, {"statistics": []}),
                lineups_url: _json_result(lineups_url, {"home": {"players": []}, "away": {"players": []}}),
                esports_games_url: _json_result(esports_games_url, {"games": [{"id": 1, "status": "finished", "mapName": "Dust2"}]}),
            }
        )
        raw_store = _FakeRawSnapshotStore()
        orchestrator = PilotOrchestrator(
            fetch_executor=FetchExecutor(transport=transport, raw_repository=raw_store, sql_executor=object()),
            snapshot_store=raw_store,
            normalize_worker=NormalizeWorker(ParserRegistry.default()),
            planner=Planner(capability_rollup={"/api/v1/event/{event_id}/graph": "unsupported"}),
            capability_repository=_FakeCapabilityRepository(),
            sql_executor=object(),
        )

        report = await orchestrator.run_event(event_id=20001, sport_slug="esports")

        self.assertIn(esports_games_url, transport.seen_urls)
        self.assertNotIn(team_url, transport.seen_urls)
        self.assertIn("esports_games", {item.parser_family for item in report.parse_results})


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
