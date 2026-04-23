from __future__ import annotations

import unittest

from schema_inspector.fetch_executor import FetchExecutor, PrefetchedFetchRecord
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

    async def insert_payload_snapshot_if_missing_returning_id(self, executor, record) -> int:
        return await self.insert_payload_snapshot_returning_id(executor, record)

    async def upsert_snapshot_head(self, executor, record) -> None:
        del executor, record

    def stage_snapshot(self, record) -> int:
        snapshot_id = self._next_id
        self._next_id += 1
        self.snapshots_by_id[snapshot_id] = record
        return snapshot_id

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


class _RecordingGate:
    def __init__(self) -> None:
        self.coarse_patterns = {
            "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/regularSeason"
        }
        self.events = ("prefetch-event",)

    async def blocked_endpoint_patterns(self, *, sport_slug, unique_tournament_id, season_id, endpoint_patterns):
        del sport_slug, unique_tournament_id, season_id
        return tuple(pattern for pattern in endpoint_patterns if pattern in self.coarse_patterns)

    async def coarse_filter_widget_jobs(self, *, sport_slug, unique_tournament_id, season_id, jobs):
        raise AssertionError("orchestrator should filter B_structural patterns before widget job generation")

    async def decide_widget_probe(self, *, sport_slug, unique_tournament_id, season_id, widget_job, endpoint_pattern):
        del sport_slug, unique_tournament_id, season_id, widget_job
        return {"should_fetch": endpoint_pattern not in self.coarse_patterns}

    async def record_widget_outcome(self, *, decision, endpoint_pattern, outcome):
        del decision, endpoint_pattern, outcome

    def build_replay_gate(self):
        return _ReplayGate(self.coarse_patterns)


class _ReplayGate:
    def __init__(self, coarse_patterns: set[str]) -> None:
        self.coarse_patterns = coarse_patterns

    async def blocked_endpoint_patterns(self, *, sport_slug, unique_tournament_id, season_id, endpoint_patterns):
        del sport_slug, unique_tournament_id, season_id
        return tuple(pattern for pattern in endpoint_patterns if pattern in self.coarse_patterns)

    async def coarse_filter_widget_jobs(self, *, sport_slug, unique_tournament_id, season_id, jobs):
        raise AssertionError("replay should honor blocked patterns before widget job generation")

    async def decide_widget_probe(self, *, sport_slug, unique_tournament_id, season_id, widget_job, endpoint_pattern):
        del sport_slug, unique_tournament_id, season_id, widget_job
        return {"should_fetch": endpoint_pattern not in self.coarse_patterns}

    async def record_widget_outcome(self, *, decision, endpoint_pattern, outcome):
        del decision, endpoint_pattern, outcome


class PilotSeasonWidgetNegativeCacheTests(unittest.IsolatedAsyncioTestCase):
    async def test_prefetch_and_replay_share_same_coarse_widget_suppression(self) -> None:
        from schema_inspector.cli import PrefetchedRun, ReplayFetchExecutor

        event_id = 14439306
        event_url = f"https://www.sofascore.com/api/v1/event/{event_id}"
        statistics_url = f"https://www.sofascore.com/api/v1/event/{event_id}/statistics"
        lineups_url = f"https://www.sofascore.com/api/v1/event/{event_id}/lineups"
        incidents_url = f"https://www.sofascore.com/api/v1/event/{event_id}/incidents"

        transport = _FakeTransport(
            {
                event_url: _json_result(
                    event_url,
                    {
                        "event": {
                            "id": event_id,
                            "slug": "team-a-team-b",
                            "tournament": {
                                "id": 100,
                                "slug": "nba",
                                "name": "NBA",
                                "uniqueTournament": {"id": 132, "slug": "nba", "name": "NBA"},
                            },
                            "season": {"id": 84695, "name": "NBA 25/26", "year": "25/26"},
                            "status": {"type": "finished"},
                            "homeTeam": {"id": 1, "slug": "lakers", "name": "Lakers"},
                            "awayTeam": {"id": 2, "slug": "celtics", "name": "Celtics"},
                        }
                    },
                ),
                statistics_url: _json_result(statistics_url, {"statistics": []}),
                lineups_url: _json_result(lineups_url, {"home": {"players": []}, "away": {"players": []}}),
                incidents_url: _json_result(incidents_url, {"incidents": []}),
            }
        )
        snapshot_store = _FakeRawSnapshotStore()
        gate = _RecordingGate()
        prefetch_executor = FetchExecutor(
            transport=transport,
            raw_repository=snapshot_store,
            sql_executor=object(),
            snapshot_store=snapshot_store,
            write_mode="deferred",
        )
        prefetch_orchestrator = PilotOrchestrator(
            fetch_executor=prefetch_executor,
            snapshot_store=snapshot_store,
            normalize_worker=NormalizeWorker(ParserRegistry.default()),
            planner=_RecordingPlanner(capability_rollup={}),
            capability_repository=None,
            sql_executor=None,
            season_widget_gate=gate,
        )

        await prefetch_orchestrator.run_event(event_id=event_id, sport_slug="basketball", hydration_mode="full")

        self.assertEqual(
            prefetch_orchestrator.planner.blocked_endpoint_patterns_calls,
            [
                (
                    "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/top-players/regularSeason",
                )
            ],
        )

        self.assertNotIn(
            "https://www.sofascore.com/api/v1/unique-tournament/132/season/84695/top-players/regularSeason",
            transport.seen_urls,
        )

        prefetched_run = PrefetchedRun(
            event_id=event_id,
            sport_slug="basketball",
            fetch_records=prefetch_executor.prefetched_records,
            snapshot_store=snapshot_store,
            initial_capability_rollup={},
            widget_negative_cache_events=gate.events,
            replay_widget_gate=gate.build_replay_gate(),
        )
        replay_orchestrator = PilotOrchestrator(
            fetch_executor=ReplayFetchExecutor(prefetched_run),
            snapshot_store=snapshot_store,
            normalize_worker=NormalizeWorker(ParserRegistry.default()),
            planner=_RecordingPlanner(capability_rollup={}),
            capability_repository=None,
            sql_executor=object(),
            season_widget_gate=prefetched_run.replay_widget_gate,
        )

        await replay_orchestrator.run_event(event_id=event_id, sport_slug="basketball", hydration_mode="full")


class _RecordingPlanner(Planner):
    def __init__(self, capability_rollup) -> None:
        super().__init__(capability_rollup=capability_rollup)
        self.blocked_endpoint_patterns_calls: list[tuple[str, ...]] = []

    def plan_season_widgets(
        self,
        sport_slug: str,
        *,
        unique_tournament_id: int,
        season_id: int,
        blocked_endpoint_patterns: tuple[str, ...] = (),
    ) -> tuple[object, ...]:
        self.blocked_endpoint_patterns_calls.append(tuple(blocked_endpoint_patterns))
        return super().plan_season_widgets(
            sport_slug,
            unique_tournament_id=unique_tournament_id,
            season_id=season_id,
            blocked_endpoint_patterns=blocked_endpoint_patterns,
        )


def _json_result(url: str, payload: object, *, status_code: int = 200) -> TransportResult:
    body = __import__("json").dumps(payload).encode("utf-8")
    return TransportResult(
        resolved_url=url,
        status_code=status_code,
        headers={"content-type": "application/json"},
        body_bytes=body,
        attempts=(TransportAttempt(1, "proxy_1", status_code, None, None),),
        final_proxy_name="proxy_1",
        challenge_reason=None,
    )


if __name__ == "__main__":
    unittest.main()
