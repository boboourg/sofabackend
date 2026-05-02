from __future__ import annotations

import unittest
from types import SimpleNamespace

from schema_inspector.fetch_models import FetchOutcomeEnvelope
from schema_inspector.parsers.base import ParseResult
from schema_inspector.endpoints import EVENT_H2H_ENDPOINT, EVENT_H2H_EVENTS_ENDPOINT
from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator


class _FakeFetchExecutor:
    def __init__(self) -> None:
        self.tasks = []

    async def execute(self, task):
        self.tasks.append(task)
        snapshot_id = len(self.tasks)
        return FetchOutcomeEnvelope(
            trace_id=task.trace_id,
            job_id=task.job_id,
            endpoint_pattern=task.endpoint_pattern,
            source_url=task.source_url,
            resolved_url=task.source_url,
            http_status=200,
            classification="success_json",
            proxy_id="proxy_1",
            challenge_reason=None,
            snapshot_id=snapshot_id,
            payload_hash="hash",
            payload_root_keys=("event",),
            is_valid_json=True,
            fetched_at="2026-05-01T00:00:00+00:00",
        )


class _FakeSnapshotStore:
    def load_snapshot(self, snapshot_id: int):
        return SimpleNamespace(snapshot_id=snapshot_id)


class _FakeNormalizeWorker:
    def __init__(self, *, duplicate_player: bool = False) -> None:
        self.duplicate_player = duplicate_player

    def handle(self, snapshot):
        if snapshot.snapshot_id != 1:
            return ParseResult(
                snapshot_id=snapshot.snapshot_id,
                parser_family="fake",
                parser_version="v1",
                status="parsed",
            )
        player_rows = ({"id": 77}, {"id": 77}) if self.duplicate_player else ({"id": 77},)
        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family="fake",
            parser_version="v1",
            status="parsed",
            entity_upserts={
                "event": (
                    {
                        "id": 123,
                        "status_type": "scheduled",
                        "start_timestamp": 1_900_000_000,
                    },
                ),
                "team": ({"id": 11},),
                "player": player_rows,
                "manager": ({"id": 33},),
            },
        )


class _FakePlanner:
    def __init__(self) -> None:
        self.capability_rollup = {}

    def expand(self, job):
        del job
        return ()

    def plan_lineup_followups(self, job, parse_result):
        del job, parse_result
        return ()

    def plan_season_widget_patterns(self, sport_slug: str, *, unique_tournament_id: int, season_id: int):
        del sport_slug, unique_tournament_id, season_id
        return ()

    def plan_season_widgets(
        self,
        sport_slug: str,
        *,
        unique_tournament_id: int,
        season_id: int,
        blocked_endpoint_patterns=(),
    ):
        del sport_slug, unique_tournament_id, season_id, blocked_endpoint_patterns
        return ()


class _FakeFreshnessStore:
    def __init__(self, fresh_keys: set[str] | None = None) -> None:
        self.fresh_keys = set(fresh_keys or ())
        self.checked: list[str] = []

    def is_fresh(self, key: str) -> bool:
        self.checked.append(key)
        return key in self.fresh_keys


class PilotOrchestratorFreshnessTests(unittest.IsolatedAsyncioTestCase):
    async def test_player_fan_out_skipped_when_profile_is_fresh(self) -> None:
        fetch_executor = _FakeFetchExecutor()
        freshness_store = _FakeFreshnessStore({"freshness:player:77"})
        orchestrator = _build_orchestrator(fetch_executor=fetch_executor, freshness_store=freshness_store)

        await orchestrator.run_event(event_id=123, sport_slug="football")

        endpoint_patterns = [task.endpoint_pattern for task in fetch_executor.tasks]
        self.assertIn("/api/v1/team/{team_id}", endpoint_patterns)
        self.assertNotIn("/api/v1/player/{player_id}", endpoint_patterns)
        self.assertEqual(
            freshness_store.checked,
            ["freshness:team:11", "freshness:player:77", "freshness:manager:33"],
        )

    async def test_team_and_manager_fan_out_skipped_when_profiles_are_fresh(self) -> None:
        fetch_executor = _FakeFetchExecutor()
        freshness_store = _FakeFreshnessStore({"freshness:team:11", "freshness:manager:33"})
        orchestrator = _build_orchestrator(fetch_executor=fetch_executor, freshness_store=freshness_store)

        await orchestrator.run_event(event_id=123, sport_slug="football")

        endpoint_patterns = [task.endpoint_pattern for task in fetch_executor.tasks]
        self.assertNotIn("/api/v1/team/{team_id}", endpoint_patterns)
        self.assertIn("/api/v1/player/{player_id}", endpoint_patterns)
        self.assertNotIn("/api/v1/manager/{manager_id}", endpoint_patterns)

    async def test_player_fan_out_proceeds_with_freshness_fields_when_stale(self) -> None:
        fetch_executor = _FakeFetchExecutor()
        freshness_store = _FakeFreshnessStore()
        orchestrator = _build_orchestrator(fetch_executor=fetch_executor, freshness_store=freshness_store)

        await orchestrator.run_event(event_id=123, sport_slug="football")

        player_tasks = [
            task
            for task in fetch_executor.tasks
            if task.endpoint_pattern == "/api/v1/player/{player_id}"
        ]
        self.assertEqual(len(player_tasks), 1)
        self.assertEqual(player_tasks[0].freshness_key, "freshness:player:77")
        self.assertEqual(player_tasks[0].freshness_ttl_seconds, 86_400)

        team_tasks = [
            task
            for task in fetch_executor.tasks
            if task.endpoint_pattern == "/api/v1/team/{team_id}"
        ]
        manager_tasks = [
            task
            for task in fetch_executor.tasks
            if task.endpoint_pattern == "/api/v1/manager/{manager_id}"
        ]
        self.assertEqual(team_tasks[0].freshness_key, "freshness:team:11")
        self.assertEqual(team_tasks[0].freshness_ttl_seconds, 86_400)
        self.assertEqual(manager_tasks[0].freshness_key, "freshness:manager:33")
        self.assertEqual(manager_tasks[0].freshness_ttl_seconds, 604_800)

    async def test_h2h_endpoint_skipped_when_fresh(self) -> None:
        fetch_executor = _FakeFetchExecutor()
        freshness_store = _FakeFreshnessStore(
            {"freshness:event-detail:123:/api/v1/event/{event_id}/h2h"}
        )
        orchestrator = _build_orchestrator(fetch_executor=fetch_executor, freshness_store=freshness_store)

        outcome, parsed = await orchestrator._fetch_gated_event_endpoint(
            endpoint=EVENT_H2H_ENDPOINT,
            sport_slug="football",
            path_params={"event_id": 123},
            context_entity_type="event",
            context_entity_id=123,
            context_event_id=123,
            fetch_reason="hydrate_special_route",
            status_phase="pre",
        )

        self.assertIsNone(outcome)
        self.assertIsNone(parsed)
        self.assertEqual(fetch_executor.tasks, [])

    async def test_custom_h2h_events_endpoint_uses_static_freshness_key(self) -> None:
        fetch_executor = _FakeFetchExecutor()
        freshness_store = _FakeFreshnessStore()
        orchestrator = _build_orchestrator(fetch_executor=fetch_executor, freshness_store=freshness_store)

        await orchestrator._fetch_gated_event_endpoint(
            endpoint=EVENT_H2H_EVENTS_ENDPOINT,
            sport_slug="football",
            path_params={"event_id": 123, "custom_id": "abc123"},
            context_entity_type="event",
            context_entity_id=123,
            context_event_id=123,
            fetch_reason="hydrate_special_route",
            status_phase="pre",
        )

        task = fetch_executor.tasks[-1]
        self.assertEqual(
            task.freshness_key,
            "freshness:event-detail-custom:abc123:/api/v1/event/{custom_id}/h2h/events",
        )
        self.assertEqual(task.freshness_ttl_seconds, 86_400)

    async def test_event_player_endpoint_uses_short_freshness_ttl(self) -> None:
        fetch_executor = _FakeFetchExecutor()
        freshness_store = _FakeFreshnessStore()
        orchestrator = _build_orchestrator(fetch_executor=fetch_executor, freshness_store=freshness_store)

        await orchestrator._run_special_job(
            job=SimpleNamespace(
                job_type="hydrate_special_route",
                params={"special_kind": "event_player_statistics", "player_id": 77},
            ),
            sport_slug="football",
            event_id=123,
            status_phase="live",
        )

        task = fetch_executor.tasks[-1]
        self.assertEqual(
            task.freshness_key,
            "freshness:event-player:123:77:/api/v1/event/{event_id}/player/{player_id}/statistics",
        )
        self.assertEqual(task.freshness_ttl_seconds, 300)

    async def test_duplicate_player_still_uses_in_process_hydrated_entities_dedup(self) -> None:
        fetch_executor = _FakeFetchExecutor()
        orchestrator = _build_orchestrator(
            fetch_executor=fetch_executor,
            freshness_store=_FakeFreshnessStore(),
            normalize_worker=_FakeNormalizeWorker(duplicate_player=True),
        )

        await orchestrator.run_event(event_id=123, sport_slug="football")

        player_tasks = [
            task
            for task in fetch_executor.tasks
            if task.endpoint_pattern == "/api/v1/player/{player_id}"
        ]
        self.assertEqual(len(player_tasks), 1)


def _build_orchestrator(
    *,
    fetch_executor,
    freshness_store,
    normalize_worker=None,
) -> PilotOrchestrator:
    return PilotOrchestrator(
        fetch_executor=fetch_executor,
        snapshot_store=_FakeSnapshotStore(),
        normalize_worker=normalize_worker or _FakeNormalizeWorker(),
        planner=_FakePlanner(),
        capability_repository=None,
        sql_executor=object(),
        freshness_store=freshness_store,
        now_ms_factory=lambda: 1_800_000_000_000,
    )


if __name__ == "__main__":
    unittest.main()
