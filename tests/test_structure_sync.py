from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
import unittest
from unittest import mock

from schema_inspector.jobs.envelope import JobEnvelope
from schema_inspector.jobs.types import JOB_DISCOVER_SPORT_SURFACE, JOB_SYNC_TOURNAMENT_STRUCTURE
from schema_inspector.queue.streams import GROUP_STRUCTURE_SYNC, STREAM_STRUCTURE_SYNC, StreamEntry
from schema_inspector.workers._stream_jobs import encode_stream_job


class StructureRuntimeConfigTests(unittest.TestCase):
    def test_load_structure_runtime_config_uses_only_structure_proxy_pool(self) -> None:
        from schema_inspector.runtime import load_structure_runtime_config

        config = load_structure_runtime_config(
            env={
                "SCHEMA_INSPECTOR_PROXY_URLS": "http://live-1.local:8080,http://live-2.local:8080",
                "SCHEMA_INSPECTOR_STRUCTURE_PROXY_URL": "http://dc-1.local:8080",
                "SCHEMA_INSPECTOR_STRUCTURE_PROXY_URLS": "http://dc-2.local:8080,http://dc-3.local:8080",
            }
        )

        self.assertEqual(
            [endpoint.url for endpoint in config.proxy_endpoints],
            [
                "http://dc-1.local:8080",
                "http://dc-2.local:8080",
                "http://dc-3.local:8080",
            ],
        )

    def test_load_structure_runtime_config_requires_non_residential_pool_by_default(self) -> None:
        from schema_inspector.runtime import load_structure_runtime_config

        with self.assertRaisesRegex(RuntimeError, "SCHEMA_INSPECTOR_STRUCTURE_PROXY_URLS"):
            load_structure_runtime_config(env={})


class StructurePlannerTests(unittest.IsolatedAsyncioTestCase):
    async def test_structure_planner_publishes_bootstrap_then_refresh_after_interval(self) -> None:
        from schema_inspector.services.structure_planner import (
            StructureCursorStore,
            StructurePlannerDaemon,
            StructurePlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()
        observed_now = iter((1_000, 5_000, 12_500))
        daemon = StructurePlannerDaemon(
            queue=queue,
            cursor_store=StructureCursorStore(backend),
            targets=(
                StructurePlanningTarget(
                    sport_slug="football",
                    unique_tournament_id=17,
                    refresh_interval_seconds=10.0,
                ),
            ),
            now_ms_factory=lambda: next(observed_now),
        )

        first = await daemon.tick()
        second = await daemon.tick()
        third = await daemon.tick()

        self.assertEqual((first, second, third), (1, 0, 1))
        self.assertEqual(
            [int(payload["entity_id"]) for _, payload in queue.published],
            [17, 17],
        )
        self.assertEqual(
            [payload["params_json"] for _, payload in queue.published],
            ['{"phase":"bootstrap","structure_mode":"rounds"}', '{"phase":"refresh","structure_mode":"rounds"}'],
        )
        self.assertEqual(
            backend.hashes["hash:etl:structure_sync_cursor"]["football:17:last_refresh_ms"],
            "12500",
        )

    def test_load_managed_tournaments_reads_env_overrides(self) -> None:
        from schema_inspector.services.structure_planner import load_managed_tournaments

        targets = load_managed_tournaments(
            env={
                "SCHEMA_INSPECTOR_STRUCTURE_MANAGED_TOURNAMENTS": '{"football":[17,8],"tennis":[101]}'
            },
            sport_slugs=("football", "tennis"),
        )

        self.assertEqual(
            [(target.sport_slug, target.unique_tournament_id) for target in targets],
            [("football", 17), ("football", 8), ("tennis", 101)],
        )

    def test_load_managed_tournaments_prefers_registry_rows_over_env_overrides_for_sport(self) -> None:
        from schema_inspector.services.structure_planner import load_managed_tournaments
        from schema_inspector.services.tournament_registry_service import TournamentRegistryTarget

        targets = load_managed_tournaments(
            env={
                "SCHEMA_INSPECTOR_STRUCTURE_MANAGED_TOURNAMENTS": '{"football":[17,8],"tennis":[101]}'
            },
            sport_slugs=("football", "tennis"),
            registry_targets=(
                TournamentRegistryTarget(source_slug="sofascore", sport_slug="football", unique_tournament_id=99),
            ),
        )

        self.assertEqual(
            [(target.sport_slug, target.unique_tournament_id) for target in targets],
            [("football", 99), ("tennis", 101)],
        )


class StructureWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_structure_worker_invokes_orchestrator_for_structure_jobs(self) -> None:
        from schema_inspector.workers.structure_worker import StructureSyncWorker

        orchestrator = _FakeOrchestrator(success=True)
        worker = StructureSyncWorker(
            orchestrator=orchestrator,
            queue=_FakeRuntimeQueue(),
            consumer="structure-1",
        )

        result = await worker.handle(_structure_entry(job_type=JOB_SYNC_TOURNAMENT_STRUCTURE, entity_id=17))

        self.assertEqual(result, "completed")
        self.assertEqual(orchestrator.calls, [(17, "football")])

    async def test_structure_worker_raises_when_structure_sync_reports_unsuccessful_result(self) -> None:
        from schema_inspector.workers.structure_worker import StructureSyncWorker

        orchestrator = _FakeOrchestrator(success=False, reason="upstream empty")
        worker = StructureSyncWorker(
            orchestrator=orchestrator,
            queue=_FakeRuntimeQueue(),
            consumer="structure-1",
        )

        with self.assertRaisesRegex(RuntimeError, "upstream empty"):
            await worker.handle(_structure_entry(job_type=JOB_SYNC_TOURNAMENT_STRUCTURE, entity_id=17))

    async def test_structure_worker_ignores_other_job_types(self) -> None:
        from schema_inspector.workers.structure_worker import StructureSyncWorker

        orchestrator = _FakeOrchestrator(success=True)
        worker = StructureSyncWorker(
            orchestrator=orchestrator,
            queue=_FakeRuntimeQueue(),
            consumer="structure-1",
        )

        result = await worker.handle(_structure_entry(job_type=JOB_DISCOVER_SPORT_SURFACE, entity_id=None))

        self.assertEqual(result, "ignored")
        self.assertEqual(orchestrator.calls, [])


class StructureServiceAppTests(unittest.TestCase):
    def test_service_app_ensures_structure_consumer_groups(self) -> None:
        from schema_inspector.services.service_app import ServiceApp

        stream_queue = _FakeStreamQueue()
        app = type(
            "App",
            (),
            {
                "redis_backend": object(),
                "stream_queue": stream_queue,
                "live_state_store": object(),
            },
        )()

        service_app = ServiceApp(app)
        service_app.ensure_structure_consumer_groups()

        self.assertIn((STREAM_STRUCTURE_SYNC, GROUP_STRUCTURE_SYNC, "0-0"), stream_queue.groups)

    def test_service_app_builds_structure_planner_from_managed_tournaments_env(self) -> None:
        from schema_inspector.services.service_app import ServiceApp

        stream_queue = _FakeStreamQueue()
        app = type(
            "App",
            (),
            {
                "redis_backend": object(),
                "stream_queue": stream_queue,
                "live_state_store": object(),
            },
        )()
        service_app = ServiceApp(app)

        with mock.patch.dict(
            "os.environ",
            {"SCHEMA_INSPECTOR_STRUCTURE_MANAGED_TOURNAMENTS": '{"football":[17,8]}'},
            clear=False,
        ):
            daemon = service_app.build_structure_planner_daemon(sport_slugs=("football",))

        self.assertEqual(
            [(target.sport_slug, target.unique_tournament_id) for target in daemon._static_targets],
            [("football", 17), ("football", 8)],
        )


class StructureServiceAppAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_service_app_build_structure_planner_daemon_uses_registry_backed_targets_when_database_available(self) -> None:
        from schema_inspector.services.service_app import ServiceApp
        from schema_inspector.services.tournament_registry_service import TournamentRegistryTarget

        stream_queue = _FakeStreamQueue()
        app = type(
            "App",
            (),
            {
                "redis_backend": object(),
                "stream_queue": stream_queue,
                "live_state_store": object(),
                "database": _FakeDatabase(),
            },
        )()

        with mock.patch("schema_inspector.services.service_app.TournamentRegistryRepository") as repository_cls:
            repository = repository_cls.return_value
            repository.list_active_targets = mock.AsyncMock(
                return_value=(
                    TournamentRegistryTarget(
                        source_slug="sofascore",
                        sport_slug="football",
                        unique_tournament_id=42,
                    ),
                )
            )
            daemon = ServiceApp(app).build_structure_planner_daemon(sport_slugs=("football",))
            targets = await daemon._target_loader()

        self.assertEqual(daemon._static_targets, ())
        self.assertEqual(
            [(target.sport_slug, target.unique_tournament_id) for target in targets],
            [("football", 42)],
        )
        repository.list_active_targets.assert_awaited_once()

    async def test_service_app_registry_loader_falls_back_to_env_targets_when_registry_read_fails(self) -> None:
        from schema_inspector.services.service_app import ServiceApp

        stream_queue = _FakeStreamQueue()
        app = type(
            "App",
            (),
            {
                "redis_backend": object(),
                "stream_queue": stream_queue,
                "live_state_store": object(),
                "database": _FakeDatabase(),
            },
        )()

        with (
            mock.patch("schema_inspector.services.service_app.TournamentRegistryRepository") as repository_cls,
            mock.patch.dict(
                "os.environ",
                {"SCHEMA_INSPECTOR_STRUCTURE_MANAGED_TOURNAMENTS": '{"football":[17,8]}'},
                clear=False,
            ),
            mock.patch("schema_inspector.services.service_app.logger") as logger_mock,
        ):
            repository = repository_cls.return_value
            repository.list_active_targets = mock.AsyncMock(side_effect=RuntimeError("registry unavailable"))
            daemon = ServiceApp(app).build_structure_planner_daemon(sport_slugs=("football",))
            targets = await daemon._target_loader()

        self.assertEqual(
            [(target.sport_slug, target.unique_tournament_id) for target in targets],
            [("football", 17), ("football", 8)],
        )
        logger_mock.warning.assert_called_once()


class StructureSyncServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_structure_sync_uses_rounds_path_for_round_based_profiles(self) -> None:
        from schema_inspector.competition_parser import UniqueTournamentRecord
        from schema_inspector.services.structure_sync_service import run_structure_sync_for_tournament

        competition_job = mock.Mock()
        competition_job.run = mock.AsyncMock(
            side_effect=[
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=17,
                            slug="premier-league",
                            name="Premier League",
                            category_id=1,
                            has_rounds=True,
                        ),
                    ),
                    season_ids=(9001,),
                ),
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=17,
                            slug="premier-league",
                            name="Premier League",
                            category_id=1,
                            has_rounds=True,
                        ),
                    ),
                    season_ids=(9001,),
                ),
            ]
        )
        event_list_job = mock.Mock()
        event_list_job.run_round = mock.AsyncMock(
            side_effect=[
                _event_result((101, 102)),
                _event_result(()),
            ]
        )
        event_list_job.run_featured = mock.AsyncMock()
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock()

        with (
            mock.patch("schema_inspector.services.structure_sync_service.CompetitionIngestJob", return_value=competition_job),
            mock.patch("schema_inspector.services.structure_sync_service.EventListIngestJob", return_value=event_list_job),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=17,
                sport_slug="football",
                runtime_config=object(),
                transport=object(),
            )

        self.assertEqual(result.mode, "rounds")
        self.assertEqual(result.season_ids, (9001,))
        self.assertEqual(result.rounds_probed, 2)
        self.assertEqual(result.rounds_with_events, 1)
        self.assertEqual(result.event_ids, (101, 102))
        event_list_job.run_round.assert_has_awaits(
            [
                mock.call(
                    unique_tournament_id=17,
                    season_id=9001,
                    round_number=1,
                    sport_slug="football",
                    timeout=20.0,
                ),
                mock.call(
                    unique_tournament_id=17,
                    season_id=9001,
                    round_number=2,
                    sport_slug="football",
                    timeout=20.0,
                ),
            ]
        )
        event_list_job.run_featured.assert_not_awaited()
        event_list_job.run_unique_tournament_scheduled.assert_not_awaited()

    async def test_structure_sync_auto_falls_back_to_calendar_when_tournament_has_no_rounds(self) -> None:
        from schema_inspector.competition_parser import UniqueTournamentRecord
        from schema_inspector.services.structure_sync_service import run_structure_sync_for_tournament
        from schema_inspector.sport_profiles import resolve_sport_profile

        competition_job = mock.Mock()
        competition_job.run = mock.AsyncMock(
            side_effect=[
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=501,
                            slug="masters",
                            name="Masters",
                            category_id=2,
                            has_rounds=False,
                        ),
                    ),
                    season_ids=(7001,),
                ),
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=501,
                            slug="masters",
                            name="Masters",
                            category_id=2,
                            has_rounds=False,
                        ),
                    ),
                    season_ids=(7001,),
                ),
            ]
        )
        event_list_job = mock.Mock()
        event_list_job.run_round = mock.AsyncMock()
        event_list_job.run_featured = mock.AsyncMock(return_value=_event_result((201,)))
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock(return_value=_event_result(()))

        tennis_auto = replace(
            resolve_sport_profile("tennis"),
            structure_sync_mode="auto",
            structure_calendar_forward_months=1,
        )

        with (
            mock.patch("schema_inspector.services.structure_sync_service.CompetitionIngestJob", return_value=competition_job),
            mock.patch("schema_inspector.services.structure_sync_service.EventListIngestJob", return_value=event_list_job),
            mock.patch("schema_inspector.services.structure_sync_service.resolve_sport_profile", return_value=tennis_auto),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=501,
                sport_slug="tennis",
                runtime_config=object(),
                transport=object(),
            )

        self.assertEqual(result.mode, "calendar")
        self.assertEqual(result.season_ids, (7001,))
        self.assertEqual(result.event_ids, (201,))
        event_list_job.run_round.assert_not_awaited()
        event_list_job.run_featured.assert_awaited_once()
        event_list_job.run_unique_tournament_scheduled.assert_awaited()

    async def test_structure_sync_returns_reason_when_upstream_reports_no_seasons(self) -> None:
        from schema_inspector.competition_parser import UniqueTournamentRecord
        from schema_inspector.services.structure_sync_service import run_structure_sync_for_tournament

        competition_job = mock.Mock()
        competition_job.run = mock.AsyncMock(
            return_value=_competition_result(
                tournaments=(
                    UniqueTournamentRecord(
                        id=77,
                        slug="cup",
                        name="Cup",
                        category_id=5,
                        has_rounds=True,
                    ),
                ),
                season_ids=(),
            )
        )
        event_list_job = mock.Mock()

        with (
            mock.patch("schema_inspector.services.structure_sync_service.CompetitionIngestJob", return_value=competition_job),
            mock.patch("schema_inspector.services.structure_sync_service.EventListIngestJob", return_value=event_list_job),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=77,
                sport_slug="football",
                runtime_config=object(),
                transport=object(),
            )

        self.assertTrue(result.success)
        self.assertEqual(result.mode, "rounds")
        self.assertEqual(result.season_ids, ())
        self.assertIn("no seasons", result.reason or "")

    async def test_rounds_to_calendar_fallback_preserves_now_factory_anchor(self) -> None:
        from schema_inspector.competition_parser import UniqueTournamentRecord
        from schema_inspector.services.structure_sync_service import run_structure_sync_for_tournament

        competition_job = mock.Mock()
        competition_job.run = mock.AsyncMock(
            side_effect=[
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=17,
                            slug="premier-league",
                            name="Premier League",
                            category_id=1,
                            has_rounds=True,
                        ),
                    ),
                    season_ids=(9001,),
                ),
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=17,
                            slug="premier-league",
                            name="Premier League",
                            category_id=1,
                            has_rounds=True,
                        ),
                    ),
                    season_ids=(9001,),
                ),
            ]
        )
        event_list_job = mock.Mock()
        event_list_job.run_round = mock.AsyncMock(
            side_effect=[
                _event_result(()),
                _event_result(()),
                _event_result(()),
            ]
        )
        event_list_job.run_featured = mock.AsyncMock(return_value=_event_result((501,)))
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock(return_value=_event_result(()))

        with (
            mock.patch("schema_inspector.services.structure_sync_service.CompetitionIngestJob", return_value=competition_job),
            mock.patch("schema_inspector.services.structure_sync_service.EventListIngestJob", return_value=event_list_job),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=17,
                sport_slug="football",
                runtime_config=object(),
                transport=object(),
                now_factory=lambda: datetime(2025, 1, 15, tzinfo=timezone.utc),
            )

        self.assertEqual(result.mode, "rounds->calendar")
        self.assertGreaterEqual(result.calendar_dates_probed, 1)
        first_scheduled_call = event_list_job.run_unique_tournament_scheduled.await_args_list[0]
        self.assertEqual(first_scheduled_call.kwargs["date"], "2025-01-15")

    async def test_disabled_profiles_short_circuit_before_client_and_jobs(self) -> None:
        from schema_inspector.services.structure_sync_service import run_structure_sync_for_tournament
        from schema_inspector.sport_profiles import resolve_sport_profile

        disabled_profile = replace(resolve_sport_profile("football"), structure_sync_mode="disabled")

        with (
            mock.patch("schema_inspector.services.structure_sync_service.resolve_sport_profile", return_value=disabled_profile),
            mock.patch("schema_inspector.services.structure_sync_service.SofascoreClient") as client_cls,
            mock.patch("schema_inspector.services.structure_sync_service.CompetitionIngestJob") as competition_job_cls,
            mock.patch("schema_inspector.services.structure_sync_service.EventListIngestJob") as event_list_job_cls,
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=17,
                sport_slug="football",
            )

        self.assertEqual(result.mode, "disabled")
        self.assertTrue(result.success)
        client_cls.assert_not_called()
        competition_job_cls.assert_not_called()
        event_list_job_cls.assert_not_called()


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(key, {}).update(dict(mapping))
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))


class _FakeQueue:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []

    def publish(self, stream: str, values: dict[str, object]) -> str:
        self.published.append((stream, dict(values)))
        return f"{stream}:{len(self.published)}"


class _FakeRuntimeQueue:
    def read_group(self, *args, **kwargs):
        del args, kwargs
        return ()

    def ack(self, *args, **kwargs) -> int:
        del args, kwargs
        return 0


class _FakeOrchestrator:
    def __init__(self, *, success: bool, reason: str | None = None) -> None:
        self.success = success
        self.reason = reason
        self.calls: list[tuple[int, str]] = []

    async def run_structure_sync_for_tournament(self, *, unique_tournament_id: int, sport_slug: str):
        self.calls.append((unique_tournament_id, sport_slug))
        return type("Result", (), {"success": self.success, "reason": self.reason})()


class _FakeStreamQueue:
    def __init__(self) -> None:
        self.groups: list[tuple[str, str, str]] = []

    def ensure_group(self, stream: str, group: str, *, start_id: str = "0-0") -> None:
        self.groups.append((stream, group, start_id))


class _FakeStructureApp:
    def __init__(self) -> None:
        self.runtime_config = object()
        self.transport = object()
        self.database = object()


class _FakeDatabaseConnection:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeDatabase:
    def connection(self):
        return _FakeDatabaseConnection()


def _structure_entry(*, job_type: str, entity_id: int | None) -> StreamEntry:
    payload = encode_stream_job(
        JobEnvelope.create(
            job_type=job_type,
            sport_slug="football",
            entity_type="unique_tournament",
            entity_id=entity_id,
            scope="structure",
            params={},
            priority=10,
            trace_id=None,
        )
    )
    return StreamEntry(
        stream=STREAM_STRUCTURE_SYNC,
        message_id="1-1",
        values=payload,
    )


def _competition_result(*, tournaments, season_ids: tuple[int, ...]):
    seasons = tuple(SimpleNamespace(id=season_id) for season_id in season_ids)
    parsed = SimpleNamespace(unique_tournaments=tournaments, seasons=seasons)
    return SimpleNamespace(parsed=parsed)


def _event_result(event_ids: tuple[int, ...]):
    events = tuple(SimpleNamespace(id=event_id) for event_id in event_ids)
    parsed = SimpleNamespace(events=events)
    return SimpleNamespace(parsed=parsed)


if __name__ == "__main__":
    unittest.main()
