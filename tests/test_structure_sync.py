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

    def test_load_structure_runtime_config_uses_aggressive_structure_shaping_defaults(self) -> None:
        from schema_inspector.runtime import load_structure_runtime_config

        config = load_structure_runtime_config(
            env={
                "SCHEMA_INSPECTOR_STRUCTURE_PROXY_URLS": "http://isp-1.local:8080,http://isp-2.local:8080",
            }
        )

        self.assertEqual(config.proxy_request_cooldown_seconds, 0.5)
        self.assertEqual(config.proxy_request_jitter_seconds, 1.5)
        self.assertEqual(config.retry_policy.max_attempts, 2)
        self.assertEqual(config.retry_policy.challenge_max_attempts, 2)
        self.assertEqual(config.retry_policy.network_error_max_attempts, 2)
        self.assertEqual(config.retry_policy.backoff_seconds, 0.25)


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

    def test_load_managed_tournaments_uses_registry_refresh_interval_override(self) -> None:
        from schema_inspector.services.structure_planner import load_managed_tournaments
        from schema_inspector.services.tournament_registry_service import TournamentRegistryTarget

        targets = load_managed_tournaments(
            sport_slugs=("football",),
            registry_targets=(
                TournamentRegistryTarget(
                    source_slug="sofascore",
                    sport_slug="football",
                    unique_tournament_id=99,
                    refresh_interval_seconds=123,
                ),
            ),
        )

        self.assertEqual(
            [
                (target.sport_slug, target.unique_tournament_id, target.refresh_interval_seconds)
                for target in targets
            ],
            [("football", 99, 123.0)],
        )

    def test_load_managed_tournaments_registry_authoritative_skips_env_fallback_for_missing_sport(self) -> None:
        from schema_inspector.services.structure_planner import load_managed_tournaments

        targets = load_managed_tournaments(
            env={
                "SCHEMA_INSPECTOR_STRUCTURE_MANAGED_TOURNAMENTS": '{"football":[17,8]}'
            },
            sport_slugs=("football",),
            registry_targets=(),
            registry_authoritative=True,
        )

        self.assertEqual(targets, ())


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

    async def test_structure_worker_runtime_requeues_upstream_access_denied_without_crashing(self) -> None:
        from schema_inspector.sofascore_client import SofascoreAccessDeniedError
        from schema_inspector.workers.structure_worker import StructureSyncWorker

        scheduler = _FakeDelayedScheduler()
        payload_store = _FakePayloadStore()
        queue = _FakeRuntimeQueue()
        worker = StructureSyncWorker(
            orchestrator=_FailingStructureOrchestrator(SofascoreAccessDeniedError("Access denied by upstream")),
            queue=queue,
            consumer="structure-1",
            delayed_scheduler=scheduler,
            delayed_payload_store=payload_store,
            now_ms_factory=lambda: 1_800_000_000_000,
            block_ms=0,
        )
        entry = _structure_entry(job_type=JOB_SYNC_TOURNAMENT_STRUCTURE, entity_id=17)
        entry.values["attempt"] = "1"

        result = await worker.runtime._handle_entry(entry)

        self.assertEqual(result, "requeued")
        self.assertEqual(payload_store.saved_message_ids, ["1-1"])
        self.assertEqual(scheduler.calls, [(str(entry.values["job_id"]), 1_800_000_030_000)])
        self.assertEqual(queue.acked, [(STREAM_STRUCTURE_SYNC, GROUP_STRUCTURE_SYNC, ("1-1",))])


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

    async def test_service_app_registry_loader_keeps_registry_authoritative_when_rows_are_empty(self) -> None:
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
        ):
            repository = repository_cls.return_value
            repository.list_active_targets = mock.AsyncMock(return_value=())
            daemon = ServiceApp(app).build_structure_planner_daemon(sport_slugs=("football",))
            targets = await daemon._target_loader()

        self.assertEqual(targets, ())
        self.assertEqual(repository.list_active_targets.await_args.kwargs["surface"], "structure")


class StructureSyncServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_structure_sync_explicit_brackets_mode_uses_bracket_path(self) -> None:
        from schema_inspector.competition_parser import UniqueTournamentRecord
        from schema_inspector.services.structure_sync_service import run_structure_sync_for_tournament
        from schema_inspector.sport_profiles import resolve_sport_profile

        competition_job = mock.Mock()
        competition_job.run = mock.AsyncMock(
            side_effect=[
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=700,
                            slug="cup",
                            name="Cup",
                            category_id=5,
                            has_rounds=False,
                            has_playoff_series=True,
                        ),
                    ),
                    season_ids=(9001,),
                ),
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=700,
                            slug="cup",
                            name="Cup",
                            category_id=5,
                            has_rounds=False,
                            has_playoff_series=True,
                        ),
                    ),
                    season_ids=(9001,),
                ),
            ]
        )
        event_list_job = mock.Mock()
        event_list_job.run_round = mock.AsyncMock()
        event_list_job.run_brackets = mock.AsyncMock(return_value=_event_result((301, 302)))
        event_list_job.run_featured = mock.AsyncMock()
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock()

        brackets_profile = replace(resolve_sport_profile("basketball"), structure_sync_mode="brackets")

        with (
            mock.patch(
                "schema_inspector.services.structure_sync_service.build_source_adapter",
                return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
            ),
            mock.patch("schema_inspector.services.structure_sync_service.resolve_sport_profile", return_value=brackets_profile),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=700,
                sport_slug="basketball",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
                transport=object(),
            )

        self.assertEqual(result.mode, "brackets")
        self.assertEqual(result.event_ids, (301, 302))
        event_list_job.run_brackets.assert_awaited_once_with(
            unique_tournament_id=700,
            season_id=9001,
            sport_slug="basketball",
            timeout=20.0,
        )
        event_list_job.run_round.assert_not_awaited()
        event_list_job.run_featured.assert_not_awaited()
        event_list_job.run_unique_tournament_scheduled.assert_not_awaited()

    async def test_structure_sync_auto_prefers_rounds_over_brackets_when_both_are_present(self) -> None:
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
                            has_playoff_series=True,
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
                            has_playoff_series=True,
                        ),
                    ),
                    season_ids=(9001,),
                ),
            ]
        )
        event_list_job = mock.Mock()
        event_list_job.run_round = mock.AsyncMock(side_effect=[_event_result((101, 102)), _event_result(())])
        event_list_job.run_brackets = mock.AsyncMock()
        event_list_job.run_featured = mock.AsyncMock()
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock()

        with mock.patch(
            "schema_inspector.services.structure_sync_service.build_source_adapter",
            return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=17,
                sport_slug="football",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
                transport=object(),
            )

        self.assertEqual(result.mode, "rounds")
        self.assertEqual(result.event_ids, (101, 102))
        event_list_job.run_round.assert_awaited()
        event_list_job.run_brackets.assert_not_awaited()

    async def test_structure_sync_crawls_season_last_and_next_until_complete_before_edge_refresh_mode(self) -> None:
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
                    season_ids=(76986,),
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
                    season_ids=(76986,),
                ),
            ]
        )
        event_list_job = mock.Mock()
        event_list_job.run_round = mock.AsyncMock(side_effect=[_event_result((101,)), _event_result(())])
        event_list_job.run_brackets = mock.AsyncMock()
        event_list_job.run_featured = mock.AsyncMock()
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock()
        event_list_job.run_season_last = mock.AsyncMock(
            side_effect=[
                _event_result_with_has_next((201,), has_next=True),
                _event_result_with_has_next((202,), has_next=False),
            ]
        )
        event_list_job.run_season_next = mock.AsyncMock(
            side_effect=[
                _event_result_with_has_next((301,), has_next=True),
                _event_result_with_has_next((302,), has_next=False),
            ]
        )

        with mock.patch(
            "schema_inspector.services.structure_sync_service.build_source_adapter",
            return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=17,
                sport_slug="football",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
                transport=object(),
            )

        self.assertEqual(result.mode, "rounds")
        self.assertEqual(result.event_ids, (101, 201, 202, 301, 302))
        self.assertEqual(
            event_list_job.run_season_last.await_args_list,
            [
                mock.call(unique_tournament_id=17, season_id=76986, page=0, sport_slug="football", timeout=20.0),
                mock.call(unique_tournament_id=17, season_id=76986, page=1, sport_slug="football", timeout=20.0),
            ],
        )
        self.assertEqual(
            event_list_job.run_season_next.await_args_list,
            [
                mock.call(unique_tournament_id=17, season_id=76986, page=0, sport_slug="football", timeout=20.0),
                mock.call(unique_tournament_id=17, season_id=76986, page=1, sport_slug="football", timeout=20.0),
            ],
        )

    async def test_structure_sync_refreshes_only_first_season_pages_after_has_next_false_seen(self) -> None:
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
                    season_ids=(76986,),
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
                    season_ids=(76986,),
                ),
            ]
        )
        event_list_job = mock.Mock()
        event_list_job.run_round = mock.AsyncMock(side_effect=[_event_result((101,)), _event_result(())])
        event_list_job.run_brackets = mock.AsyncMock()
        event_list_job.run_featured = mock.AsyncMock()
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock()
        event_list_job.run_season_last = mock.AsyncMock(return_value=_event_result_with_has_next((201,), has_next=True))
        event_list_job.run_season_next = mock.AsyncMock(return_value=_event_result_with_has_next((301,), has_next=True))
        app = _FakeStructureApp(database=_FakeDatabase(surface_last_complete=True, surface_next_complete=True))

        with mock.patch(
            "schema_inspector.services.structure_sync_service.build_source_adapter",
            return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
        ):
            result = await run_structure_sync_for_tournament(
                app,
                unique_tournament_id=17,
                sport_slug="football",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
                transport=object(),
            )

        self.assertEqual(result.event_ids, (101, 201, 301))
        event_list_job.run_season_last.assert_awaited_once_with(
            unique_tournament_id=17,
            season_id=76986,
            page=0,
            sport_slug="football",
            timeout=20.0,
        )
        event_list_job.run_season_next.assert_awaited_once_with(
            unique_tournament_id=17,
            season_id=76986,
            page=0,
            sport_slug="football",
            timeout=20.0,
        )

    async def test_structure_sync_does_not_mark_season_surfaces_fresh_after_partial_page_failure(self) -> None:
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
                    season_ids=(76986,),
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
                    season_ids=(76986,),
                ),
            ]
        )
        event_list_job = mock.Mock()
        event_list_job.run_round = mock.AsyncMock(side_effect=[_event_result((101,)), _event_result(())])
        event_list_job.run_brackets = mock.AsyncMock()
        event_list_job.run_featured = mock.AsyncMock()
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock()
        event_list_job.run_season_last = mock.AsyncMock(return_value=_event_result_with_has_next((201,), has_next=True))
        event_list_job.run_season_next = mock.AsyncMock(side_effect=RuntimeError("page failed"))
        app = _FakeStructureApp()
        app.redis_backend = _FakeFreshnessRedis()

        with mock.patch(
            "schema_inspector.services.structure_sync_service.build_source_adapter",
            return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
        ):
            result = await run_structure_sync_for_tournament(
                app,
                unique_tournament_id=17,
                sport_slug="football",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
                transport=object(),
            )

        self.assertEqual(result.event_ids, (101, 201))
        self.assertEqual(app.redis_backend.set_calls, [])

    async def test_structure_sync_auto_chooses_brackets_when_rounds_absent_but_playoff_capability_present(self) -> None:
        from schema_inspector.competition_parser import UniqueTournamentRecord
        from schema_inspector.services.structure_sync_service import run_structure_sync_for_tournament
        from schema_inspector.sport_profiles import resolve_sport_profile

        competition_job = mock.Mock()
        competition_job.run = mock.AsyncMock(
            side_effect=[
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=88,
                            slug="playoffs",
                            name="Playoffs",
                            category_id=2,
                            has_rounds=False,
                            has_playoff_series=True,
                        ),
                    ),
                    season_ids=(7001,),
                ),
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=88,
                            slug="playoffs",
                            name="Playoffs",
                            category_id=2,
                            has_rounds=False,
                            has_playoff_series=True,
                        ),
                    ),
                    season_ids=(7001,),
                ),
            ]
        )
        event_list_job = mock.Mock()
        event_list_job.run_round = mock.AsyncMock()
        event_list_job.run_brackets = mock.AsyncMock(return_value=_event_result((601,)))
        event_list_job.run_featured = mock.AsyncMock()
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock()

        auto_profile = replace(resolve_sport_profile("basketball"), structure_sync_mode="auto")

        with (
            mock.patch(
                "schema_inspector.services.structure_sync_service.build_source_adapter",
                return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
            ),
            mock.patch("schema_inspector.services.structure_sync_service.resolve_sport_profile", return_value=auto_profile),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=88,
                sport_slug="basketball",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
                transport=object(),
            )

        self.assertEqual(result.mode, "brackets")
        self.assertEqual(result.event_ids, (601,))
        event_list_job.run_brackets.assert_awaited_once_with(
            unique_tournament_id=88,
            season_id=7001,
            sport_slug="basketball",
            timeout=20.0,
        )
        event_list_job.run_round.assert_not_awaited()

    async def test_structure_sync_rounds_empty_tries_brackets_before_calendar_when_playoff_capability_exists(self) -> None:
        from schema_inspector.competition_parser import UniqueTournamentRecord
        from schema_inspector.services.structure_sync_service import run_structure_sync_for_tournament

        competition_job = mock.Mock()
        competition_job.run = mock.AsyncMock(
            side_effect=[
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=91,
                            slug="cup",
                            name="Cup",
                            category_id=4,
                            has_rounds=True,
                            has_playoff_series=True,
                        ),
                    ),
                    season_ids=(8101,),
                ),
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=91,
                            slug="cup",
                            name="Cup",
                            category_id=4,
                            has_rounds=True,
                            has_playoff_series=True,
                        ),
                    ),
                    season_ids=(8101,),
                ),
            ]
        )
        event_list_job = mock.Mock()
        event_list_job.run_round = mock.AsyncMock(side_effect=[_event_result(()), _event_result(()), _event_result(())])
        event_list_job.run_brackets = mock.AsyncMock(return_value=_event_result((901,)))
        event_list_job.run_featured = mock.AsyncMock()
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock()

        with mock.patch(
            "schema_inspector.services.structure_sync_service.build_source_adapter",
            return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=91,
                sport_slug="football",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
                transport=object(),
            )

        self.assertEqual(result.mode, "rounds->brackets")
        self.assertEqual(result.event_ids, (901,))
        event_list_job.run_brackets.assert_awaited_once_with(
            unique_tournament_id=91,
            season_id=8101,
            sport_slug="football",
            timeout=20.0,
        )
        event_list_job.run_featured.assert_not_awaited()
        event_list_job.run_unique_tournament_scheduled.assert_not_awaited()

    async def test_structure_sync_explicit_brackets_failure_degrades_without_failing_sync(self) -> None:
        from schema_inspector.competition_parser import UniqueTournamentRecord
        from schema_inspector.services.structure_sync_service import run_structure_sync_for_tournament
        from schema_inspector.sport_profiles import resolve_sport_profile

        competition_job = mock.Mock()
        competition_job.run = mock.AsyncMock(
            side_effect=[
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=701,
                            slug="cup",
                            name="Cup",
                            category_id=5,
                            has_rounds=False,
                            has_playoff_series=True,
                        ),
                    ),
                    season_ids=(9001,),
                ),
                _competition_result(
                    tournaments=(
                        UniqueTournamentRecord(
                            id=701,
                            slug="cup",
                            name="Cup",
                            category_id=5,
                            has_rounds=False,
                            has_playoff_series=True,
                        ),
                    ),
                    season_ids=(9001,),
                ),
            ]
        )
        event_list_job = mock.Mock()
        event_list_job.run_round = mock.AsyncMock()
        event_list_job.run_brackets = mock.AsyncMock(side_effect=RuntimeError("bracket timeout"))
        event_list_job.run_featured = mock.AsyncMock(return_value=_event_result((401,)))
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock(return_value=_event_result(()))

        brackets_profile = replace(resolve_sport_profile("basketball"), structure_sync_mode="brackets")

        with (
            mock.patch(
                "schema_inspector.services.structure_sync_service.build_source_adapter",
                return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
            ),
            mock.patch("schema_inspector.services.structure_sync_service.resolve_sport_profile", return_value=brackets_profile),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=701,
                sport_slug="basketball",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
                transport=object(),
            )

        self.assertEqual(result.mode, "brackets->calendar")
        self.assertTrue(result.success)
        self.assertEqual(result.event_ids, (401,))
        self.assertIn("brackets", result.reason or "")
        event_list_job.run_brackets.assert_awaited_once()
        event_list_job.run_featured.assert_awaited_once()

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

        with mock.patch(
            "schema_inspector.services.structure_sync_service.build_source_adapter",
            return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=17,
                sport_slug="football",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
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

    async def test_structure_sync_rounds_mode_stops_after_consecutive_missing_round_404s(self) -> None:
        from schema_inspector.competition_parser import UniqueTournamentRecord
        from schema_inspector.services.structure_sync_service import run_structure_sync_for_tournament
        from schema_inspector.sofascore_client import SofascoreHttpError

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
        missing_round = SofascoreHttpError(
            "HTTP request failed: status=404",
            transport_result=SimpleNamespace(status_code=404),
        )
        event_list_job = mock.Mock()
        event_list_job.run_round = mock.AsyncMock(
            side_effect=[
                _event_result((101, 102)),
                missing_round,
                missing_round,
            ]
        )
        event_list_job.run_featured = mock.AsyncMock()
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock()

        with mock.patch(
            "schema_inspector.services.structure_sync_service.build_source_adapter",
            return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=17,
                sport_slug="football",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
                transport=object(),
            )

        self.assertEqual(result.mode, "rounds")
        self.assertEqual(result.rounds_probed, 3)
        self.assertEqual(result.rounds_with_events, 1)
        self.assertEqual(result.event_ids, (101, 102))
        self.assertEqual(event_list_job.run_round.await_count, 3)

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
            mock.patch(
                "schema_inspector.services.structure_sync_service.build_source_adapter",
                return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
            ),
            mock.patch("schema_inspector.services.structure_sync_service.resolve_sport_profile", return_value=tennis_auto),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=501,
                sport_slug="tennis",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
                transport=object(),
            )

        self.assertEqual(result.mode, "calendar")
        self.assertEqual(result.season_ids, (7001,))
        self.assertEqual(result.event_ids, (201,))
        event_list_job.run_round.assert_not_awaited()
        event_list_job.run_featured.assert_awaited_once()
        event_list_job.run_unique_tournament_scheduled.assert_awaited()

    async def test_structure_sync_calendar_mode_stops_after_three_empty_days(self) -> None:
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
        event_list_job.run_featured = mock.AsyncMock(return_value=_event_result(()))
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock(return_value=_event_result(()))

        tennis_auto = replace(
            resolve_sport_profile("tennis"),
            structure_sync_mode="auto",
            structure_calendar_forward_months=1,
            structure_calendar_backward_months=1,
        )

        with (
            mock.patch(
                "schema_inspector.services.structure_sync_service.build_source_adapter",
                return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
            ),
            mock.patch("schema_inspector.services.structure_sync_service.resolve_sport_profile", return_value=tennis_auto),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=501,
                sport_slug="tennis",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
                transport=object(),
                now_factory=lambda: datetime(2025, 1, 15, tzinfo=timezone.utc),
            )

        self.assertEqual(result.mode, "calendar")
        self.assertEqual(result.calendar_dates_probed, 6)
        self.assertEqual(event_list_job.run_unique_tournament_scheduled.await_count, 6)
        self.assertEqual(
            [call.kwargs["date"] for call in event_list_job.run_unique_tournament_scheduled.await_args_list],
            [
                "2025-01-15",
                "2025-01-16",
                "2025-01-17",
                "2025-01-14",
                "2025-01-13",
                "2025-01-12",
            ],
        )

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

        with mock.patch(
            "schema_inspector.services.structure_sync_service.build_source_adapter",
            return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=77,
                sport_slug="football",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
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

        with mock.patch(
            "schema_inspector.services.structure_sync_service.build_source_adapter",
            return_value=_FakeStructureSourceAdapter(competition_job, event_list_job),
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=17,
                sport_slug="football",
                runtime_config=SimpleNamespace(source_slug="sofascore"),
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
            mock.patch("schema_inspector.services.structure_sync_service.build_source_adapter") as adapter_factory,
        ):
            result = await run_structure_sync_for_tournament(
                _FakeStructureApp(),
                unique_tournament_id=17,
                sport_slug="football",
            )

        self.assertEqual(result.mode, "disabled")
        self.assertTrue(result.success)
        adapter_factory.assert_not_called()

    async def test_structure_sync_builds_source_adapter_and_uses_adapter_jobs(self) -> None:
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
        event_list_job.run_round = mock.AsyncMock(side_effect=[_event_result((101,)), _event_result(())])
        event_list_job.run_brackets = mock.AsyncMock()
        event_list_job.run_featured = mock.AsyncMock()
        event_list_job.run_unique_tournament_scheduled = mock.AsyncMock()
        adapter = _FakeStructureSourceAdapter(competition_job, event_list_job)
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        transport = object()
        app = _FakeStructureApp()

        with mock.patch(
            "schema_inspector.services.structure_sync_service.build_source_adapter",
            return_value=adapter,
        ) as adapter_factory:
            result = await run_structure_sync_for_tournament(
                app,
                unique_tournament_id=17,
                sport_slug="football",
                runtime_config=runtime_config,
                transport=transport,
            )

        self.assertEqual(result.mode, "rounds")
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
            transport=transport,
        )
        self.assertEqual(adapter.competition_build_calls, [app.database])
        self.assertEqual(adapter.event_list_build_calls, [app.database])

    async def test_structure_sync_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.services.structure_sync_service import run_structure_sync_for_tournament
        from schema_inspector.sources import UnsupportedSourceAdapterError

        with mock.patch(
            "schema_inspector.services.structure_sync_service.build_source_adapter",
            return_value=_UnsupportedStructureSourceAdapter(),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "competition ingestion is not wired for source secondary_source",
            ):
                await run_structure_sync_for_tournament(
                    _FakeStructureApp(),
                    unique_tournament_id=17,
                    sport_slug="football",
                    runtime_config=SimpleNamespace(source_slug="secondary_source"),
                    transport=object(),
                )


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
    def __init__(self) -> None:
        self.acked: list[tuple[str, str, tuple[str, ...]]] = []

    def read_group(self, *args, **kwargs):
        del args, kwargs
        return ()

    def ack(self, *args, **kwargs) -> int:
        stream, group, message_ids = args
        del kwargs
        self.acked.append((stream, group, tuple(message_ids)))
        return len(message_ids)


class _FakeDelayedScheduler:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def schedule(self, job_id: str, *, run_at_epoch_ms: int) -> None:
        self.calls.append((job_id, run_at_epoch_ms))


class _FakePayloadStore:
    def __init__(self) -> None:
        self.saved_message_ids: list[str] = []

    def save_entry(self, entry: StreamEntry) -> None:
        self.saved_message_ids.append(entry.message_id)


class _FakeOrchestrator:
    def __init__(self, *, success: bool, reason: str | None = None) -> None:
        self.success = success
        self.reason = reason
        self.calls: list[tuple[int, str]] = []

    async def run_structure_sync_for_tournament(self, *, unique_tournament_id: int, sport_slug: str):
        self.calls.append((unique_tournament_id, sport_slug))
        return type("Result", (), {"success": self.success, "reason": self.reason})()


class _FailingStructureOrchestrator:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    async def run_structure_sync_for_tournament(self, *, unique_tournament_id: int, sport_slug: str):
        del unique_tournament_id, sport_slug
        raise self.exc


class _FakeStreamQueue:
    def __init__(self) -> None:
        self.groups: list[tuple[str, str, str]] = []

    def ensure_group(self, stream: str, group: str, *, start_id: str = "0-0") -> None:
        self.groups.append((stream, group, start_id))


class _FakeStructureApp:
    def __init__(self, *, database=None) -> None:
        self.runtime_config = object()
        self.transport = object()
        self.database = database if database is not None else object()


class _FakeFreshnessRedis:
    def __init__(self) -> None:
        self.keys: set[str] = set()
        self.set_calls: list[tuple[str, object, dict[str, object]]] = []

    def exists(self, key: str, **kwargs) -> bool:
        del kwargs
        return key in self.keys

    def set(self, key: str, value: object, **kwargs) -> None:
        self.keys.add(key)
        self.set_calls.append((key, value, kwargs))


class _FakeStructureSourceAdapter:
    def __init__(self, competition_job, event_list_job) -> None:
        self.competition_job = competition_job
        self.event_list_job = event_list_job
        self.competition_build_calls: list[object] = []
        self.event_list_build_calls: list[object] = []

    def build_competition_job(self, database):
        self.competition_build_calls.append(database)
        return self.competition_job

    def build_event_list_job(self, database):
        self.event_list_build_calls.append(database)
        return self.event_list_job


class _UnsupportedStructureSourceAdapter:
    def build_competition_job(self, database):
        del database
        from schema_inspector.sources import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "competition ingestion is not wired for source secondary_source"
        )

    def build_event_list_job(self, database):
        del database
        return object()


class _FakeDatabaseConnection:
    def __init__(self, *, surface_last_complete: bool = False, surface_next_complete: bool = False) -> None:
        self.surface_last_complete = surface_last_complete
        self.surface_next_complete = surface_next_complete

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    async def fetchrow(self, *args, **kwargs):
        del args, kwargs
        return {
            "last_complete": self.surface_last_complete,
            "next_complete": self.surface_next_complete,
        }

class _FakeDatabase:
    def __init__(self, *, surface_last_complete: bool = False, surface_next_complete: bool = False) -> None:
        self.surface_last_complete = surface_last_complete
        self.surface_next_complete = surface_next_complete

    def connection(self):
        return _FakeDatabaseConnection(
            surface_last_complete=self.surface_last_complete,
            surface_next_complete=self.surface_next_complete,
        )


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


def _event_result_with_has_next(event_ids: tuple[int, ...], *, has_next: bool):
    events = tuple(SimpleNamespace(id=event_id) for event_id in event_ids)
    snapshot = SimpleNamespace(payload={"hasNextPage": has_next})
    parsed = SimpleNamespace(events=events, payload_snapshots=(snapshot,))
    return SimpleNamespace(parsed=parsed)


if __name__ == "__main__":
    unittest.main()
