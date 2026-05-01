from __future__ import annotations

import argparse
import asyncio
import io
import types
import unittest
from contextlib import redirect_stdout
from unittest import mock


class HybridCliTests(unittest.IsolatedAsyncioTestCase):
    async def test_collect_health_report_includes_runtime_gate_fields(self) -> None:
        from schema_inspector.ops.health import collect_health_report

        report = await collect_health_report(
            sql_executor=_FakeSqlExecutor(
                {
                    "SELECT COUNT(*) FROM api_payload_snapshot": 7,
                    "SELECT COUNT(*) FROM endpoint_capability_rollup": 3,
                }
            ),
            live_state_store=_FakeLiveStateStore(_FakeLaneRedisBackend({"live:hot": ("1", "2"), "live:warm": ("3",)})),
            redis_backend=_FakeRedis(),
        )

        self.assertTrue(report.database_ok)
        self.assertTrue(report.redis_ok)
        self.assertEqual(report.redis_backend_kind, "fakeredis")
        self.assertEqual(report.snapshot_count, 7)
        self.assertEqual(report.capability_rollup_count, 3)
        self.assertEqual(report.live_hot_count, 2)
        self.assertEqual(report.live_warm_count, 1)
        self.assertEqual(report.live_cold_count, 0)

    async def test_collect_health_report_includes_drift_summary_flags(self) -> None:
        from schema_inspector.ops.health import collect_health_report

        report = await collect_health_report(
            sql_executor=_FakeSqlExecutor(
                {
                    "SELECT COUNT(*) FROM api_payload_snapshot": 7,
                    "SELECT COUNT(*) FROM endpoint_capability_rollup": 3,
                },
                rows_by_query={
                    "health_drift": [
                        {
                            "sport_slug": "football",
                            "surface": "sport_live_events",
                            "reason": "snapshot_older_than_terminal_state",
                        }
                    ]
                },
            ),
            live_state_store=_FakeLiveStateStore(_FakeLaneRedisBackend({})),
            redis_backend=_FakeRedis(),
        )

        self.assertEqual(report.drift_summary.flag_count, 1)
        self.assertEqual(
            report.drift_summary.flags[0].surface,
            "sport_live_events",
        )
        self.assertEqual(report.drift_summary.flags[0].sport_slug, "football")
        self.assertEqual(
            report.drift_summary.flags[0].reason,
            "snapshot_older_than_terminal_state",
        )

    async def test_collect_health_report_includes_coverage_summary_rollup(self) -> None:
        from schema_inspector.ops.health import collect_health_report

        report = await collect_health_report(
            sql_executor=_FakeSqlExecutor(
                {
                    "SELECT COUNT(*) FROM api_payload_snapshot": 7,
                    "SELECT COUNT(*) FROM endpoint_capability_rollup": 3,
                },
                rows_by_query={
                    "health_coverage": [
                        {
                            "tracked_scope_count": 11,
                            "fresh_scope_count": 7,
                            "stale_scope_count": 2,
                            "other_scope_count": 2,
                            "source_count": 2,
                            "sport_count": 3,
                            "surface_count": 4,
                            "avg_completeness_ratio": 0.625,
                        }
                    ]
                },
            ),
            live_state_store=_FakeLiveStateStore(_FakeLaneRedisBackend({})),
            redis_backend=_FakeRedis(),
        )

        self.assertEqual(report.coverage_summary.tracked_scope_count, 11)
        self.assertEqual(report.coverage_summary.fresh_scope_count, 7)
        self.assertEqual(report.coverage_summary.stale_scope_count, 2)
        self.assertEqual(report.coverage_summary.other_scope_count, 2)
        self.assertEqual(report.coverage_summary.source_count, 2)
        self.assertEqual(report.coverage_summary.sport_count, 3)
        self.assertEqual(report.coverage_summary.surface_count, 4)
        self.assertAlmostEqual(report.coverage_summary.avg_completeness_ratio, 0.625)

    async def test_collect_health_report_includes_coverage_alert_when_stale_scopes_exist(self) -> None:
        from schema_inspector.ops.health import collect_health_report

        report = await collect_health_report(
            sql_executor=_FakeSqlExecutor(
                {
                    "SELECT COUNT(*) FROM api_payload_snapshot": 7,
                    "SELECT COUNT(*) FROM endpoint_capability_rollup": 3,
                },
                rows_by_query={
                    "health_coverage": [
                        {
                            "tracked_scope_count": 11,
                            "fresh_scope_count": 7,
                            "stale_scope_count": 2,
                            "other_scope_count": 2,
                            "source_count": 2,
                            "sport_count": 3,
                            "surface_count": 4,
                            "avg_completeness_ratio": 0.625,
                        }
                    ]
                },
            ),
            live_state_store=_FakeLiveStateStore(_FakeLaneRedisBackend({})),
            redis_backend=_FakeRedis(),
        )

        self.assertEqual(report.coverage_alert_summary.flag_count, 1)
        self.assertEqual(report.coverage_alert_summary.flags[0].severity, "warning")
        self.assertEqual(report.coverage_alert_summary.flags[0].reason, "stale_coverage_scopes_present")
        self.assertEqual(report.coverage_alert_summary.flags[0].stale_scope_count, 2)

    async def test_collect_health_report_keeps_coverage_alerts_empty_when_no_stale_scopes_exist(self) -> None:
        from schema_inspector.ops.health import collect_health_report

        report = await collect_health_report(
            sql_executor=_FakeSqlExecutor(
                {
                    "SELECT COUNT(*) FROM api_payload_snapshot": 7,
                    "SELECT COUNT(*) FROM endpoint_capability_rollup": 3,
                },
                rows_by_query={
                    "health_coverage": [
                        {
                            "tracked_scope_count": 11,
                            "fresh_scope_count": 11,
                            "stale_scope_count": 0,
                            "other_scope_count": 0,
                            "source_count": 2,
                            "sport_count": 3,
                            "surface_count": 4,
                            "avg_completeness_ratio": 1.0,
                        }
                    ]
                },
            ),
            live_state_store=_FakeLiveStateStore(_FakeLaneRedisBackend({})),
            redis_backend=_FakeRedis(),
        )

        self.assertEqual(report.coverage_alert_summary.flag_count, 0)
        self.assertEqual(report.coverage_alert_summary.flags, ())

    async def test_collect_health_report_includes_reconcile_policy_summary(self) -> None:
        from schema_inspector.ops.health import collect_health_report

        report = await collect_health_report(
            sql_executor=_FakeSqlExecutor(
                {
                    "SELECT COUNT(*) FROM api_payload_snapshot": 7,
                    "SELECT COUNT(*) FROM endpoint_capability_rollup": 3,
                }
            ),
            live_state_store=_FakeLiveStateStore(_FakeLaneRedisBackend({})),
            redis_backend=_FakeRedis(),
        )

        self.assertTrue(report.reconcile_policy_summary.policy_enabled)
        self.assertEqual(report.reconcile_policy_summary.primary_source_slug, "sofascore")
        self.assertEqual(report.reconcile_policy_summary.source_count, 2)
        self.assertEqual(
            tuple((item.source_slug, item.priority) for item in report.reconcile_policy_summary.sources),
            (
                ("sofascore", 100),
                ("secondary_source", 80),
            ),
        )

    async def test_hybrid_app_collect_health_passes_stream_queue_to_report_builder(self) -> None:
        import schema_inspector.cli as hybrid_cli
        from schema_inspector.runtime import RuntimeConfig

        captured: dict[str, object] = {}

        async def _fake_collect_health_report(**kwargs):
            captured.update(kwargs)
            return object()

        class _FakeDatabase:
            def connection(self):
                return _FakeAsyncpgDatabaseContext(object())

        original_collect_health_report = hybrid_cli.collect_health_report
        try:
            hybrid_cli.collect_health_report = _fake_collect_health_report
            app = hybrid_cli.HybridApp(
                database=_FakeDatabase(),
                runtime_config=RuntimeConfig(require_proxy=False),
                redis_backend=_FakeRedisBackend(),
            )
            await app.collect_health()
            await app.close()
        finally:
            hybrid_cli.collect_health_report = original_collect_health_report

        self.assertIs(captured["stream_queue"], app.stream_queue)
        self.assertIs(captured["live_state_store"], app.live_state_store)
        self.assertIs(captured["redis_backend"], app.redis_backend)

    def test_parser_accepts_allow_memory_redis_global_flag(self) -> None:
        from schema_inspector.cli import _build_parser

        parser = _build_parser()

        args = parser.parse_args(
            [
                "--allow-memory-redis",
                "health",
            ]
        )

        self.assertTrue(args.allow_memory_redis)

    def test_parser_accepts_source_override(self) -> None:
        from schema_inspector.cli import _build_parser

        parser = _build_parser()

        args = parser.parse_args(
            [
                "--source",
                "secondary_source",
                "health",
            ]
        )

        self.assertEqual(args.source, "secondary_source")

    def test_parser_accepts_audit_db_on_hydration_commands(self) -> None:
        from schema_inspector.cli import _build_parser

        parser = _build_parser()
        command_variants = (
            ["event", "--sport-slug", "football", "--event-id", "11", "--audit-db"],
            ["live", "--sport-slug", "football", "--audit-db"],
            ["scheduled", "--sport-slug", "football", "--date", "2026-04-17", "--audit-db"],
            ["full-backfill", "--sport-slug", "football", "--audit-db"],
        )

        for argv in command_variants:
            with self.subTest(argv=argv):
                args = parser.parse_args(argv)
                self.assertTrue(args.audit_db)

    def test_parser_accepts_coverage_missing_flags_for_full_backfill(self) -> None:
        from schema_inspector.cli import _build_parser

        parser = _build_parser()

        args = parser.parse_args(
            [
                "full-backfill",
                "--sport-slug",
                "football",
                "--coverage-missing",
                "--coverage-surface",
                "statistics",
                "--coverage-surface",
                "lineups",
            ]
        )

        self.assertTrue(args.coverage_missing)
        self.assertEqual(args.coverage_surface, ["statistics", "lineups"])

    def test_parser_accepts_event_concurrency_after_scheduled_subcommand(self) -> None:
        from schema_inspector.cli import _build_parser

        parser = _build_parser()

        args = parser.parse_args(
            [
                "--log-level",
                "INFO",
                "scheduled",
                "--sport-slug",
                "football",
                "--date",
                "2026-04-17",
                "--event-concurrency",
                "15",
            ]
        )

        self.assertEqual(args.command, "scheduled")
        self.assertEqual(args.sport_slug, "football")
        self.assertEqual(args.date, "2026-04-17")
        self.assertEqual(args.event_concurrency, 15)

    def test_parser_accepts_service_commands(self) -> None:
        from schema_inspector.cli import _build_parser

        parser = _build_parser()

        planner = parser.parse_args(["planner-daemon", "--sport-slug", "football"])
        historical_planner = parser.parse_args(
            [
                "historical-planner-daemon",
                "--date-from",
                "2025-01-01",
                "--date-to",
                "2025-01-31",
            ]
        )
        historical_planner_rolling = parser.parse_args(["historical-planner-daemon"])
        discovery = parser.parse_args(["worker-discovery", "--consumer-name", "discovery-1"])
        historical_discovery = parser.parse_args(
            ["worker-historical-discovery", "--consumer-name", "historical-discovery-1"]
        )
        historical_tournament_planner = parser.parse_args(
            ["historical-tournament-planner-daemon", "--consumer-name", "historical-tournament-planner-1"]
        )
        registry_refresh_planner = parser.parse_args(
            ["tournament-registry-refresh-daemon", "--sport-slug", "esports"]
        )
        live_discovery_planner = parser.parse_args(["live-discovery-planner-daemon", "--sport-slug", "football"])
        historical_tournament = parser.parse_args(
            ["worker-historical-tournament", "--consumer-name", "historical-tournament-1"]
        )
        structure_planner = parser.parse_args(["structure-planner-daemon", "--sport-slug", "football"])
        structure_worker = parser.parse_args(["worker-structure-sync", "--consumer-name", "structure-1"])
        historical_enrichment = parser.parse_args(
            ["worker-historical-enrichment", "--consumer-name", "historical-enrichment-1"]
        )
        live_discovery = parser.parse_args(["worker-live-discovery", "--consumer-name", "live-discovery-1"])
        hydrate = parser.parse_args(["worker-hydrate", "--consumer-name", "hydrate-1"])
        historical_hydrate = parser.parse_args(
            ["worker-historical-hydrate", "--consumer-name", "historical-hydrate-1"]
        )
        live_hot = parser.parse_args(["worker-live-hot", "--consumer-name", "hot-1"])
        live_warm = parser.parse_args(["worker-live-warm", "--consumer-name", "warm-1"])
        maintenance = parser.parse_args(["worker-maintenance", "--consumer-name", "maint-1"])
        historical_maintenance = parser.parse_args(
            ["worker-historical-maintenance", "--consumer-name", "historical-maint-1"]
        )

        self.assertEqual(planner.command, "planner-daemon")
        self.assertEqual(planner.sport_slug, ["football"])
        self.assertEqual(historical_planner.command, "historical-planner-daemon")
        self.assertEqual(historical_planner.date_from, "2025-01-01")
        self.assertEqual(historical_planner.date_to, "2025-01-31")
        self.assertIsNone(historical_planner_rolling.date_from)
        self.assertIsNone(historical_planner_rolling.date_to)
        self.assertEqual(discovery.command, "worker-discovery")
        self.assertEqual(historical_discovery.command, "worker-historical-discovery")
        self.assertEqual(historical_tournament_planner.command, "historical-tournament-planner-daemon")
        self.assertEqual(registry_refresh_planner.command, "tournament-registry-refresh-daemon")
        self.assertEqual(registry_refresh_planner.sport_slug, ["esports"])
        self.assertEqual(live_discovery_planner.command, "live-discovery-planner-daemon")
        self.assertEqual(live_discovery_planner.sport_slug, ["football"])
        self.assertEqual(historical_tournament.command, "worker-historical-tournament")
        self.assertEqual(structure_planner.command, "structure-planner-daemon")
        self.assertEqual(structure_planner.sport_slug, ["football"])
        self.assertEqual(structure_worker.command, "worker-structure-sync")
        self.assertEqual(historical_enrichment.command, "worker-historical-enrichment")
        self.assertEqual(live_discovery.command, "worker-live-discovery")
        self.assertEqual(hydrate.command, "worker-hydrate")
        self.assertEqual(historical_hydrate.command, "worker-historical-hydrate")
        self.assertEqual(live_hot.command, "worker-live-hot")
        self.assertEqual(live_warm.command, "worker-live-warm")
        self.assertEqual(maintenance.command, "worker-maintenance")
        self.assertEqual(historical_maintenance.command, "worker-historical-maintenance")

    def test_service_app_ensures_consumer_groups(self) -> None:
        from schema_inspector.services.service_app import ServiceApp

        fake_stream_queue = _FakeStreamQueue()
        app = type(
            "App",
            (),
            {
                "redis_backend": object(),
                "stream_queue": fake_stream_queue,
                "live_state_store": object(),
            },
        )()

        service_app = ServiceApp(app)
        service_app.ensure_consumer_groups()

        self.assertIn(("stream:etl:discovery", "cg:discovery", "0-0"), fake_stream_queue.groups)
        self.assertIn(("stream:etl:hydrate", "cg:hydrate", "0-0"), fake_stream_queue.groups)
        self.assertIn(("stream:etl:live_discovery", "cg:live_discovery", "0-0"), fake_stream_queue.groups)
        self.assertIn(("stream:etl:live_hot", "cg:live_hot", "0-0"), fake_stream_queue.groups)
        self.assertIn(("stream:etl:live_warm", "cg:live_warm", "0-0"), fake_stream_queue.groups)
        self.assertIn(("stream:etl:maintenance", "cg:maintenance", "0-0"), fake_stream_queue.groups)

    def test_service_app_ensures_historical_consumer_groups(self) -> None:
        from schema_inspector.services.service_app import ServiceApp

        fake_stream_queue = _FakeStreamQueue()
        app = type(
            "App",
            (),
            {
                "redis_backend": object(),
                "stream_queue": fake_stream_queue,
                "live_state_store": object(),
            },
        )()

        service_app = ServiceApp(app)
        service_app.ensure_historical_consumer_groups()

        self.assertIn(("stream:etl:historical_discovery", "cg:historical_discovery", "0-0"), fake_stream_queue.groups)
        self.assertIn(("stream:etl:historical_hydrate", "cg:historical_hydrate", "0-0"), fake_stream_queue.groups)
        self.assertIn(("stream:etl:historical_tournament", "cg:historical_tournament", "0-0"), fake_stream_queue.groups)
        self.assertIn(("stream:etl:historical_enrichment", "cg:historical_enrichment", "0-0"), fake_stream_queue.groups)
        self.assertIn(("stream:etl:historical_maintenance", "cg:historical_maintenance", "0-0"), fake_stream_queue.groups)

    def test_load_redis_backend_fails_without_url_when_memory_fallback_disabled(self) -> None:
        import schema_inspector.cli as hybrid_cli

        with mock.patch.dict(hybrid_cli.os.environ, {}, clear=True):
            with mock.patch.object(hybrid_cli, "_load_project_env", return_value={}):
                with self.assertRaisesRegex(RuntimeError, "Redis is required for production runs"):
                    hybrid_cli._load_redis_backend(None, allow_memory_fallback=False)

    def test_load_redis_backend_fails_without_redis_package_when_memory_fallback_disabled(self) -> None:
        import schema_inspector.cli as hybrid_cli

        with mock.patch.dict(hybrid_cli.os.environ, {"REDIS_URL": "redis://127.0.0.1:6379/0"}, clear=True):
            with mock.patch.dict("sys.modules", {"redis": None}):
                with self.assertRaisesRegex(RuntimeError, "Install python package `redis`"):
                    hybrid_cli._load_redis_backend(None, allow_memory_fallback=False)

    def test_load_redis_backend_uses_memory_only_when_explicitly_allowed(self) -> None:
        import schema_inspector.cli as hybrid_cli

        with mock.patch.dict(hybrid_cli.os.environ, {}, clear=True):
            with mock.patch.object(hybrid_cli, "_load_project_env", return_value={}):
                backend = hybrid_cli._load_redis_backend(None, allow_memory_fallback=True)

        self.assertIsInstance(backend, hybrid_cli._MemoryRedisBackend)

    def test_load_redis_backend_pings_real_redis_backend(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_backend = _FakeRedisBackend()
        fake_redis_module = types.SimpleNamespace(
            Redis=types.SimpleNamespace(from_url=lambda *args, **kwargs: fake_backend),
        )

        with mock.patch.dict(hybrid_cli.os.environ, {"REDIS_URL": "redis://127.0.0.1:6379/0"}, clear=True):
            with mock.patch.dict("sys.modules", {"redis": fake_redis_module}):
                backend = hybrid_cli._load_redis_backend(None, allow_memory_fallback=False)

        self.assertIs(backend, fake_backend)
        self.assertEqual(fake_backend.ping_calls, 1)

    def test_load_redis_backend_reads_url_from_project_env_when_process_env_is_empty(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_backend = _FakeRedisBackend()
        fake_redis_module = types.SimpleNamespace(
            Redis=types.SimpleNamespace(from_url=lambda *args, **kwargs: fake_backend),
        )

        with mock.patch.dict(hybrid_cli.os.environ, {}, clear=True):
            with mock.patch.object(
                hybrid_cli,
                "_load_project_env",
                return_value={"REDIS_URL": "redis://127.0.0.1:6379/0"},
            ):
                with mock.patch.dict("sys.modules", {"redis": fake_redis_module}):
                    backend = hybrid_cli._load_redis_backend(None, allow_memory_fallback=False)

        self.assertIs(backend, fake_backend)
        self.assertEqual(fake_backend.ping_calls, 1)

    async def test_hybrid_app_close_closes_transport(self) -> None:
        from schema_inspector.cli import HybridApp
        from schema_inspector.runtime import RuntimeConfig

        app = HybridApp(database=_FakeDatabase(), runtime_config=RuntimeConfig(require_proxy=False), redis_backend=None)
        transport = _FakeClosableTransport()
        app.transport = transport

        await app.close()

        self.assertTrue(transport.closed)

    async def test_hybrid_app_write_db_audit_coverage_uses_transaction(self) -> None:
        import schema_inspector.cli as hybrid_cli
        from schema_inspector.runtime import RuntimeConfig

        database = _FakeDatabase()
        app = hybrid_cli.HybridApp(database=database, runtime_config=RuntimeConfig(require_proxy=False), redis_backend=None)
        report = object()

        async def _fake_persist_audit_coverage(**kwargs):
            return kwargs

        with mock.patch.object(hybrid_cli, "persist_audit_coverage", side_effect=_fake_persist_audit_coverage) as persist:
            result = await app.write_db_audit_coverage(report=report)

        self.assertEqual(database.transaction_calls, 1)
        self.assertIs(result["sql_executor"], database.connection)
        self.assertEqual(result["source_slug"], "sofascore")
        self.assertIs(result["report"], report)
        persist.assert_called_once()

    def test_hybrid_app_init_does_not_build_source_adapter_eagerly(self) -> None:
        import schema_inspector.cli as hybrid_cli
        from schema_inspector.runtime import RuntimeConfig

        fake_adapter = object()
        runtime_config = RuntimeConfig(require_proxy=False, source_slug="secondary_source")

        with mock.patch.object(hybrid_cli, "build_source_adapter", return_value=fake_adapter) as adapter_factory:
            app = hybrid_cli.HybridApp(database=_FakeDatabase(), runtime_config=runtime_config, redis_backend=None)

        self.assertIsNone(app._source_adapter)
        adapter_factory.assert_not_called()

    async def test_hybrid_app_unsupported_adapter_does_not_fail_init_but_blocks_live_discovery(self) -> None:
        import schema_inspector.cli as hybrid_cli
        from schema_inspector.runtime import RuntimeConfig

        class _UnsupportedAdapter:
            source_slug = "secondary_source"

            def build_event_list_job(self, database):
                del database
                raise RuntimeError("event-list discovery is not wired for source secondary_source")

        unsupported_adapter = _UnsupportedAdapter()

        with mock.patch.object(hybrid_cli, "build_source_adapter", return_value=unsupported_adapter):
            app = hybrid_cli.HybridApp(
                database=_FakeDatabase(),
                runtime_config=RuntimeConfig(require_proxy=False, source_slug="secondary_source"),
                redis_backend=None,
            )

        with self.assertRaisesRegex(RuntimeError, "event-list discovery is not wired for source secondary_source"):
            await app.discover_live_events(sport_slug="football", timeout=20.0)

    async def test_hybrid_app_first_live_discovery_builds_source_adapter_from_runtime_config(self) -> None:
        import schema_inspector.cli as hybrid_cli
        from schema_inspector.runtime import RuntimeConfig

        fake_adapter = _FakeAdapterWithEventListJob()

        with mock.patch.object(hybrid_cli, "build_source_adapter", return_value=fake_adapter) as adapter_factory:
            app = hybrid_cli.HybridApp(
                database=_FakeDatabase(),
                runtime_config=RuntimeConfig(require_proxy=False, source_slug="sofascore"),
                redis_backend=None,
            )

            result = await app.discover_live_events(sport_slug="football", timeout=12.0)

        self.assertEqual(result, {"job": "live"})
        adapter_factory.assert_called_once()
        self.assertEqual(adapter_factory.call_args.args[0], "sofascore")
        self.assertIs(adapter_factory.call_args.kwargs["runtime_config"], app.runtime_config)
        self.assertIs(adapter_factory.call_args.kwargs["transport"], app.transport)

    async def test_hybrid_app_builds_event_list_job_lazily_using_adapter_contract(self) -> None:
        import schema_inspector.cli as hybrid_cli
        from schema_inspector.runtime import RuntimeConfig

        job_instances: list[_FakeLazyEventListJob] = []
        fake_adapter = _FakeAdapterWithEventListJob(job_instances=job_instances)

        with mock.patch.object(hybrid_cli, "build_source_adapter", return_value=fake_adapter):
            app = hybrid_cli.HybridApp(
                database=_FakeDatabase(),
                runtime_config=RuntimeConfig(require_proxy=False),
                redis_backend=None,
            )

            self.assertIsNone(app._event_list_job)
            result = await app.discover_live_events(sport_slug="football", timeout=12.0)

        self.assertEqual(len(job_instances), 1)
        self.assertEqual(job_instances[0].run_live_calls, [("football", 12.0)])
        self.assertEqual(result, {"job": "live"})
        self.assertEqual(fake_adapter.build_event_list_job_calls, 1)

    async def test_hybrid_app_select_event_ids_for_missing_coverage_skips_early_lineups_until_recheck_window(self) -> None:
        from schema_inspector.cli import HybridApp
        from schema_inspector.runtime import RuntimeConfig

        now_seconds = 1_800_000_000
        database = _FakeCoverageSelectionDatabase(
            fetch_rows=[
                {
                    "scope_id": 9001,
                    "surface_name": "lineups",
                    "freshness_status": "possible",
                    "start_timestamp": now_seconds + (2 * 60 * 60),
                },
                {
                    "scope_id": 9002,
                    "surface_name": "lineups",
                    "freshness_status": "possible",
                    "start_timestamp": now_seconds + (40 * 60),
                },
                {
                    "scope_id": 9003,
                    "surface_name": "lineups",
                    "freshness_status": "missing",
                    "start_timestamp": now_seconds + (3 * 60 * 60),
                },
                {
                    "scope_id": 9004,
                    "surface_name": "lineups",
                    "freshness_status": "missing",
                    "start_timestamp": now_seconds + (30 * 60),
                },
            ]
        )
        app = HybridApp(database=database, runtime_config=RuntimeConfig(require_proxy=False), redis_backend=None)

        with mock.patch("schema_inspector.cli.time.time", return_value=float(now_seconds)):
            event_ids = await app.select_event_ids_for_missing_coverage(
                limit=2,
                offset=0,
                sport_slug="football",
                surface_names=("lineups",),
            )

        self.assertEqual(event_ids, (9002, 9004))

    async def test_dispatch_closes_hybrid_app_before_return(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_app = _FakeDispatchHybridApp()
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: object()
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext
            hybrid_cli.HybridApp = lambda **kwargs: fake_app

            exit_code = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="health",
                    timeout=20.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                db_timeout=None,
                redis_url=None,
                allow_memory_redis=True,
                event_concurrency=None,
                log_level="INFO",
            )
            )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app

        self.assertEqual(exit_code, 0)
        self.assertTrue(fake_app.closed)

    async def test_dispatch_health_prints_runtime_gate_fields(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_app = _FakeDispatchHybridApp()
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        stdout = io.StringIO()
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: object()
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext
            hybrid_cli.HybridApp = lambda **kwargs: fake_app

            with redirect_stdout(stdout):
                exit_code = await hybrid_cli._dispatch(
                    argparse.Namespace(
                        command="health",
                        timeout=20.0,
                        proxy=[],
                        user_agent=None,
                        max_attempts=None,
                        database_url=None,
                        db_min_size=None,
                        db_max_size=None,
                        db_timeout=None,
                        redis_url=None,
                        allow_memory_redis=True,
                        event_concurrency=None,
                        log_level="INFO",
                    )
                )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app

        self.assertEqual(exit_code, 0)
        output = stdout.getvalue().strip()
        self.assertIn("health ", output)
        self.assertIn("db_ok=1", output)
        self.assertIn("redis_ok=1", output)
        self.assertIn("redis_backend=fakeredis", output)
        self.assertIn("coverage_tracked=12", output)
        self.assertIn("coverage_stale=3", output)
        self.assertIn("drift_flags=1", output)
        self.assertIn("coverage_alerts=1", output)
        self.assertIn("reconcile_sources=2", output)
        self.assertIn("primary_source=sofascore", output)
        self.assertIn("go_live=0", output)
        self.assertIn("gate_flags=2", output)

    async def test_dispatch_overrides_runtime_config_source_slug_when_source_is_passed(self) -> None:
        import schema_inspector.cli as hybrid_cli
        from schema_inspector.runtime import RuntimeConfig

        fake_app = _FakeDispatchHybridApp()
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        captured_runtime_configs: list[RuntimeConfig] = []
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: RuntimeConfig(require_proxy=False, source_slug="sofascore")
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext

            def _build_app(**kwargs):
                captured_runtime_configs.append(kwargs["runtime_config"])
                return fake_app

            hybrid_cli.HybridApp = _build_app

            exit_code = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="health",
                    timeout=20.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                    source=" Secondary_Source ",
                )
            )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(captured_runtime_configs), 1)
        self.assertEqual(captured_runtime_configs[0].source_slug, "secondary_source")

    async def test_dispatch_keeps_default_runtime_source_when_source_not_passed(self) -> None:
        import schema_inspector.cli as hybrid_cli
        from schema_inspector.runtime import RuntimeConfig

        fake_app = _FakeDispatchHybridApp()
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        captured_runtime_configs: list[RuntimeConfig] = []
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: RuntimeConfig(require_proxy=False, source_slug="sofascore")
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext

            def _build_app(**kwargs):
                captured_runtime_configs.append(kwargs["runtime_config"])
                return fake_app

            hybrid_cli.HybridApp = _build_app

            exit_code = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="health",
                    timeout=20.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                    source=None,
                )
            )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app

        self.assertEqual(exit_code, 0)
        self.assertEqual(len(captured_runtime_configs), 1)
        self.assertEqual(captured_runtime_configs[0].source_slug, "sofascore")

    async def test_dispatch_audit_db_prints_compact_report(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_app = _FakeDispatchHybridApp()
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        stdout = io.StringIO()
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: object()
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext
            hybrid_cli.HybridApp = lambda **kwargs: fake_app

            with redirect_stdout(stdout):
                exit_code = await hybrid_cli._dispatch(
                    argparse.Namespace(
                        command="audit-db",
                        sport_slug="tennis",
                        event_id=[15991729],
                        timeout=20.0,
                        proxy=[],
                        user_agent=None,
                        max_attempts=None,
                        database_url=None,
                        db_min_size=None,
                        db_max_size=None,
                        db_timeout=None,
                        redis_url=None,
                        allow_memory_redis=True,
                        event_concurrency=None,
                        log_level="INFO",
                    )
                )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app

        self.assertEqual(exit_code, 0)
        output = stdout.getvalue().strip()
        self.assertIn("db_audit sport=tennis", output)
        self.assertIn("events=1", output)
        self.assertIn("requests=5", output)
        self.assertIn("snapshots=4", output)
        self.assertIn("tennis_point_by_point=7", output)
        self.assertIn("tennis_power=3", output)

    async def test_dispatch_scheduled_runs_post_hydration_audit_when_requested(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_app = _FakeHydrationDispatchApp()
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        stdout = io.StringIO()
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: object()
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext
            hybrid_cli.HybridApp = lambda **kwargs: fake_app

            with redirect_stdout(stdout):
                exit_code = await hybrid_cli._dispatch(
                    argparse.Namespace(
                        command="scheduled",
                        sport_slug="football",
                        date="2026-04-17",
                        audit_db=True,
                        timeout=20.0,
                        proxy=[],
                        user_agent=None,
                        max_attempts=None,
                        database_url=None,
                        db_min_size=None,
                        db_max_size=None,
                        db_timeout=None,
                        redis_url=None,
                        allow_memory_redis=True,
                        event_concurrency=2,
                        log_level="INFO",
                    )
                )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app

        self.assertEqual(exit_code, 0)
        self.assertEqual(fake_app.audit_calls, [("football", (101, 202))])
        self.assertEqual(len(fake_app.coverage_write_reports), 1)
        self.assertEqual(fake_app.coverage_write_reports[0].sport_slug, "football")
        output = stdout.getvalue().strip()
        self.assertIn("scheduled_hydrate events=2 event_ids=101,202", output)
        self.assertIn("db_audit sport=football", output)

    async def test_dispatch_scheduled_raises_when_post_run_audit_is_empty(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_app = _FakeHydrationDispatchApp(
            audit_report_overrides={
                "raw_snapshots": 0,
                "events": 0,
            }
        )
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: object()
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext
            hybrid_cli.HybridApp = lambda **kwargs: fake_app

            with self.assertRaisesRegex(RuntimeError, "DB audit failed"):
                await hybrid_cli._dispatch(
                    argparse.Namespace(
                        command="scheduled",
                        sport_slug="football",
                        date="2026-04-17",
                        audit_db=True,
                        timeout=20.0,
                        proxy=[],
                        user_agent=None,
                        max_attempts=None,
                        database_url=None,
                        db_min_size=None,
                        db_max_size=None,
                        db_timeout=None,
                        redis_url=None,
                        allow_memory_redis=True,
                        event_concurrency=2,
                        log_level="INFO",
                    )
                )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app

    async def test_dispatch_planner_daemon_runs_service_loop(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_app = _FakeDispatchHybridApp()
        fake_service_app = _FakeServiceApp()
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        original_service_app = hybrid_cli.ServiceApp
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: object()
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext
            hybrid_cli.HybridApp = lambda **kwargs: fake_app
            hybrid_cli.ServiceApp = lambda app: fake_service_app

            exit_code = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="planner-daemon",
                    sport_slug=["football", "tennis"],
                    scheduled_interval_seconds=120.0,
                    loop_interval_seconds=3.0,
                    timeout=20.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                )
            )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app
            hybrid_cli.ServiceApp = original_service_app

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            fake_service_app.calls,
            [("planner", ("football", "tennis"), 120.0, 3.0)],
        )

    async def test_dispatch_worker_hydrate_runs_service_loop(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_app = _FakeDispatchHybridApp()
        fake_service_app = _FakeServiceApp()
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        original_service_app = hybrid_cli.ServiceApp
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: object()
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext
            hybrid_cli.HybridApp = lambda **kwargs: fake_app
            hybrid_cli.ServiceApp = lambda app: fake_service_app

            exit_code = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="worker-hydrate",
                    consumer_name="hydrate-a",
                    block_ms=2500,
                    timeout=20.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                )
            )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app
            hybrid_cli.ServiceApp = original_service_app

        self.assertEqual(exit_code, 0)
        self.assertEqual(fake_service_app.calls, [("hydrate", "hydrate-a", 2500)])

    async def test_dispatch_worker_discovery_runs_service_loop(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_app = _FakeDispatchHybridApp()
        fake_service_app = _FakeServiceApp()
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        original_service_app = hybrid_cli.ServiceApp
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: object()
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext
            hybrid_cli.HybridApp = lambda **kwargs: fake_app
            hybrid_cli.ServiceApp = lambda app: fake_service_app

            exit_code = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="worker-discovery",
                    consumer_name="discovery-a",
                    block_ms=2100,
                    timeout=17.5,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                )
            )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app
            hybrid_cli.ServiceApp = original_service_app

        self.assertEqual(exit_code, 0)
        self.assertEqual(fake_service_app.calls, [("discovery", "discovery-a", 2100, 17.5)])

    async def test_dispatch_historical_service_commands_run_service_loops(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_app = _FakeDispatchHybridApp()
        fake_service_app = _FakeServiceApp()
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_historical_runtime_config = hybrid_cli.load_historical_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        original_service_app = hybrid_cli.ServiceApp
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: object()
            hybrid_cli.load_historical_runtime_config = lambda **kwargs: object()
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext
            hybrid_cli.HybridApp = lambda **kwargs: fake_app
            hybrid_cli.ServiceApp = lambda app: fake_service_app

            planner_exit = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="historical-planner-daemon",
                    sport_slug=["football"],
                    date_from="2025-01-01",
                    date_to="2025-01-31",
                    dates_per_tick=2,
                    loop_interval_seconds=7.0,
                    timeout=20.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                )
            )
            discovery_exit = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="worker-historical-discovery",
                    consumer_name="historical-discovery-a",
                    block_ms=2200,
                    timeout=18.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                )
            )
            hydrate_exit = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="worker-historical-hydrate",
                    consumer_name="historical-hydrate-a",
                    block_ms=2300,
                    timeout=20.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                )
            )
            maintenance_exit = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="worker-historical-maintenance",
                    consumer_name="historical-maint-a",
                    block_ms=2400,
                    timeout=20.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                )
            )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_historical_runtime_config = original_load_historical_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app
            hybrid_cli.ServiceApp = original_service_app

        self.assertEqual(planner_exit, 0)
        self.assertEqual(discovery_exit, 0)
        self.assertEqual(hydrate_exit, 0)
        self.assertEqual(maintenance_exit, 0)
        self.assertEqual(
            fake_service_app.calls,
            [
                ("historical_planner", ("football",), "2025-01-01", "2025-01-31", 2, 7.0),
                ("historical_discovery", "historical-discovery-a", 2200, 18.0),
                ("historical_hydrate", "historical-hydrate-a", 2300),
                ("historical_maintenance", "historical-maint-a", 2400),
            ],
        )

    async def test_dispatch_structure_service_commands_run_service_loops(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_app = _FakeDispatchHybridApp()
        fake_service_app = _FakeServiceApp()
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        original_service_app = hybrid_cli.ServiceApp
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: object()
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext
            hybrid_cli.HybridApp = lambda **kwargs: fake_app
            hybrid_cli.ServiceApp = lambda app: fake_service_app

            planner_exit = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="structure-planner-daemon",
                    sport_slug=["football"],
                    consumer_name="structure-planner-1",
                    loop_interval_seconds=11.0,
                    timeout=20.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                )
            )
            worker_exit = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="worker-structure-sync",
                    consumer_name="structure-worker-a",
                    block_ms=2600,
                    timeout=20.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                )
            )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app
            hybrid_cli.ServiceApp = original_service_app

        self.assertEqual(planner_exit, 0)
        self.assertEqual(worker_exit, 0)
        self.assertEqual(
            fake_service_app.calls,
            [
                ("structure_planner", ("football",), 11.0),
                ("structure_worker", "structure-worker-a", 2600),
            ],
        )

    async def test_dispatch_registry_refresh_service_command_runs_service_loop(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_app = _FakeDispatchHybridApp()
        fake_service_app = _FakeServiceApp()
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        original_service_app = hybrid_cli.ServiceApp
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: object()
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext
            hybrid_cli.HybridApp = lambda **kwargs: fake_app
            hybrid_cli.ServiceApp = lambda app: fake_service_app

            exit_code = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="tournament-registry-refresh-daemon",
                    sport_slug=["esports"],
                    refresh_interval_seconds=86400.0,
                    loop_interval_seconds=300.0,
                    sports_per_tick=1,
                    timeout=19.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                )
            )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app
            hybrid_cli.ServiceApp = original_service_app

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            fake_service_app.calls,
            [("registry_refresh", ("esports",), 86400.0, 300.0, 1, 19.0)],
        )

    async def test_dispatch_worker_live_and_maintenance_run_service_loops(self) -> None:
        import schema_inspector.cli as hybrid_cli

        fake_app = _FakeDispatchHybridApp()
        fake_service_app = _FakeServiceApp()
        original_load_runtime_config = hybrid_cli.load_runtime_config
        original_load_database_config = hybrid_cli.load_database_config
        original_database_class = hybrid_cli.AsyncpgDatabase
        original_hybrid_app = hybrid_cli.HybridApp
        original_service_app = hybrid_cli.ServiceApp
        try:
            hybrid_cli.load_runtime_config = lambda **kwargs: object()
            hybrid_cli.load_database_config = lambda **kwargs: object()
            hybrid_cli.AsyncpgDatabase = _FakeAsyncpgDatabaseContext
            hybrid_cli.HybridApp = lambda **kwargs: fake_app
            hybrid_cli.ServiceApp = lambda app: fake_service_app

            live_hot_exit = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="worker-live-hot",
                    consumer_name="live-hot-a",
                    block_ms=1500,
                    timeout=20.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                )
            )
            live_warm_exit = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="worker-live-warm",
                    consumer_name="live-warm-a",
                    block_ms=1700,
                    timeout=20.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                )
            )
            maintenance_exit = await hybrid_cli._dispatch(
                argparse.Namespace(
                    command="worker-maintenance",
                    consumer_name="maint-a",
                    block_ms=1900,
                    timeout=20.0,
                    proxy=[],
                    user_agent=None,
                    max_attempts=None,
                    database_url=None,
                    db_min_size=None,
                    db_max_size=None,
                    db_timeout=None,
                    redis_url=None,
                    allow_memory_redis=True,
                    event_concurrency=None,
                    log_level="INFO",
                )
            )
        finally:
            hybrid_cli.load_runtime_config = original_load_runtime_config
            hybrid_cli.load_database_config = original_load_database_config
            hybrid_cli.AsyncpgDatabase = original_database_class
            hybrid_cli.HybridApp = original_hybrid_app
            hybrid_cli.ServiceApp = original_service_app

        self.assertEqual(live_hot_exit, 0)
        self.assertEqual(live_warm_exit, 0)
        self.assertEqual(maintenance_exit, 0)
        self.assertEqual(
            fake_service_app.calls,
            [
                ("live", "hot", "live-hot-a", 1500),
                ("live", "warm", "live-warm-a", 1700),
                ("maintenance", "maint-a", 1900),
            ],
        )

    async def test_hybrid_app_run_event_uses_prefetch_commit_and_persist_phases(self) -> None:
        from schema_inspector.cli import HybridApp
        from schema_inspector.runtime import RuntimeConfig

        database = _FakeDatabase()
        app = HybridApp(database=database, runtime_config=RuntimeConfig(require_proxy=False), redis_backend=None)
        app._seeded_endpoint_registry_sports.add("football")
        phase_calls = []
        prefetched_run = object()
        committed_run = object()

        async def _fake_prefetch(*, event_id: int, sport_slug: str, hydration_mode: str):
            phase_calls.append(("prefetch", event_id, sport_slug, hydration_mode))
            return prefetched_run

        async def _fake_commit(run):
            phase_calls.append(("commit", run))
            return committed_run

        async def _fake_persist(run, *, hydration_mode: str):
            phase_calls.append(("persist", run, hydration_mode))
            return {"event_id": 11, "sport_slug": "football", "hydration_mode": hydration_mode}

        with mock.patch.object(app, "_prefetch_event_run", side_effect=_fake_prefetch):
            with mock.patch.object(app, "_commit_prefetched_run", side_effect=_fake_commit):
                with mock.patch.object(app, "_persist_prefetched_run", side_effect=_fake_persist):
                    with mock.patch.object(app, "_warn_if_prefetched_run_large") as warn_mock:
                        result = await app.run_event(event_id=11, sport_slug="football", hydration_mode="core")

        self.assertEqual(result["hydration_mode"], "core")
        self.assertEqual(
            phase_calls,
            [
                ("prefetch", 11, "football", "core"),
                ("commit", prefetched_run),
                ("persist", committed_run, "core"),
            ],
        )
        warn_mock.assert_called_once_with(prefetched_run)

    async def test_hybrid_app_warns_when_prefetched_run_exceeds_limit(self) -> None:
        from schema_inspector.cli import HybridApp, HybridSnapshotStore, PrefetchedRun
        from schema_inspector.fetch_executor import PrefetchedFetchRecord
        from schema_inspector.fetch_models import FetchOutcomeEnvelope, FetchTask
        from schema_inspector.runtime import RuntimeConfig
        from schema_inspector.storage.raw_repository import ApiRequestLogRecord, PayloadSnapshotRecord

        database = _FakeDatabase()
        app = HybridApp(database=database, runtime_config=RuntimeConfig(require_proxy=False), redis_backend=None)
        snapshot_store = HybridSnapshotStore(app.raw_repository, None)
        prefetched = PrefetchedRun(
            event_id=99,
            sport_slug="football",
            fetch_records=(
                PrefetchedFetchRecord(
                    task=FetchTask(
                        trace_id="trace-99",
                        job_id="job-99",
                        sport_slug="football",
                        endpoint_pattern="/api/v1/event/{event_id}",
                        source_url="https://www.sofascore.com/api/v1/event/99",
                        timeout_profile="pilot",
                        context_entity_type="event",
                        context_entity_id=99,
                        context_event_id=99,
                        fetch_reason="hydrate_event_root",
                    ),
                    outcome=FetchOutcomeEnvelope(
                        trace_id="trace-99",
                        job_id="job-99",
                        endpoint_pattern="/api/v1/event/{event_id}",
                        source_url="https://www.sofascore.com/api/v1/event/99",
                        resolved_url="https://www.sofascore.com/api/v1/event/99",
                        http_status=200,
                        classification="success_json",
                        proxy_id="proxy",
                        challenge_reason=None,
                        snapshot_id=-1,
                        payload_hash="hash-99",
                        fetched_at="2026-04-19T12:00:00+00:00",
                    ),
                    request_log=ApiRequestLogRecord(
                        trace_id="trace-99",
                        job_id="job-99",
                        job_type="hydrate_event_root",
                        sport_slug="football",
                        method="GET",
                        source_url="https://www.sofascore.com/api/v1/event/99",
                        endpoint_pattern="/api/v1/event/{event_id}",
                        request_headers_redacted=None,
                        query_params=None,
                        proxy_id="proxy",
                        transport_attempt=1,
                        http_status=200,
                        challenge_reason=None,
                        started_at="2026-04-19T12:00:00+00:00",
                        finished_at="2026-04-19T12:00:01+00:00",
                        latency_ms=100,
                    ),
                    payload_snapshot=PayloadSnapshotRecord(
                        trace_id="trace-99",
                        job_id="job-99",
                        sport_slug="football",
                        endpoint_pattern="/api/v1/event/{event_id}",
                        source_url="https://www.sofascore.com/api/v1/event/99",
                        resolved_url="https://www.sofascore.com/api/v1/event/99",
                        envelope_key="event",
                        context_entity_type="event",
                        context_entity_id=99,
                        context_unique_tournament_id=None,
                        context_season_id=None,
                        context_event_id=99,
                        http_status=200,
                        payload={"event": {"id": 99}},
                        payload_hash="hash-99",
                        payload_size_bytes=2 * 1024 * 1024,
                        content_type="application/json",
                        is_valid_json=True,
                        is_soft_error_payload=False,
                        fetched_at="2026-04-19T12:00:01+00:00",
                    ),
                    snapshot_head=None,
                ),
            ),
            snapshot_store=snapshot_store,
            initial_capability_rollup={},
        )

        with mock.patch.dict("os.environ", {"BP_PREFETCH_MEMORY_WARN_MB": "1"}, clear=False):
            with self.assertLogs("schema_inspector.cli", level="WARNING") as captured:
                app._warn_if_prefetched_run_large(prefetched)

        self.assertIn("event_id=99", captured.output[0])
        self.assertIn("endpoint_count=1", captured.output[0])

    async def test_run_event_command_limits_batch_concurrency(self) -> None:
        from schema_inspector.cli import run_event_command

        orchestrator = _SlowFakeOrchestrator()
        args = argparse.Namespace(
            sport_slug="football",
            event_id=[11, 12, 13, 14],
            hydration_mode="core",
            event_concurrency=2,
        )

        report = await run_event_command(args, orchestrator=orchestrator)

        self.assertEqual(report.processed_event_ids, (11, 12, 13, 14))
        self.assertEqual(orchestrator.max_active, 2)
        self.assertEqual(
            orchestrator.calls,
            [
                ("football", 11, "core"),
                ("football", 12, "core"),
                ("football", 13, "core"),
                ("football", 14, "core"),
            ],
        )

    async def test_hybrid_app_bootstraps_endpoint_registry_once_per_sport(self) -> None:
        from schema_inspector.cli import HybridApp
        from schema_inspector.runtime import RuntimeConfig

        database = _FakeDatabase()
        app = HybridApp(database=database, runtime_config=RuntimeConfig(require_proxy=False), redis_backend=None)
        repository = _FakeRawRepository()
        app.raw_repository = repository

        await app.ensure_endpoint_registry("football")
        await app.ensure_endpoint_registry("football")

        self.assertEqual(database.transaction_calls, 1)
        self.assertEqual(len(repository.calls), 1)
        patterns = set(repository.calls[0])
        self.assertIn("/api/v1/event/{event_id}/statistics", patterns)
        self.assertIn("/api/v1/event/{event_id}/lineups", patterns)
        self.assertIn("/api/v1/event/{event_id}/incidents", patterns)
        self.assertIn("/api/v1/team/{team_id}", patterns)
        self.assertIn("/api/v1/player/{player_id}", patterns)

    async def test_run_event_command_hydrates_each_event(self) -> None:
        from schema_inspector.cli import run_event_command

        orchestrator = _FakeOrchestrator()
        args = argparse.Namespace(sport_slug="football", event_id=[11, 12])

        report = await run_event_command(args, orchestrator=orchestrator)

        self.assertEqual(orchestrator.calls, [("football", 11, "full"), ("football", 12, "full")])
        self.assertEqual(report.processed_event_ids, (11, 12))

    async def test_run_full_backfill_uses_selector_and_orchestrator(self) -> None:
        from schema_inspector.cli import run_full_backfill_command

        orchestrator = _FakeOrchestrator()
        selector = _FakeEventSelector((20, 21, 22))
        args = argparse.Namespace(limit=3, offset=0, event_id=[], sport_slug=None, coverage_missing=False, coverage_surface=[])

        report = await run_full_backfill_command(
            args,
            orchestrator=orchestrator,
            event_selector=selector,
        )

        self.assertEqual(selector.calls, [(3, 0, None)])
        self.assertEqual(report.processed_event_ids, (20, 21, 22))
        self.assertEqual(
            orchestrator.calls,
            [(None, 20, "full"), (None, 21, "full"), (None, 22, "full")],
        )

    async def test_run_full_backfill_uses_coverage_selector_when_requested(self) -> None:
        from schema_inspector.cli import run_full_backfill_command

        orchestrator = _FakeOrchestrator()
        selector = _FakeEventSelector((30, 31))
        args = argparse.Namespace(
            limit=5,
            offset=2,
            event_id=[],
            sport_slug="football",
            coverage_missing=True,
            coverage_surface=["statistics", "lineups"],
        )

        report = await run_full_backfill_command(
            args,
            orchestrator=orchestrator,
            event_selector=selector,
        )

        self.assertEqual(selector.calls, [])
        self.assertEqual(selector.coverage_calls, [(5, 2, "football", ("statistics", "lineups"))])
        self.assertEqual(report.processed_event_ids, (30, 31))
        self.assertEqual(
            orchestrator.calls,
            [("football", 30, "full"), ("football", 31, "full")],
        )

    async def test_run_full_backfill_uses_default_coverage_surfaces_when_not_passed(self) -> None:
        from schema_inspector.cli import run_full_backfill_command

        orchestrator = _FakeOrchestrator()
        selector = _FakeEventSelector((41,))
        args = argparse.Namespace(
            limit=1,
            offset=0,
            event_id=[],
            sport_slug=None,
            coverage_missing=True,
            coverage_surface=[],
        )

        report = await run_full_backfill_command(
            args,
            orchestrator=orchestrator,
            event_selector=selector,
        )

        self.assertEqual(selector.calls, [])
        self.assertEqual(
            selector.coverage_calls,
            [(1, 0, None, ("event_core", "statistics", "incidents", "lineups"))],
        )
        self.assertEqual(report.processed_event_ids, (41,))


class _FakeOrchestrator:
    def __init__(self) -> None:
        self.calls = []

    async def run_event(self, *, event_id: int, sport_slug: str | None, hydration_mode: str = "full"):
        self.calls.append((sport_slug, event_id, hydration_mode))
        return {"event_id": event_id, "sport_slug": sport_slug, "hydration_mode": hydration_mode}


class _SlowFakeOrchestrator:
    def __init__(self) -> None:
        self.calls = []
        self.active = 0
        self.max_active = 0

    async def run_event(self, *, event_id: int, sport_slug: str | None, hydration_mode: str = "full"):
        self.calls.append((sport_slug, event_id, hydration_mode))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0.01)
        self.active -= 1
        return {"event_id": event_id, "sport_slug": sport_slug, "hydration_mode": hydration_mode}


class _FakeEventSelector:
    def __init__(self, event_ids) -> None:
        self.event_ids = tuple(event_ids)
        self.calls = []
        self.coverage_calls = []

    async def select_event_ids(self, *, limit: int | None, offset: int, sport_slug: str | None):
        self.calls.append((limit, offset, sport_slug))
        return self.event_ids

    async def select_event_ids_for_missing_coverage(
        self,
        *,
        limit: int | None,
        offset: int,
        sport_slug: str | None,
        surface_names: tuple[str, ...],
    ):
        self.coverage_calls.append((limit, offset, sport_slug, surface_names))
        return self.event_ids


class _FakeDatabase:
    def __init__(self) -> None:
        self.transaction_calls = 0
        self.connection = object()

    def transaction(self):
        self.transaction_calls += 1
        return _FakeTransaction(self.connection)


class _FakeCoverageSelectionDatabase:
    def __init__(self, *, fetch_rows: list[dict[str, object]]) -> None:
        self._connection = _FakeCoverageSelectionConnection(fetch_rows)

    def connection(self):
        return _FakeTransaction(self._connection)


class _FakeCoverageSelectionConnection:
    def __init__(self, fetch_rows: list[dict[str, object]]) -> None:
        self.fetch_rows = list(fetch_rows)
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, query: str, *args: object):
        self.fetch_calls.append((query, args))
        return list(self.fetch_rows)


class _FakeTransaction:
    def __init__(self, connection) -> None:
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeRawRepository:
    def __init__(self) -> None:
        self.calls: list[tuple[str, ...]] = []

    async def upsert_endpoint_registry_entries(self, executor, entries) -> None:
        del executor
        self.calls.append(tuple(item.pattern for item in entries))


class _FakePilotOrchestrator:
    def __init__(self) -> None:
        self.calls = []

    async def run_event(self, *, event_id: int, sport_slug: str, hydration_mode: str = "full"):
        self.calls.append((event_id, sport_slug, hydration_mode))
        return {"event_id": event_id, "sport_slug": sport_slug, "hydration_mode": hydration_mode}


class _FakeClosableTransport:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _FakeLazyEventListJob:
    def __init__(self, *, parser, repository, database) -> None:
        self.parser = parser
        self.repository = repository
        self.database = database
        self.run_live_calls: list[tuple[str, float]] = []

    async def run_live(self, *, sport_slug: str, timeout: float):
        self.run_live_calls.append((sport_slug, timeout))
        return {"job": "live"}


class _FakeAdapterWithEventListJob:
    def __init__(self, *, job_instances: list[_FakeLazyEventListJob] | None = None) -> None:
        self.source_slug = "sofascore"
        self.job_instances = job_instances if job_instances is not None else []
        self.build_event_list_job_calls = 0

    def build_event_list_job(self, database):
        self.build_event_list_job_calls += 1
        job = _FakeLazyEventListJob(parser=None, repository=None, database=database)
        self.job_instances.append(job)
        return job


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.ping_calls = 0

    def ping(self) -> bool:
        self.ping_calls += 1
        return True


class _FakeRedis:
    def __init__(self) -> None:
        self.ping_calls = 0

    def ping(self) -> bool:
        self.ping_calls += 1
        return True


class _FakeLaneRedisBackend:
    def __init__(self, members_by_key: dict[str, tuple[str, ...]]) -> None:
        self.members_by_key = members_by_key

    def zrangebyscore(self, key: str, _start: float, _end: float):
        return self.members_by_key.get(key, ())


class _FakeLiveStateStore:
    def __init__(self, backend) -> None:
        self.backend = backend

    def _lane_key(self, lane: str) -> str:
        return f"live:{lane}"


class _FakeSqlExecutor:
    def __init__(self, values_by_query: dict[str, int], rows_by_query: dict[str, list[dict[str, object]]] | None = None) -> None:
        self.values_by_query = values_by_query
        self.rows_by_query = dict(rows_by_query or {})

    async def fetchval(self, query: str):
        return self.values_by_query[query]

    async def fetch(self, query: str):
        normalized = " ".join(query.split())
        if "FROM event_terminal_state AS ets" in normalized:
            return list(self.rows_by_query.get("health_drift", ()))
        if "FROM coverage_ledger" in normalized:
            return list(self.rows_by_query.get("health_coverage", ()))
        return []


class _FakeDispatchHybridApp:
    def __init__(self) -> None:
        self.closed = False
        self.redis_backend = _FakeRedisBackend()

    async def collect_health(self):
        return type(
            "HealthReport",
            (),
            {
                "snapshot_count": 1,
                "capability_rollup_count": 2,
                "live_hot_count": 3,
                "live_warm_count": 4,
                "live_cold_count": 5,
                "database_ok": True,
                "redis_ok": True,
                "redis_backend_kind": "fakeredis",
                "drift_summary": type("DriftSummary", (), {"flag_count": 1})(),
                "coverage_summary": type(
                    "CoverageSummary",
                    (),
                    {"tracked_scope_count": 12, "stale_scope_count": 3},
                )(),
                "reconcile_policy_summary": type(
                    "ReconcilePolicySummary",
                    (),
                    {
                        "policy_enabled": True,
                        "primary_source_slug": "sofascore",
                        "source_count": 2,
                    },
                )(),
                "go_live": type(
                    "GoLiveSummary",
                    (),
                    {
                        "ready": False,
                        "flag_count": 2,
                        "snapshot_age_seconds": 420,
                        "historical_enrichment_lag": 2400,
                        "historical_retry_share": 0.024,
                        "housekeeping_dry_run": True,
                    },
                )(),
            },
        )()

    async def close(self) -> None:
        self.closed = True

    async def collect_db_audit(self, *, sport_slug: str, event_ids: tuple[int, ...]):
        del event_ids
        return type(
            "DatabaseAuditReport",
            (),
            {
                "sport_slug": sport_slug,
                "event_count": 1,
                "raw_requests": 5,
                "raw_snapshots": 4,
                "events": 1,
                "statistics": 9,
                "incidents": 2,
                "lineup_sides": 0,
                "lineup_players": 0,
                "special_counts": {"tennis_point_by_point": 7, "tennis_power": 3},
            },
        )()


class _FakeHydrationDispatchApp:
    def __init__(self, *, audit_report_overrides: dict[str, object] | None = None) -> None:
        self.closed = False
        self.redis_backend = _FakeRedisBackend()
        self.audit_calls: list[tuple[str, tuple[int, ...]]] = []
        self.coverage_write_reports: list[object] = []
        self.run_event_calls: list[tuple[int, str | None, str]] = []
        self.audit_report_overrides = dict(audit_report_overrides or {})

    async def discover_scheduled_event_ids(self, *, sport_slug: str, date: str, timeout: float):
        del sport_slug, date, timeout
        return (101, 202)

    async def run_event(self, *, event_id: int, sport_slug: str | None, hydration_mode: str = "full"):
        self.run_event_calls.append((event_id, sport_slug, hydration_mode))
        return {"event_id": event_id}

    async def collect_db_audit(self, *, sport_slug: str, event_ids: tuple[int, ...]):
        self.audit_calls.append((sport_slug, event_ids))
        payload = {
            "sport_slug": sport_slug,
            "event_count": len(event_ids),
            "raw_requests": 6,
            "raw_snapshots": 4,
            "events": len(event_ids),
            "statistics": 8,
            "incidents": 3,
            "lineup_sides": 2,
            "lineup_players": 22,
            "special_counts": {},
        }
        payload.update(self.audit_report_overrides)
        return type("DatabaseAuditReport", (), payload)()

    async def write_db_audit_coverage(self, *, report) -> None:
        self.coverage_write_reports.append(report)

    async def close(self) -> None:
        self.closed = True


class _FakeStreamQueue:
    def __init__(self) -> None:
        self.groups: list[tuple[str, str, str]] = []

    def ensure_group(self, stream: str, group: str, *, start_id: str = "0-0") -> None:
        self.groups.append((stream, group, start_id))


class _FakeServiceApp:
    def __init__(self) -> None:
        self.calls = []

    async def run_planner_daemon(self, *, sport_slugs: tuple[str, ...], scheduled_interval_seconds: float, loop_interval_seconds: float) -> None:
        self.calls.append(("planner", sport_slugs, scheduled_interval_seconds, loop_interval_seconds))

    async def run_discovery_worker(self, *, consumer_name: str, block_ms: int, timeout_s: float) -> None:
        self.calls.append(("discovery", consumer_name, block_ms, timeout_s))

    async def run_hydrate_worker(self, *, consumer_name: str, block_ms: int) -> None:
        self.calls.append(("hydrate", consumer_name, block_ms))

    async def run_live_worker(self, *, lane: str, consumer_name: str, block_ms: int) -> None:
        self.calls.append(("live", lane, consumer_name, block_ms))

    async def run_maintenance_worker(self, *, consumer_name: str, block_ms: int) -> None:
        self.calls.append(("maintenance", consumer_name, block_ms))

    async def run_historical_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...],
        date_from: str | None,
        date_to: str | None,
        dates_per_tick: int,
        loop_interval_seconds: float,
    ) -> None:
        self.calls.append(
            ("historical_planner", sport_slugs, date_from, date_to, dates_per_tick, loop_interval_seconds)
        )

    async def run_historical_discovery_worker(self, *, consumer_name: str, block_ms: int, timeout_s: float) -> None:
        self.calls.append(("historical_discovery", consumer_name, block_ms, timeout_s))

    async def run_historical_hydrate_worker(self, *, consumer_name: str, block_ms: int) -> None:
        self.calls.append(("historical_hydrate", consumer_name, block_ms))

    async def run_historical_maintenance_worker(self, *, consumer_name: str, block_ms: int) -> None:
        self.calls.append(("historical_maintenance", consumer_name, block_ms))

    async def run_structure_planner_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...],
        loop_interval_seconds: float,
        targets=None,
    ) -> None:
        del targets
        self.calls.append(("structure_planner", sport_slugs, loop_interval_seconds))

    async def run_structure_worker(self, *, consumer_name: str, block_ms: int) -> None:
        self.calls.append(("structure_worker", consumer_name, block_ms))

    async def run_tournament_registry_refresh_daemon(
        self,
        *,
        sport_slugs: tuple[str, ...],
        refresh_interval_seconds: float,
        loop_interval_seconds: float,
        sports_per_tick: int,
        timeout_s: float,
    ) -> None:
        self.calls.append(
            ("registry_refresh", sport_slugs, refresh_interval_seconds, loop_interval_seconds, sports_per_tick, timeout_s)
        )


class _FakeAsyncpgDatabaseContext:
    def __init__(self, config) -> None:
        self.config = config

    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


if __name__ == "__main__":
    unittest.main()
