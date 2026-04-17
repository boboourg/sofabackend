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
        discovery = parser.parse_args(["worker-discovery", "--consumer-name", "discovery-1"])
        historical_discovery = parser.parse_args(
            ["worker-historical-discovery", "--consumer-name", "historical-discovery-1"]
        )
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
        self.assertEqual(discovery.command, "worker-discovery")
        self.assertEqual(historical_discovery.command, "worker-historical-discovery")
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

    async def test_hybrid_app_run_event_passes_hydration_mode_to_pilot_orchestrator(self) -> None:
        from schema_inspector.cli import HybridApp
        from schema_inspector.runtime import RuntimeConfig

        database = _FakeDatabase()
        app = HybridApp(database=database, runtime_config=RuntimeConfig(require_proxy=False), redis_backend=None)
        repository = _FakeRawRepository()
        app.raw_repository = repository
        app.transport = object()
        app.capability_repository = object()
        app.live_state_repository = object()
        app.normalize_repository = object()

        original_orchestrator = getattr(__import__("schema_inspector.cli", fromlist=["PilotOrchestrator"]), "PilotOrchestrator")
        fake_orchestrator = _FakePilotOrchestrator()
        try:
            import schema_inspector.cli as hybrid_cli

            hybrid_cli.PilotOrchestrator = lambda **kwargs: fake_orchestrator
            result = await app.run_event(event_id=11, sport_slug="football", hydration_mode="core")
        finally:
            hybrid_cli.PilotOrchestrator = original_orchestrator

        self.assertEqual(result["hydration_mode"], "core")
        self.assertEqual(fake_orchestrator.calls, [(11, "football", "core")])

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
        args = argparse.Namespace(limit=3, offset=0, event_id=[], sport_slug=None)

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

    async def select_event_ids(self, *, limit: int | None, offset: int, sport_slug: str | None):
        self.calls.append((limit, offset, sport_slug))
        return self.event_ids


class _FakeDatabase:
    def __init__(self) -> None:
        self.transaction_calls = 0
        self.connection = object()

    def transaction(self):
        self.transaction_calls += 1
        return _FakeTransaction(self.connection)


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
    def __init__(self, values_by_query: dict[str, int]) -> None:
        self.values_by_query = values_by_query

    async def fetchval(self, query: str):
        return self.values_by_query[query]


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
        date_from: str,
        date_to: str,
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


class _FakeAsyncpgDatabaseContext:
    def __init__(self, config) -> None:
        self.config = config

    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


if __name__ == "__main__":
    unittest.main()
