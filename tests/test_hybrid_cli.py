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

    def test_load_redis_backend_fails_without_url_when_memory_fallback_disabled(self) -> None:
        import schema_inspector.cli as hybrid_cli

        with mock.patch.dict(hybrid_cli.os.environ, {}, clear=True):
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


class _FakeAsyncpgDatabaseContext:
    def __init__(self, config) -> None:
        self.config = config

    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


if __name__ == "__main__":
    unittest.main()
