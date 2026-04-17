from __future__ import annotations

import argparse
import asyncio
import unittest


class HybridCliTests(unittest.IsolatedAsyncioTestCase):
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


class _FakeDispatchHybridApp:
    def __init__(self) -> None:
        self.closed = False

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
