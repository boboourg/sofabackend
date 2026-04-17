from __future__ import annotations

import argparse
import unittest


class HybridCliTests(unittest.IsolatedAsyncioTestCase):
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

        self.assertEqual(orchestrator.calls, [("football", 11), ("football", 12)])
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
        self.assertEqual(orchestrator.calls, [(None, 20), (None, 21), (None, 22)])


class _FakeOrchestrator:
    def __init__(self) -> None:
        self.calls = []

    async def run_event(self, *, event_id: int, sport_slug: str | None):
        self.calls.append((sport_slug, event_id))
        return {"event_id": event_id, "sport_slug": sport_slug}


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


if __name__ == "__main__":
    unittest.main()
