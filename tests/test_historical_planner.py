from __future__ import annotations

import json
from datetime import date, timedelta
from types import SimpleNamespace
import unittest
from unittest import mock


class HistoricalHorizonTests(unittest.TestCase):
    def test_compute_historical_horizon_uses_default_windows(self) -> None:
        from schema_inspector.services.historical_planner import compute_historical_horizon

        today = date(2026, 4, 22)
        horizon = compute_historical_horizon(today=today, sport_slug="football")

        expected_recent_from = today - timedelta(days=730)
        self.assertEqual(horizon.deep_from, date(1996, 1, 1))
        self.assertEqual(horizon.deep_to, expected_recent_from - timedelta(days=1))
        self.assertEqual(horizon.recent_from, expected_recent_from)
        self.assertEqual(horizon.recent_to, today)

    def test_build_historical_targets_skips_recent_when_override_window_is_closed(self) -> None:
        from schema_inspector.services.historical_planner import (
            build_historical_planning_targets,
            compute_historical_horizon,
        )

        horizon = compute_historical_horizon(
            today=date(2026, 4, 22),
            sport_slug="tennis",
            start_override=date(2010, 1, 1),
            end_override=date(2010, 12, 31),
            recent_refresh_days=30,
        )

        targets = build_historical_planning_targets(
            source_slug="sofascore",
            sport_slug="tennis",
            horizon=horizon,
        )

        self.assertEqual(len(targets), 1)
        self.assertEqual(targets[0].scope, "historical_deep")
        self.assertEqual(targets[0].date_from, "2010-01-01")
        self.assertEqual(targets[0].date_to, "2010-12-31")


class HistoricalPlannerTests(unittest.IsolatedAsyncioTestCase):
    async def test_historical_planner_publishes_dates_progressively_and_persists_cursor(self) -> None:
        from schema_inspector.services.historical_planner import (
            HistoricalCursorStore,
            HistoricalPlannerDaemon,
            HistoricalPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()
        cursor_store = HistoricalCursorStore(backend)
        daemon = HistoricalPlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            targets=(
                HistoricalPlanningTarget(
                    sport_slug="football",
                    date_from="2025-01-01",
                    date_to="2025-01-02",
                ),
            ),
            dates_per_tick=1,
        )

        first_published = await daemon.tick()
        second_published = await daemon.tick()
        third_published = await daemon.tick()

        self.assertEqual(first_published, 1)
        self.assertEqual(second_published, 1)
        self.assertEqual(third_published, 0)
        self.assertEqual([stream for stream, _ in queue.published], ["stream:etl:historical_discovery"] * 2)
        self.assertEqual(
            [json.loads(payload["params_json"])["date"] for _, payload in queue.published],
            ["2025-01-01", "2025-01-02"],
        )
        self.assertEqual(cursor_store.load_next_date("football", "2025-01-01", "2025-01-02"), "2025-01-03")

    async def test_historical_planner_pauses_when_backpressure_is_above_threshold(self) -> None:
        from schema_inspector.services.backpressure import BackpressureLimit, QueueBackpressure
        from schema_inspector.services.historical_planner import (
            HistoricalCursorStore,
            HistoricalPlannerDaemon,
            HistoricalPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue(group_info_by_stream={("stream:etl:historical_hydrate", "cg:historical_hydrate"): _FakeGroupInfo(lag=500)})
        cursor_store = HistoricalCursorStore(backend)
        daemon = HistoricalPlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            targets=(
                HistoricalPlanningTarget(
                    sport_slug="football",
                    date_from="2025-01-01",
                    date_to="2025-01-02",
                ),
            ),
            backpressure=QueueBackpressure(
                queue=queue,
                limits=(BackpressureLimit(stream="stream:etl:historical_hydrate", group="cg:historical_hydrate", max_lag=100),),
            ),
        )

        published = await daemon.tick()

        self.assertEqual(published, 0)
        self.assertEqual(queue.published, [])
        self.assertIsNone(cursor_store.load_next_date("football", "2025-01-01", "2025-01-02"))

    async def test_historical_planner_cycles_recent_refresh_window(self) -> None:
        from schema_inspector.services.historical_planner import (
            HistoricalCursorStore,
            HistoricalPlannerDaemon,
            HistoricalPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()
        cursor_store = HistoricalCursorStore(backend)
        daemon = HistoricalPlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            targets=(
                HistoricalPlanningTarget(
                    sport_slug="football",
                    scope="historical_recent_refresh",
                    date_from="2025-01-01",
                    date_to="2025-01-02",
                    repeat_from_start=True,
                ),
            ),
            dates_per_tick=1,
        )

        first_published = await daemon.tick()
        second_published = await daemon.tick()
        third_published = await daemon.tick()

        self.assertEqual((first_published, second_published, third_published), (1, 1, 1))
        self.assertEqual(
            [json.loads(payload["params_json"])["date"] for _, payload in queue.published],
            ["2025-01-01", "2025-01-02", "2025-01-01"],
        )
        self.assertEqual(cursor_store.load_next_date("football", "2025-01-01", "2025-01-02"), "2025-01-02")


class HistoricalPlannerPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_postgres_cursor_store_loads_and_saves_cursor_by_scope(self) -> None:
        from schema_inspector.services.historical_planner import (
            HistoricalPlanningTarget,
            PostgresHistoricalCursorStore,
        )
        from schema_inspector.storage.planner_cursor_repository import PlannerCursorRecord

        repository = mock.Mock()
        repository.load_cursor = mock.AsyncMock(
            return_value=PlannerCursorRecord(
                planner_name="historical_planner",
                source_slug="sofascore",
                sport_slug="football",
                scope_type="historical_deep",
                scope_id=None,
                cursor_date=date(2025, 1, 3),
                cursor_id=None,
            )
        )
        repository.upsert_cursor = mock.AsyncMock()
        store = PostgresHistoricalCursorStore(
            repository=repository,
            connection_factory=_FakeDatabaseConnectionContext,
            planner_name="historical_planner",
            default_source_slug="sofascore",
        )
        target = HistoricalPlanningTarget(
            sport_slug="football",
            scope="historical_deep",
            date_from="2025-01-01",
            date_to="2025-12-31",
        )

        loaded = await store.load_next_date(target)
        await store.save_next_date(target, "2025-01-04")

        self.assertEqual(loaded, "2025-01-03")
        repository.load_cursor.assert_awaited_once()
        repository.upsert_cursor.assert_awaited_once_with(
            mock.ANY,
            planner_name="historical_planner",
            source_slug="sofascore",
            sport_slug="football",
            scope_type="historical_deep",
            scope_id=None,
            cursor_date=date(2025, 1, 4),
            cursor_id=None,
        )

    async def test_service_app_builds_registry_backed_historical_targets(self) -> None:
        from schema_inspector.services.service_app import ServiceApp
        from schema_inspector.storage.tournament_registry_repository import HistoricalPlanningPolicy

        app = type(
            "App",
            (),
            {
                "redis_backend": _FakeRedisBackend(),
                "stream_queue": _FakeQueue(),
                "live_state_store": object(),
                "database": _FakeDatabase(),
                "runtime_config": SimpleNamespace(source_slug="sofascore"),
            },
        )()

        with (
            mock.patch("schema_inspector.services.service_app.TournamentRegistryRepository") as repository_cls,
            mock.patch("schema_inspector.services.service_app.PlannerCursorRepository"),
        ):
            repository = repository_cls.return_value
            repository.list_historical_planning_policies = mock.AsyncMock(
                return_value=(
                    HistoricalPlanningPolicy(
                        source_slug="sofascore",
                        sport_slug="football",
                        historical_backfill_start_date=date(2010, 1, 1),
                        historical_backfill_end_date=None,
                        recent_refresh_days=30,
                    ),
                )
            )
            daemon = ServiceApp(app).build_historical_planner_daemon(
                sport_slugs=("football",),
                today_factory=lambda: date(2026, 4, 22),
            )
            targets = await daemon.target_loader()

        self.assertEqual([target.scope for target in targets], ["historical_deep", "historical_recent_refresh"])
        self.assertEqual(targets[0].date_from, "2010-01-01")
        self.assertEqual(targets[0].date_to, "2026-03-22")
        self.assertEqual(targets[1].date_from, "2026-03-23")
        self.assertEqual(targets[1].date_to, "2026-04-22")


class _FakeQueue:
    def __init__(self, *, group_info_by_stream: dict[tuple[str, str], object] | None = None) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []
        self.group_info_by_stream = dict(group_info_by_stream or {})

    def publish(self, stream: str, values: dict[str, object]) -> str:
        self.published.append((stream, dict(values)))
        return f"{stream}:{len(self.published)}"

    def group_info(self, stream: str, group: str):
        return self.group_info_by_stream.get((stream, group))


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(key, {}).update(dict(mapping))
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))


class _FakeDatabaseConnection:
    async def fetch(self, query: str, *args):
        del query, args
        return []

    async def execute(self, query: str, *args):
        del query, args
        return "OK"


class _FakeDatabaseConnectionContext:
    async def __aenter__(self):
        return _FakeDatabaseConnection()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeDatabase:
    def connection(self):
        return _FakeDatabaseConnectionContext()


class _FakeGroupInfo:
    def __init__(self, *, lag: int | None) -> None:
        self.lag = lag


if __name__ == "__main__":
    unittest.main()
