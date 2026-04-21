from __future__ import annotations

import json
import unittest
from unittest import mock


class HistoricalTournamentPlannerTests(unittest.IsolatedAsyncioTestCase):
    async def test_tournament_planner_publishes_progressively_and_persists_cursor(self) -> None:
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()

        async def selector(*, sport_slug: str, after_unique_tournament_id: int, limit: int) -> tuple[int, ...]:
            self.assertEqual(sport_slug, "football")
            self.assertEqual(limit, 2)
            if after_unique_tournament_id <= 0:
                return (11, 17)
            if after_unique_tournament_id == 17:
                return (23,)
            return ()

        cursor_store = HistoricalTournamentCursorStore(backend)
        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            selector=selector,
            targets=(HistoricalTournamentPlanningTarget(sport_slug="football"),),
            tournaments_per_tick=2,
        )

        first = await daemon.tick()
        second = await daemon.tick()
        third = await daemon.tick()

        self.assertEqual(first, 2)
        self.assertEqual(second, 1)
        self.assertEqual(third, 0)
        self.assertEqual(
            [stream for stream, _ in queue.published],
            ["stream:etl:historical_tournament"] * 3,
        )
        self.assertEqual(
            [int(payload["entity_id"]) for _, payload in queue.published],
            [11, 17, 23],
        )
        self.assertEqual(cursor_store.load_last_unique_tournament_id("football"), 23)
        self.assertEqual(json.loads(str(queue.published[0][1]["params_json"])), {})

    async def test_tournament_planner_pauses_when_backpressure_is_above_threshold(self) -> None:
        from schema_inspector.services.backpressure import BackpressureLimit, QueueBackpressure
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue(group_info_by_stream={("stream:etl:historical_enrichment", "cg:historical_enrichment"): _FakeGroupInfo(lag=200)})

        async def selector(*, sport_slug: str, after_unique_tournament_id: int, limit: int) -> tuple[int, ...]:
            del sport_slug, after_unique_tournament_id, limit
            return (11, 17)

        cursor_store = HistoricalTournamentCursorStore(backend)
        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            selector=selector,
            targets=(HistoricalTournamentPlanningTarget(sport_slug="football"),),
            tournaments_per_tick=2,
            backpressure=QueueBackpressure(
                queue=queue,
                limits=(BackpressureLimit(stream="stream:etl:historical_enrichment", group="cg:historical_enrichment", max_lag=100),),
            ),
        )

        published = await daemon.tick()

        self.assertEqual(published, 0)
        self.assertEqual(queue.published, [])
        self.assertEqual(cursor_store.load_last_unique_tournament_id("football"), 0)

    async def test_tournament_planner_passes_registry_scoped_ids_to_selector(self) -> None:
        from schema_inspector.services.historical_tournament_planner import (
            HistoricalTournamentCursorStore,
            HistoricalTournamentPlannerDaemon,
            HistoricalTournamentPlanningTarget,
        )

        backend = _FakeRedisBackend()
        queue = _FakeQueue()
        observed_calls: list[tuple[str, int, int, tuple[int, ...]]] = []

        async def selector(
            *,
            sport_slug: str,
            after_unique_tournament_id: int,
            limit: int,
            allowed_unique_tournament_ids: tuple[int, ...],
        ) -> tuple[int, ...]:
            observed_calls.append(
                (
                    sport_slug,
                    after_unique_tournament_id,
                    limit,
                    tuple(int(item) for item in allowed_unique_tournament_ids),
                )
            )
            return (23, 99)

        cursor_store = HistoricalTournamentCursorStore(backend)
        daemon = HistoricalTournamentPlannerDaemon(
            queue=queue,
            cursor_store=cursor_store,
            selector=selector,
            targets=(
                HistoricalTournamentPlanningTarget(
                    sport_slug="football",
                    allowed_unique_tournament_ids=(23, 99, 101),
                ),
            ),
            tournaments_per_tick=2,
        )

        published = await daemon.tick()

        self.assertEqual(published, 2)
        self.assertEqual(
            observed_calls,
            [("football", 0, 2, (23, 99, 101))],
        )
        self.assertEqual(
            [int(payload["entity_id"]) for _, payload in queue.published],
            [23, 99],
        )
        self.assertEqual(cursor_store.load_last_unique_tournament_id("football"), 99)


class HistoricalTournamentServiceAppAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_service_app_builds_registry_backed_historical_targets_when_database_available(self) -> None:
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
                "select_unique_tournament_ids_after_cursor": mock.AsyncMock(return_value=()),
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
                    TournamentRegistryTarget(
                        source_slug="sofascore",
                        sport_slug="football",
                        unique_tournament_id=99,
                    ),
                    TournamentRegistryTarget(
                        source_slug="sofascore",
                        sport_slug="tennis",
                        unique_tournament_id=101,
                    ),
                )
            )
            daemon = ServiceApp(app).build_historical_tournament_planner_daemon(
                sport_slugs=("football", "tennis")
            )
            targets = await daemon._target_loader()

        self.assertEqual(daemon._static_targets, ())
        self.assertEqual(
            [(target.sport_slug, target.allowed_unique_tournament_ids) for target in targets],
            [("football", (42, 99)), ("tennis", (101,))],
        )
        self.assertEqual(repository.list_active_targets.await_args.kwargs["surface"], "historical")

    async def test_service_app_historical_registry_loader_does_not_fall_back_when_rows_are_empty(self) -> None:
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
                "select_unique_tournament_ids_after_cursor": mock.AsyncMock(return_value=()),
            },
        )()

        with mock.patch("schema_inspector.services.service_app.TournamentRegistryRepository") as repository_cls:
            repository = repository_cls.return_value
            repository.list_active_targets = mock.AsyncMock(return_value=())
            daemon = ServiceApp(app).build_historical_tournament_planner_daemon(
                sport_slugs=("football",)
            )
            targets = await daemon._target_loader()

        self.assertEqual(daemon._static_targets, ())
        self.assertEqual(targets, ())
        self.assertEqual(repository.list_active_targets.await_args.kwargs["surface"], "historical")


class _FakeQueue:
    def __init__(self, *, group_info_by_stream: dict[tuple[str, str], object] | None = None) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []
        self.group_info_by_stream = dict(group_info_by_stream or {})

    def publish(self, stream: str, values: dict[str, object]) -> str:
        self.published.append((stream, dict(values)))
        return f"{stream}:{len(self.published)}"

    def group_info(self, stream: str, group: str):
        return self.group_info_by_stream.get((stream, group))


class _FakeStreamQueue:
    def ensure_group(self, *args, **kwargs) -> None:
        del args, kwargs


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(key, {}).update(dict(mapping))
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))


class _FakeGroupInfo:
    def __init__(self, *, lag: int | None) -> None:
        self.lag = lag


class _FakeDatabaseConnection:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeDatabase:
    def connection(self):
        return _FakeDatabaseConnection()


if __name__ == "__main__":
    unittest.main()
