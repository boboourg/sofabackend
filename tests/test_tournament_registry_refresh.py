from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest import mock


class TournamentRegistryRefreshRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_runner_discovers_all_categories_for_one_sport(self) -> None:
        from schema_inspector.services.tournament_registry_refresh import (
            refresh_tournament_registry_for_sport,
        )

        category_job = mock.Mock()
        category_job.run_categories_all = mock.AsyncMock(
            return_value=SimpleNamespace(
                parsed=SimpleNamespace(category_ids=(7, 9)),
            )
        )
        category_job.run_category_unique_tournaments = mock.AsyncMock(
            side_effect=[
                SimpleNamespace(parsed=SimpleNamespace(unique_tournament_ids=(11, 13))),
                SimpleNamespace(parsed=SimpleNamespace(unique_tournament_ids=(13, 17))),
            ]
        )

        result = await refresh_tournament_registry_for_sport(
            category_job=category_job,
            sport_slug="football",
            timeout_s=17.5,
        )

        self.assertEqual(result.sport_slug, "football")
        self.assertEqual(result.category_count, 2)
        self.assertEqual(result.category_failures, 0)
        self.assertEqual(result.discovered_unique_tournament_count, 3)
        category_job.run_categories_all.assert_awaited_once_with(
            sport_slug="football",
            timeout=17.5,
        )
        self.assertEqual(
            category_job.run_category_unique_tournaments.await_args_list,
            [
                mock.call(7, sport_slug="football", timeout=17.5),
                mock.call(9, sport_slug="football", timeout=17.5),
            ],
        )


class TournamentRegistryRefreshDaemonTests(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_daemon_bootstraps_empty_sport_and_persists_cursor(self) -> None:
        from schema_inspector.services.tournament_registry_refresh import (
            TournamentRegistryRefreshCursorStore,
            TournamentRegistryRefreshDaemon,
        )

        backend = _FakeRedisBackend()
        observed_refreshes: list[str] = []

        async def has_rows(sport_slug: str) -> bool:
            self.assertEqual(sport_slug, "football")
            return False

        async def refresh_sport(sport_slug: str):
            observed_refreshes.append(sport_slug)
            return SimpleNamespace(sport_slug=sport_slug)

        daemon = TournamentRegistryRefreshDaemon(
            cursor_store=TournamentRegistryRefreshCursorStore(backend),
            targets=("football",),
            has_registry_rows=has_rows,
            refresh_sport=refresh_sport,
            refresh_interval_seconds=86_400.0,
            sports_per_tick=1,
            now_ms_factory=lambda: 1_000,
        )

        refreshed = await daemon.tick()

        self.assertEqual(refreshed, 1)
        self.assertEqual(observed_refreshes, ["football"])
        self.assertEqual(
            backend.hashes["hash:etl:tournament_registry_refresh_cursor"]["football:last_refresh_ms"],
            "1000",
        )

    async def test_refresh_daemon_limits_sports_per_tick_and_waits_for_interval(self) -> None:
        from schema_inspector.services.tournament_registry_refresh import (
            TournamentRegistryRefreshCursorStore,
            TournamentRegistryRefreshDaemon,
        )

        backend = _FakeRedisBackend()
        observed_refreshes: list[str] = []
        observed_now = iter((1_000, 2_000, 3_000, 86_405_000))

        async def has_rows(sport_slug: str) -> bool:
            self.assertIn(sport_slug, {"football", "basketball"})
            return True

        async def refresh_sport(sport_slug: str):
            observed_refreshes.append(sport_slug)
            return SimpleNamespace(sport_slug=sport_slug)

        daemon = TournamentRegistryRefreshDaemon(
            cursor_store=TournamentRegistryRefreshCursorStore(backend),
            targets=("football", "basketball"),
            has_registry_rows=has_rows,
            refresh_sport=refresh_sport,
            refresh_interval_seconds=86_400.0,
            sports_per_tick=1,
            now_ms_factory=lambda: next(observed_now),
        )

        first = await daemon.tick()
        second = await daemon.tick()
        third = await daemon.tick()
        fourth = await daemon.tick()

        self.assertEqual((first, second, third, fourth), (1, 1, 0, 1))
        self.assertEqual(observed_refreshes, ["football", "basketball", "football"])


class TournamentRegistryRefreshServiceAppTests(unittest.IsolatedAsyncioTestCase):
    def test_default_service_sports_match_supported_sport_profiles(self) -> None:
        from schema_inspector.services.service_app import DEFAULT_SERVICE_SPORT_SLUGS
        from schema_inspector.sport_profiles import SUPPORTED_SPORT_SLUGS

        self.assertEqual(DEFAULT_SERVICE_SPORT_SLUGS, SUPPORTED_SPORT_SLUGS)

    async def test_service_app_builds_registry_refresh_daemon(self) -> None:
        from schema_inspector.services.service_app import ServiceApp

        fake_job = mock.Mock()
        fake_job.run_categories_all = mock.AsyncMock(
            return_value=SimpleNamespace(parsed=SimpleNamespace(category_ids=()))
        )
        fake_job.run_category_unique_tournaments = mock.AsyncMock()
        fake_adapter = mock.Mock()
        fake_adapter.build_category_tournaments_job.return_value = fake_job

        app = type(
            "App",
            (),
            {
                "redis_backend": _FakeRedisBackend(),
                "stream_queue": object(),
                "live_state_store": object(),
                "database": _FakeDatabase(),
                "runtime_config": SimpleNamespace(source_slug="sofascore"),
            },
        )()

        with (
            mock.patch("schema_inspector.services.service_app.build_source_adapter", return_value=fake_adapter),
            mock.patch("schema_inspector.services.service_app.TournamentRegistryRepository") as repository_cls,
        ):
            repository = repository_cls.return_value
            repository.has_rows = mock.AsyncMock(return_value=False)
            daemon = ServiceApp(app).build_tournament_registry_refresh_daemon(
                sport_slugs=("esports",),
                refresh_interval_seconds=86_400.0,
                sports_per_tick=1,
                timeout_s=19.0,
            )
            refreshed = await daemon.tick()

        self.assertEqual(refreshed, 1)
        repository.has_rows.assert_awaited_once()
        fake_adapter.build_category_tournaments_job.assert_called_once_with(app.database)
        fake_job.run_categories_all.assert_awaited_once_with(sport_slug="esports", timeout=19.0)


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


class _FakeDatabaseConnectionContext:
    async def __aenter__(self):
        return _FakeDatabaseConnection()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeDatabase:
    def connection(self):
        return _FakeDatabaseConnectionContext()


if __name__ == "__main__":
    unittest.main()
