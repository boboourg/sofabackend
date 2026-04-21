from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from schema_inspector.standings_cli import _run


class StandingsCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_uses_adapter_built_standings_job_and_passes_unique_tournament_args(self) -> None:
        fake_adapter = _FakeStandingsAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.standings_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.standings_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.standings_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.standings_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
        ):
            exit_code = await _run(_build_unique_tournament_args())

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.standings_build_calls, [fake_database])
        fake_adapter.standings_job.run_for_unique_tournament.assert_awaited_once_with(
            17,
            7001,
            scopes=("total", "home"),
            timeout=15.0,
        )
        fake_adapter.standings_job.run_for_tournament.assert_not_awaited()

    async def test_run_uses_run_for_tournament_when_tournament_id_is_passed(self) -> None:
        fake_adapter = _FakeStandingsAdapter()

        with (
            patch(
                "schema_inspector.standings_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.standings_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.standings_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.standings_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ),
        ):
            exit_code = await _run(_build_tournament_args())

        self.assertEqual(exit_code, 0)
        fake_adapter.standings_job.run_for_tournament.assert_awaited_once_with(
            99,
            7001,
            scopes=("away",),
            timeout=15.0,
        )
        fake_adapter.standings_job.run_for_unique_tournament.assert_not_awaited()

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.standings_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.standings_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.standings_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.standings_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedStandingsAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "standings ingestion is not wired for source secondary_source",
            ):
                await _run(_build_unique_tournament_args())


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeStandingsAdapter:
    def __init__(self) -> None:
        self.standings_job = _FakeStandingsJob()
        self.standings_build_calls: list[object] = []

    def build_standings_job(self, database):
        self.standings_build_calls.append(database)
        return self.standings_job


class _UnsupportedStandingsAdapter:
    def build_standings_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "standings ingestion is not wired for source secondary_source"
        )


class _FakeStandingsJob:
    def __init__(self) -> None:
        unique_result = SimpleNamespace(
            source_kind="unique_tournament",
            source_id=17,
            season_id=7001,
            scopes=("total", "home"),
            written=SimpleNamespace(standing_rows=2, standing_row_rows=8),
        )
        tournament_result = SimpleNamespace(
            source_kind="tournament",
            source_id=99,
            season_id=7001,
            scopes=("away",),
            written=SimpleNamespace(standing_rows=1, standing_row_rows=4),
        )
        self.run_for_unique_tournament = AsyncMock(return_value=unique_result)
        self.run_for_tournament = AsyncMock(return_value=tournament_result)


def _build_unique_tournament_args():
    return SimpleNamespace(
        unique_tournament_id=17,
        tournament_id=None,
        season_id=7001,
        scope=["total", "home", "total"],
        timeout=15.0,
        proxy=[],
        user_agent=None,
        max_attempts=None,
        database_url=None,
        db_min_size=None,
        db_max_size=None,
        db_timeout=None,
    )


def _build_tournament_args():
    return SimpleNamespace(
        unique_tournament_id=None,
        tournament_id=99,
        season_id=7001,
        scope=["away", "away"],
        timeout=15.0,
        proxy=[],
        user_agent=None,
        max_attempts=None,
        database_url=None,
        db_min_size=None,
        db_max_size=None,
        db_timeout=None,
    )


if __name__ == "__main__":
    unittest.main()
