from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from schema_inspector.bootstrap_pipeline_cli import _run


class BootstrapPipelineRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_and_uses_adapter_built_jobs(self) -> None:
        fake_adapter = _FakeBootstrapAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.bootstrap_pipeline_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.bootstrap_pipeline_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.bootstrap_pipeline_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.bootstrap_pipeline_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
            patch(
                "schema_inspector.bootstrap_pipeline_cli._run_competitions",
                new=AsyncMock(return_value=(_competition_item(success=True),)),
            ) as competitions_runner,
            patch(
                "schema_inspector.bootstrap_pipeline_cli.resolve_timezone_offset_seconds",
                return_value=10800,
            ),
            patch("schema_inspector.bootstrap_pipeline_cli._progress"),
        ):
            exit_code = await _run(_build_args())

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.categories_seed_build_calls, [fake_database])
        self.assertEqual(fake_adapter.competition_build_calls, [fake_database])
        self.assertEqual(fake_adapter.event_list_build_calls, [fake_database])
        self.assertEqual(fake_adapter.categories_job.run.await_count, 1)
        self.assertEqual(fake_adapter.event_list_job.run_scheduled.await_count, 1)
        self.assertEqual(fake_adapter.event_list_job.run_live.await_count, 1)
        competitions_runner.assert_awaited_once()
        self.assertIs(competitions_runner.await_args.args[0], fake_adapter.competition_job)

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.bootstrap_pipeline_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.bootstrap_pipeline_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.bootstrap_pipeline_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.bootstrap_pipeline_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedBootstrapAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "categories bootstrap is not wired for source secondary_source",
            ):
                await _run(_build_args())


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeBootstrapAdapter:
    def __init__(self) -> None:
        self.categories_job = _FakeCategoriesJob()
        self.competition_job = object()
        self.event_list_job = _FakeEventListJob()
        self.categories_seed_build_calls: list[object] = []
        self.competition_build_calls: list[object] = []
        self.event_list_build_calls: list[object] = []

    def build_categories_seed_job(self, database):
        self.categories_seed_build_calls.append(database)
        return self.categories_job

    def build_competition_job(self, database):
        self.competition_build_calls.append(database)
        return self.competition_job

    def build_event_list_job(self, database):
        self.event_list_build_calls.append(database)
        return self.event_list_job


class _UnsupportedBootstrapAdapter:
    def build_categories_seed_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "categories bootstrap is not wired for source secondary_source"
        )


class _FakeCategoriesJob:
    def __init__(self) -> None:
        self.run = AsyncMock(
            return_value=SimpleNamespace(
                written=SimpleNamespace(
                    daily_summary_rows=2,
                    daily_unique_tournament_rows=3,
                    daily_team_rows=4,
                ),
                parsed=SimpleNamespace(
                    daily_unique_tournaments=(
                        SimpleNamespace(unique_tournament_id=17),
                        SimpleNamespace(unique_tournament_id=8),
                    )
                ),
            )
        )


class _FakeEventListJob:
    def __init__(self) -> None:
        self.run_scheduled = AsyncMock(return_value=SimpleNamespace(written=SimpleNamespace(event_rows=5)))
        self.run_live = AsyncMock(return_value=SimpleNamespace(written=SimpleNamespace(event_rows=2)))


def _build_args():
    return SimpleNamespace(
        sport_slug="football",
        date=["2026-04-21"],
        date_from=None,
        date_to=None,
        timezone_offset_seconds=None,
        timezone_name=None,
        competition_limit=None,
        competition_offset=0,
        competition_concurrency=3,
        skip_competition=False,
        skip_scheduled=False,
        include_live=True,
        progress_every=25,
        timeout=20.0,
        proxy=[],
        user_agent=None,
        max_attempts=None,
        database_url=None,
        db_min_size=None,
        db_max_size=None,
        db_timeout=None,
    )


def _competition_item(*, success: bool):
    return SimpleNamespace(success=success)


if __name__ == "__main__":
    unittest.main()
