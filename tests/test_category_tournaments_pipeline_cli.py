from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from schema_inspector.category_tournaments_pipeline_cli import _run


class CategoryTournamentsPipelineCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_and_uses_adapter_built_jobs(self) -> None:
        fake_adapter = _FakeCategoryPipelineAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()
        categories_all_result = SimpleNamespace(
            parsed=SimpleNamespace(category_ids=(201, 301)),
            written=SimpleNamespace(payload_snapshot_rows=2),
        )
        category_unique_tournaments_result = SimpleNamespace(
            parsed=SimpleNamespace(
                unique_tournament_ids=(1001, 1002),
                active_unique_tournament_ids=(1002,),
                group_names=("main",),
            )
        )
        tournament_results = (
            SimpleNamespace(success=True, discovered_team_ids=4, discovered_event_ids=5, stage_failures=0),
        )

        with (
            patch(
                "schema_inspector.category_tournaments_pipeline_cli.load_runtime_config",
                return_value=runtime_config,
            ),
            patch("schema_inspector.category_tournaments_pipeline_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.category_tournaments_pipeline_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.category_tournaments_pipeline_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
            patch(
                "schema_inspector.category_tournaments_pipeline_cli._run_tournament_workers",
                AsyncMock(return_value=tournament_results),
            ) as run_tournament_workers,
            patch("schema_inspector.category_tournaments_pipeline_cli._progress"),
            patch("builtins.print"),
        ):
            fake_adapter.category_job.run_categories_all.return_value = categories_all_result
            fake_adapter.category_job.run_category_unique_tournaments.return_value = category_unique_tournaments_result

            exit_code = await _run(_build_args())

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.category_job_build_calls, [fake_database])
        self.assertEqual(fake_adapter.competition_job_build_calls, [fake_database])
        self.assertEqual(fake_adapter.event_list_job_build_calls, [fake_database])
        self.assertEqual(fake_adapter.event_detail_job_build_calls, [fake_database])

        fake_adapter.category_job.run_categories_all.assert_awaited_once_with(
            sport_slug="tennis",
            timeout=15.0,
        )
        fake_adapter.category_job.run_category_unique_tournaments.assert_awaited_once_with(
            201,
            sport_slug="tennis",
            timeout=15.0,
        )
        run_tournament_workers.assert_awaited_once_with(
            competition_job=fake_adapter.competition_job,
            event_list_job=fake_adapter.event_list_job,
            event_detail_job=fake_adapter.event_detail_job,
            sport_slug="tennis",
            observed_date="2026-04-21",
            unique_tournament_ids=(1002,),
            provider_ids=(4, 9),
            tournament_concurrency=4,
            event_concurrency=2,
            skip_competition=False,
            skip_scheduled_events=True,
            skip_event_detail=False,
            timeout=15.0,
            progress_every=3,
        )

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.category_tournaments_pipeline_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.category_tournaments_pipeline_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.category_tournaments_pipeline_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.category_tournaments_pipeline_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedCategoryPipelineAdapter(),
            ),
            patch("schema_inspector.category_tournaments_pipeline_cli._progress"),
            patch("builtins.print"),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "category tournaments discovery is not wired for source secondary_source",
            ):
                await _run(_build_args())


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeCategoryPipelineAdapter:
    def __init__(self) -> None:
        self.category_job = _FakeCategoryJob()
        self.competition_job = object()
        self.event_list_job = object()
        self.event_detail_job = object()
        self.category_job_build_calls: list[object] = []
        self.competition_job_build_calls: list[object] = []
        self.event_list_job_build_calls: list[object] = []
        self.event_detail_job_build_calls: list[object] = []

    def build_category_tournaments_job(self, database):
        self.category_job_build_calls.append(database)
        return self.category_job

    def build_competition_job(self, database):
        self.competition_job_build_calls.append(database)
        return self.competition_job

    def build_event_list_job(self, database):
        self.event_list_job_build_calls.append(database)
        return self.event_list_job

    def build_event_detail_job(self, database):
        self.event_detail_job_build_calls.append(database)
        return self.event_detail_job


class _UnsupportedCategoryPipelineAdapter:
    def build_category_tournaments_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "category tournaments discovery is not wired for source secondary_source"
        )


class _FakeCategoryJob:
    def __init__(self) -> None:
        self.run_categories_all = AsyncMock()
        self.run_category_unique_tournaments = AsyncMock()


def _build_args():
    return SimpleNamespace(
        sport_slug=" tennis ",
        date="2026-04-21",
        category_id=[201],
        include_all_categories=False,
        skip_categories_all=False,
        prefer_all_tournaments=False,
        category_limit=1,
        category_offset=0,
        tournament_limit=1,
        tournament_offset=0,
        tournament_concurrency=4,
        event_concurrency=2,
        skip_competition=False,
        skip_scheduled_events=True,
        skip_event_detail=False,
        provider_id=[4, 9, 4],
        progress_every=3,
        timeout=15.0,
        proxy=[],
        user_agent=None,
        max_attempts=None,
        log_level="INFO",
        database_url=None,
        db_min_size=None,
        db_max_size=None,
        db_timeout=None,
    )


if __name__ == "__main__":
    unittest.main()
