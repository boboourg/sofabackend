from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from schema_inspector.statistics_cli import _run


class StatisticsCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_uses_adapter_built_statistics_job_and_passes_expected_args(self) -> None:
        fake_adapter = _FakeStatisticsAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.statistics_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.statistics_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.statistics_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.statistics_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
        ):
            exit_code = await _run(_build_args())

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.statistics_build_calls, [fake_database])
        fake_adapter.statistics_job.run.assert_awaited_once()
        call = fake_adapter.statistics_job.run.await_args
        self.assertEqual(call.args, (17, 7001))
        self.assertEqual(call.kwargs["include_info"], True)
        self.assertEqual(call.kwargs["timeout"], 15.0)
        self.assertEqual(len(call.kwargs["queries"]), 1)
        query = call.kwargs["queries"][0]
        self.assertEqual(query.limit, 25)
        self.assertEqual(query.offset, 5)
        self.assertEqual(query.order, "-rating")
        self.assertEqual(query.accumulation, "total")
        self.assertEqual(query.group, "summary")
        self.assertEqual(query.fields, ("goals", "assists"))
        self.assertEqual(query.filters, ("appearances.GT.4", "team.in.42"))

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.statistics_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.statistics_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.statistics_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.statistics_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedStatisticsAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "statistics ingestion is not wired for source secondary_source",
            ):
                await _run(_build_args())


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeStatisticsAdapter:
    def __init__(self) -> None:
        self.statistics_job = _FakeStatisticsJob()
        self.statistics_build_calls: list[object] = []

    def build_statistics_job(self, database):
        self.statistics_build_calls.append(database)
        return self.statistics_job


class _UnsupportedStatisticsAdapter:
    def build_statistics_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "statistics ingestion is not wired for source secondary_source"
        )


class _FakeStatisticsJob:
    def __init__(self) -> None:
        self.run = AsyncMock(
            return_value=SimpleNamespace(
                unique_tournament_id=17,
                season_id=7001,
                queries=(object(),),
                written=SimpleNamespace(
                    config_rows=1,
                    snapshot_rows=2,
                    result_rows=3,
                ),
            )
        )


def _build_args():
    return SimpleNamespace(
        unique_tournament_id=17,
        season_id=7001,
        timeout=15.0,
        proxy=[],
        user_agent=None,
        max_attempts=None,
        skip_info=False,
        limit=25,
        offset=5,
        order="-rating",
        accumulation="total",
        group="summary",
        field=["goals", "assists", "goals"],
        filter=["appearances.GT.4", "team.in.42", "appearances.GT.4"],
        database_url=None,
        db_min_size=None,
        db_max_size=None,
        db_timeout=None,
    )


if __name__ == "__main__":
    unittest.main()
