from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from schema_inspector.statistics_backfill_cli import _run


class StatisticsBackfillCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_uses_adapter_built_statistics_job_and_passes_expected_args(self) -> None:
        fake_adapter = _FakeStatisticsAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.statistics_backfill_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.statistics_backfill_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.statistics_backfill_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.statistics_backfill_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
            patch(
                "schema_inspector.statistics_backfill_cli.StatisticsBackfillJob",
                new=_FakeBackfillJob,
            ),
        ):
            global _LAST_STATISTICS_BACKFILL_STATE
            _LAST_STATISTICS_BACKFILL_STATE = _FakeStatisticsBackfillState()
            exit_code = await _run(_build_args(skip_results=False))

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.statistics_build_calls, [fake_database])
        self.assertIs(_LAST_STATISTICS_BACKFILL_STATE.ingest_job, fake_adapter.statistics_job)
        self.assertEqual(_LAST_STATISTICS_BACKFILL_STATE.run_kwargs["season_limit"], 25)
        self.assertEqual(_LAST_STATISTICS_BACKFILL_STATE.run_kwargs["season_offset"], 4)
        self.assertEqual(_LAST_STATISTICS_BACKFILL_STATE.run_kwargs["only_missing"], True)
        self.assertEqual(_LAST_STATISTICS_BACKFILL_STATE.run_kwargs["include_info"], True)
        self.assertEqual(_LAST_STATISTICS_BACKFILL_STATE.run_kwargs["concurrency"], 6)
        self.assertEqual(_LAST_STATISTICS_BACKFILL_STATE.run_kwargs["timeout"], 15.0)
        queries = _LAST_STATISTICS_BACKFILL_STATE.run_kwargs["queries"]
        self.assertEqual(len(queries), 1)
        query = queries[0]
        self.assertEqual(query.limit, 25)
        self.assertEqual(query.offset, 5)
        self.assertEqual(query.order, "-rating")
        self.assertEqual(query.accumulation, "total")
        self.assertEqual(query.group, "summary")
        self.assertEqual(query.fields, ("goals", "assists"))
        self.assertEqual(query.filters, ("appearances.GT.4", "team.in.42"))
        _LAST_STATISTICS_BACKFILL_STATE = None

    async def test_run_uses_empty_queries_when_skip_results_is_true(self) -> None:
        with (
            patch(
                "schema_inspector.statistics_backfill_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.statistics_backfill_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.statistics_backfill_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.statistics_backfill_cli.build_source_adapter",
                create=True,
                return_value=_FakeStatisticsAdapter(),
            ),
            patch(
                "schema_inspector.statistics_backfill_cli.StatisticsBackfillJob",
                new=_FakeBackfillJob,
            ),
        ):
            global _LAST_STATISTICS_BACKFILL_STATE
            _LAST_STATISTICS_BACKFILL_STATE = _FakeStatisticsBackfillState()
            exit_code = await _run(_build_args(skip_results=True))

        self.assertEqual(exit_code, 0)
        self.assertEqual(_LAST_STATISTICS_BACKFILL_STATE.run_kwargs["queries"], ())
        _LAST_STATISTICS_BACKFILL_STATE = None

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.statistics_backfill_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.statistics_backfill_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.statistics_backfill_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.statistics_backfill_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedStatisticsAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "statistics ingestion is not wired for source secondary_source",
            ):
                await _run(_build_args(skip_results=False))


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeStatisticsAdapter:
    def __init__(self) -> None:
        self.statistics_job = object()
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


class _FakeStatisticsBackfillState:
    def __init__(self) -> None:
        self.ingest_job = None
        self.run_kwargs = None


class _FakeBackfillJob:
    def __init__(self, ingest_job, database) -> None:
        del database
        _LAST_STATISTICS_BACKFILL_STATE.ingest_job = ingest_job

    async def run(self, **kwargs):
        _LAST_STATISTICS_BACKFILL_STATE.run_kwargs = dict(kwargs)
        return SimpleNamespace(total_candidates=10, processed=10, succeeded=10, failed=0)


def _build_args(*, skip_results: bool):
    return SimpleNamespace(
        season_limit=25,
        season_offset=4,
        timeout=15.0,
        concurrency=6,
        all_seasons=False,
        skip_info=False,
        skip_results=skip_results,
        limit=25,
        offset=5,
        order="-rating",
        accumulation="total",
        group="summary",
        field=["goals", "assists", "goals"],
        filter=["appearances.GT.4", "team.in.42", "appearances.GT.4"],
        proxy=[],
        user_agent=None,
        max_attempts=None,
        database_url=None,
        db_min_size=None,
        db_max_size=None,
        db_timeout=None,
    )


_LAST_STATISTICS_BACKFILL_STATE = None


if __name__ == "__main__":
    unittest.main()
