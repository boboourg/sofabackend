from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from schema_inspector.standings_backfill_cli import _run


class StandingsBackfillCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_uses_adapter_built_standings_job_and_passes_expected_args(self) -> None:
        fake_adapter = _FakeStandingsAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.standings_backfill_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.standings_backfill_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.standings_backfill_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.standings_backfill_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
            patch(
                "schema_inspector.standings_backfill_cli.StandingsBackfillJob",
                new=_FakeBackfillJob,
            ),
        ):
            global _LAST_STANDINGS_BACKFILL_STATE
            _LAST_STANDINGS_BACKFILL_STATE = _FakeStandingsBackfillState()
            exit_code = await _run(_build_args(scope=["total", "home", "total"]))

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.standings_build_calls, [fake_database])
        self.assertIs(_LAST_STANDINGS_BACKFILL_STATE.ingest_job, fake_adapter.standings_job)
        self.assertEqual(
            _LAST_STANDINGS_BACKFILL_STATE.run_kwargs,
            {
                "season_limit": 25,
                "season_offset": 4,
                "only_missing": True,
                "scopes": ("total", "home"),
                "concurrency": 6,
                "timeout": 15.0,
            },
        )
        _LAST_STANDINGS_BACKFILL_STATE = None

    async def test_run_uses_total_scope_by_default_when_no_scope_is_passed(self) -> None:
        with (
            patch(
                "schema_inspector.standings_backfill_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.standings_backfill_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.standings_backfill_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.standings_backfill_cli.build_source_adapter",
                create=True,
                return_value=_FakeStandingsAdapter(),
            ),
            patch(
                "schema_inspector.standings_backfill_cli.StandingsBackfillJob",
                new=_FakeBackfillJob,
            ),
        ):
            global _LAST_STANDINGS_BACKFILL_STATE
            _LAST_STANDINGS_BACKFILL_STATE = _FakeStandingsBackfillState()
            exit_code = await _run(_build_args(scope=[]))

        self.assertEqual(exit_code, 0)
        self.assertEqual(_LAST_STANDINGS_BACKFILL_STATE.run_kwargs["scopes"], ("total",))
        _LAST_STANDINGS_BACKFILL_STATE = None

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.standings_backfill_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.standings_backfill_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.standings_backfill_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.standings_backfill_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedStandingsAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "standings ingestion is not wired for source secondary_source",
            ):
                await _run(_build_args(scope=["total"]))


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeStandingsAdapter:
    def __init__(self) -> None:
        self.standings_job = object()
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


class _FakeStandingsBackfillState:
    def __init__(self) -> None:
        self.ingest_job = None
        self.run_kwargs = None


class _FakeBackfillJob:
    def __init__(self, ingest_job, database) -> None:
        del database
        _LAST_STANDINGS_BACKFILL_STATE.ingest_job = ingest_job

    async def run(self, **kwargs):
        _LAST_STANDINGS_BACKFILL_STATE.run_kwargs = dict(kwargs)
        return SimpleNamespace(total_candidates=10, processed=10, succeeded=10, failed=0)


def _build_args(*, scope):
    return SimpleNamespace(
        season_limit=25,
        season_offset=4,
        scope=scope,
        timeout=15.0,
        concurrency=6,
        all_seasons=False,
        proxy=[],
        user_agent=None,
        max_attempts=None,
        database_url=None,
        db_min_size=None,
        db_max_size=None,
        db_timeout=None,
    )


_LAST_STANDINGS_BACKFILL_STATE = None


if __name__ == "__main__":
    unittest.main()
