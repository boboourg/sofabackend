from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from schema_inspector.categories_seed_cli import _run


class CategoriesSeedCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_and_uses_adapter_built_categories_seed_job(self) -> None:
        fake_adapter = _FakeCategoriesSeedAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.categories_seed_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.categories_seed_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.categories_seed_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.categories_seed_cli.resolve_timezone_offset_seconds",
                return_value=10800,
            ),
            patch(
                "schema_inspector.categories_seed_cli.build_source_adapter",
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
        self.assertEqual(fake_adapter.categories_seed_build_calls, [fake_database])
        fake_adapter.categories_seed_job.run.assert_awaited_once_with(
            "2026-04-21",
            10800,
            sport_slug="football",
            timeout=15.0,
        )

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.categories_seed_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.categories_seed_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.categories_seed_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.categories_seed_cli.resolve_timezone_offset_seconds",
                return_value=10800,
            ),
            patch(
                "schema_inspector.categories_seed_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedCategoriesSeedAdapter(),
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


class _FakeCategoriesSeedAdapter:
    def __init__(self) -> None:
        self.categories_seed_job = _FakeCategoriesSeedJob()
        self.categories_seed_build_calls: list[object] = []

    def build_categories_seed_job(self, database):
        self.categories_seed_build_calls.append(database)
        return self.categories_seed_job


class _UnsupportedCategoriesSeedAdapter:
    def build_categories_seed_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "categories bootstrap is not wired for source secondary_source"
        )


class _FakeCategoriesSeedJob:
    def __init__(self) -> None:
        self.run = AsyncMock(
            return_value=SimpleNamespace(
                observed_date="2026-04-21",
                timezone_offset_seconds=10800,
                written=SimpleNamespace(
                    category_rows=2,
                    daily_summary_rows=3,
                    daily_unique_tournament_rows=4,
                    daily_team_rows=5,
                    payload_snapshot_rows=6,
                ),
            )
        )


def _build_args():
    return SimpleNamespace(
        date="2026-04-21",
        sport_slug="football",
        timezone_offset_seconds=None,
        timezone_name=None,
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
