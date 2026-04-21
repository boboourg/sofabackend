from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from schema_inspector.scheduled_tournaments_cli import _run


class ScheduledTournamentsCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_and_uses_adapter_built_scheduled_tournaments_job(self) -> None:
        fake_adapter = _FakeScheduledTournamentsAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.scheduled_tournaments_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.scheduled_tournaments_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.scheduled_tournaments_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.scheduled_tournaments_cli.build_source_adapter",
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
        self.assertEqual(fake_adapter.scheduled_tournaments_build_calls, [fake_database])
        fake_adapter.scheduled_tournaments_job.run_page.assert_awaited_once_with(
            "2026-04-21",
            3,
            sport_slug="basketball",
            timeout=15.0,
        )

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.scheduled_tournaments_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.scheduled_tournaments_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.scheduled_tournaments_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.scheduled_tournaments_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedScheduledTournamentsAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "scheduled tournaments discovery is not wired for source secondary_source",
            ):
                await _run(_build_args())


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeScheduledTournamentsAdapter:
    def __init__(self) -> None:
        self.scheduled_tournaments_job = _FakeScheduledTournamentsJob()
        self.scheduled_tournaments_build_calls: list[object] = []

    def build_scheduled_tournaments_job(self, database):
        self.scheduled_tournaments_build_calls.append(database)
        return self.scheduled_tournaments_job


class _UnsupportedScheduledTournamentsAdapter:
    def build_scheduled_tournaments_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "scheduled tournaments discovery is not wired for source secondary_source"
        )


class _FakeScheduledTournamentsJob:
    def __init__(self) -> None:
        self.run_page = AsyncMock(
            return_value=SimpleNamespace(
                job_name="scheduled_tournaments",
                parsed=SimpleNamespace(
                    tournament_ids=(11, 12),
                    unique_tournament_ids=(101,),
                    has_next_page=True,
                ),
                written=SimpleNamespace(
                    payload_snapshot_rows=5,
                ),
            )
        )


def _build_args():
    return SimpleNamespace(
        sport_slug="basketball",
        date="2026-04-21",
        page=3,
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
