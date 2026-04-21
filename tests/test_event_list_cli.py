from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from schema_inspector.event_list_cli import _run


class EventListCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_and_uses_adapter_built_event_list_job(self) -> None:
        fake_adapter = _FakeEventListAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.event_list_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.event_list_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.event_list_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.event_list_cli.build_source_adapter",
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
        self.assertEqual(fake_adapter.event_list_build_calls, [fake_database])
        fake_adapter.event_list_job.run_scheduled.assert_awaited_once_with(
            "2026-04-21",
            sport_slug="football",
            timeout=15.0,
        )

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.event_list_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.event_list_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.event_list_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.event_list_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedEventListAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "event-list discovery is not wired for source secondary_source",
            ):
                await _run(_build_args())


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeEventListAdapter:
    def __init__(self) -> None:
        self.event_list_job = _FakeEventListJob()
        self.event_list_build_calls: list[object] = []

    def build_event_list_job(self, database):
        self.event_list_build_calls.append(database)
        return self.event_list_job


class _UnsupportedEventListAdapter:
    def build_event_list_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "event-list discovery is not wired for source secondary_source"
        )


class _FakeEventListJob:
    def __init__(self) -> None:
        result = SimpleNamespace(
            job_name="scheduled",
            written=SimpleNamespace(
                event_rows=5,
                tournament_rows=2,
                team_rows=3,
                payload_snapshot_rows=4,
            ),
        )
        self.run_scheduled = AsyncMock(return_value=result)


def _build_args():
    return SimpleNamespace(
        command="scheduled",
        date="2026-04-21",
        sport_slug="football",
        unique_tournament_id=17,
        season_id=7001,
        round_number=1,
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
