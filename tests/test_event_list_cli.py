from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, call, patch

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

    async def test_run_team_last_fetches_requested_page_only(self) -> None:
        fake_adapter = _FakeEventListAdapter()

        with (
            patch("schema_inspector.event_list_cli.load_runtime_config", return_value=SimpleNamespace(source_slug="sofascore")),
            patch("schema_inspector.event_list_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.event_list_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch("schema_inspector.event_list_cli.build_source_adapter", return_value=fake_adapter),
        ):
            exit_code = await _run(_build_args(command="team-last", team_id=2817, page=0))

        self.assertEqual(exit_code, 0)
        fake_adapter.event_list_job.run_team_last.assert_awaited_once_with(
            2817,
            0,
            sport_slug="football",
            timeout=15.0,
        )

    async def test_run_team_last_until_end_supports_more_than_one_hundred_pages(self) -> None:
        fake_adapter = _FakeEventListAdapter()
        fake_adapter.event_list_job.run_team_last = AsyncMock(
            side_effect=[
                _result(f"team_last:2817:{page}", has_next=page < 119)
                for page in range(120)
            ]
        )

        with (
            patch("schema_inspector.event_list_cli.load_runtime_config", return_value=SimpleNamespace(source_slug="sofascore")),
            patch("schema_inspector.event_list_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.event_list_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch("schema_inspector.event_list_cli.build_source_adapter", return_value=fake_adapter),
        ):
            exit_code = await _run(_build_args(command="team-last", team_id=2817, page=0, until_end=True))

        self.assertEqual(exit_code, 0)
        self.assertEqual(fake_adapter.event_list_job.run_team_last.await_count, 120)

    async def test_run_team_next_until_end_follows_has_next_page(self) -> None:
        fake_adapter = _FakeEventListAdapter()
        fake_adapter.event_list_job.run_team_next = AsyncMock(
            side_effect=[
                _result("team_next:2817:0", has_next=True),
                _result("team_next:2817:1", has_next=False),
            ]
        )

        with (
            patch("schema_inspector.event_list_cli.load_runtime_config", return_value=SimpleNamespace(source_slug="sofascore")),
            patch("schema_inspector.event_list_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.event_list_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch("schema_inspector.event_list_cli.build_source_adapter", return_value=fake_adapter),
        ):
            exit_code = await _run(_build_args(command="team-next", team_id=2817, page=0, until_end=True))

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            fake_adapter.event_list_job.run_team_next.await_args_list,
            [
                call(2817, 0, sport_slug="football", timeout=15.0),
                call(2817, 1, sport_slug="football", timeout=15.0),
            ],
        )

    async def test_run_team_surfaces_crawls_last_and_next_until_complete_when_missing_completion_snapshots(self) -> None:
        fake_adapter = _FakeEventListAdapter()
        fake_adapter.event_list_job.run_team_last = AsyncMock(
            side_effect=[
                _result("team_last:2817:0", has_next=True),
                _result("team_last:2817:1", has_next=False),
            ]
        )
        fake_adapter.event_list_job.run_team_next = AsyncMock(
            side_effect=[
                _result("team_next:2817:0", has_next=True),
                _result("team_next:2817:1", has_next=False),
            ]
        )

        with (
            patch("schema_inspector.event_list_cli.load_runtime_config", return_value=SimpleNamespace(source_slug="sofascore")),
            patch("schema_inspector.event_list_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.event_list_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(_FakeDatabase(last_complete=False, next_complete=False)),
            ),
            patch("schema_inspector.event_list_cli.build_source_adapter", return_value=fake_adapter),
        ):
            exit_code = await _run(_build_args(command="team-surfaces", team_id=2817, max_pages=10))

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            fake_adapter.event_list_job.run_team_last.await_args_list,
            [
                call(2817, 0, sport_slug="football", timeout=15.0),
                call(2817, 1, sport_slug="football", timeout=15.0),
            ],
        )
        self.assertEqual(
            fake_adapter.event_list_job.run_team_next.await_args_list,
            [
                call(2817, 0, sport_slug="football", timeout=15.0),
                call(2817, 1, sport_slug="football", timeout=15.0),
            ],
        )

    async def test_run_team_surfaces_refreshes_only_page_zero_after_completion_snapshots_exist(self) -> None:
        fake_adapter = _FakeEventListAdapter()
        fake_adapter.event_list_job.run_team_last = AsyncMock(return_value=_result("team_last:2817:0", has_next=True))
        fake_adapter.event_list_job.run_team_next = AsyncMock(return_value=_result("team_next:2817:0", has_next=True))

        with (
            patch("schema_inspector.event_list_cli.load_runtime_config", return_value=SimpleNamespace(source_slug="sofascore")),
            patch("schema_inspector.event_list_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.event_list_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(_FakeDatabase(last_complete=True, next_complete=True)),
            ),
            patch("schema_inspector.event_list_cli.build_source_adapter", return_value=fake_adapter),
        ):
            exit_code = await _run(_build_args(command="team-surfaces", team_id=2817, max_pages=10))

        self.assertEqual(exit_code, 0)
        fake_adapter.event_list_job.run_team_last.assert_awaited_once_with(
            2817,
            0,
            sport_slug="football",
            timeout=15.0,
        )
        fake_adapter.event_list_job.run_team_next.assert_awaited_once_with(
            2817,
            0,
            sport_slug="football",
            timeout=15.0,
        )


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeDatabaseConnection:
    def __init__(self, *, last_complete: bool, next_complete: bool) -> None:
        self.last_complete = last_complete
        self.next_complete = next_complete

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb

    async def fetchrow(self, *args, **kwargs):
        del args, kwargs
        return {
            "last_complete": self.last_complete,
            "next_complete": self.next_complete,
        }


class _FakeDatabase:
    def __init__(self, *, last_complete: bool, next_complete: bool) -> None:
        self.last_complete = last_complete
        self.next_complete = next_complete

    def connection(self):
        return _FakeDatabaseConnection(
            last_complete=self.last_complete,
            next_complete=self.next_complete,
        )


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
        result = _result("scheduled", has_next=False)
        self.run_scheduled = AsyncMock(return_value=result)
        self.run_team_last = AsyncMock(return_value=_result("team_last:2817:0", has_next=True))
        self.run_team_next = AsyncMock(return_value=_result("team_next:2817:0", has_next=False))


def _result(job_name: str, *, has_next: bool) -> SimpleNamespace:
    return SimpleNamespace(
        job_name=job_name,
        parsed=SimpleNamespace(payload_snapshots=(SimpleNamespace(payload={"hasNextPage": has_next}),)),
        written=SimpleNamespace(
            event_rows=5,
            tournament_rows=2,
            team_rows=3,
            payload_snapshot_rows=4,
        ),
    )


def _build_args(**overrides):
    values = dict(
        command="scheduled",
        date="2026-04-21",
        sport_slug="football",
        unique_tournament_id=17,
        season_id=7001,
        round_number=1,
        team_id=2817,
        page=0,
        until_end=False,
        max_pages=250,
        timeout=15.0,
        proxy=[],
        user_agent=None,
        max_attempts=None,
        database_url=None,
        db_min_size=None,
        db_max_size=None,
        db_timeout=None,
    )
    values.update(overrides)
    return SimpleNamespace(
        **values,
    )


if __name__ == "__main__":
    unittest.main()
