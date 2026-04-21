from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from schema_inspector.event_detail_backfill_cli import _run


class EventDetailBackfillCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_uses_adapter_built_detail_job_and_passes_backfill_args(self) -> None:
        fake_adapter = _FakeEventDetailAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.event_detail_backfill_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.event_detail_backfill_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.event_detail_backfill_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.event_detail_backfill_cli.resolve_timestamp_bounds",
                return_value=(1_700_000_000, 1_700_086_400),
            ),
            patch(
                "schema_inspector.event_detail_backfill_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
            patch(
                "schema_inspector.event_detail_backfill_cli.EventDetailBackfillJob",
                new=_FakeBackfillJob,
            ),
        ):
            global _LAST_BACKFILL_CLI_STATE
            _LAST_BACKFILL_CLI_STATE = _FakeBackfillCliState()
            exit_code = await _run(_build_args())

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.event_detail_build_calls, [fake_database])
        self.assertIs(_LAST_BACKFILL_CLI_STATE.detail_job, fake_adapter.detail_job)
        self.assertEqual(
            _LAST_BACKFILL_CLI_STATE.run_kwargs,
            {
                "limit": 25,
                "offset": 4,
                "only_missing": True,
                "unique_tournament_id": 17,
                "start_timestamp_from": 1_700_000_000,
                "start_timestamp_to": 1_700_086_400,
                "provider_ids": (1, 9),
                "concurrency": 7,
                "timeout": 15.0,
            },
        )
        _LAST_BACKFILL_CLI_STATE = None

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.event_detail_backfill_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.event_detail_backfill_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.event_detail_backfill_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.event_detail_backfill_cli.resolve_timestamp_bounds",
                return_value=(1_700_000_000, 1_700_086_400),
            ),
            patch(
                "schema_inspector.event_detail_backfill_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedEventDetailAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "event-detail enrichment is not wired for source secondary_source",
            ):
                await _run(_build_args())


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeEventDetailAdapter:
    def __init__(self) -> None:
        self.detail_job = object()
        self.event_detail_build_calls: list[object] = []

    def build_event_detail_job(self, database):
        self.event_detail_build_calls.append(database)
        return self.detail_job


class _UnsupportedEventDetailAdapter:
    def build_event_detail_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "event-detail enrichment is not wired for source secondary_source"
        )


class _FakeBackfillCliState:
    def __init__(self) -> None:
        self.detail_job = None
        self.run_kwargs = None


class _FakeBackfillJob:
    def __init__(self, detail_job, database) -> None:
        del database
        _LAST_BACKFILL_CLI_STATE.detail_job = detail_job

    async def run(self, **kwargs):
        _LAST_BACKFILL_CLI_STATE.run_kwargs = dict(kwargs)
        return SimpleNamespace(total_candidates=10, processed=10, succeeded=10, failed=0)


def _build_args():
    return SimpleNamespace(
        limit=25,
        offset=4,
        provider_id=[1, 9, 1],
        timeout=15.0,
        concurrency=7,
        unique_tournament_id=17,
        date_from="2026-04-20",
        date_to="2026-04-21",
        timezone_name="Europe/Kiev",
        all_events=False,
        proxy=[],
        user_agent=None,
        max_attempts=None,
        log_level="INFO",
        database_url=None,
        db_min_size=None,
        db_max_size=None,
        db_timeout=None,
    )


_LAST_BACKFILL_CLI_STATE = None


if __name__ == "__main__":
    unittest.main()
