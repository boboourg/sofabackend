from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from schema_inspector.entities_backfill_cli import _run


class EntitiesBackfillCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_uses_adapter_built_entities_job_and_passes_backfill_args(self) -> None:
        fake_adapter = _FakeEntitiesAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.entities_backfill_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.entities_backfill_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.entities_backfill_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.entities_backfill_cli.resolve_timestamp_bounds",
                return_value=(1_700_000_000, 1_700_086_400),
            ),
            patch(
                "schema_inspector.entities_backfill_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
            patch(
                "schema_inspector.entities_backfill_cli.EntitiesBackfillJob",
                new=_FakeBackfillJob,
            ),
        ):
            global _LAST_ENTITIES_BACKFILL_CLI_STATE
            _LAST_ENTITIES_BACKFILL_CLI_STATE = _FakeEntitiesBackfillCliState()
            exit_code = await _run(_build_args())

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.entities_build_calls, [fake_database])
        self.assertIs(_LAST_ENTITIES_BACKFILL_CLI_STATE.ingest_job, fake_adapter.entities_job)
        self.assertEqual(
            _LAST_ENTITIES_BACKFILL_CLI_STATE.run_kwargs,
            {
                "player_limit": 25,
                "player_offset": 4,
                "team_limit": 15,
                "team_offset": 2,
                "player_request_limit": 30,
                "player_request_offset": 5,
                "team_request_limit": 12,
                "team_request_offset": 3,
                "only_missing": True,
                "include_player_statistics": True,
                "include_player_overall": False,
                "include_team_overall": True,
                "include_player_heatmaps": False,
                "include_team_performance_graphs": True,
                "include_player_statistics_seasons": False,
                "include_player_transfer_history": True,
                "include_team_statistics_seasons": False,
                "include_team_player_statistics_seasons": True,
                "event_timestamp_from": 1_700_000_000,
                "event_timestamp_to": 1_700_086_400,
                "timeout": 15.0,
            },
        )
        _LAST_ENTITIES_BACKFILL_CLI_STATE = None

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.entities_backfill_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.entities_backfill_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.entities_backfill_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.entities_backfill_cli.resolve_timestamp_bounds",
                return_value=(1_700_000_000, 1_700_086_400),
            ),
            patch(
                "schema_inspector.entities_backfill_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedEntitiesAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "entities enrichment is not wired for source secondary_source",
            ):
                await _run(_build_args())


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeEntitiesAdapter:
    def __init__(self) -> None:
        self.entities_job = object()
        self.entities_build_calls: list[object] = []

    def build_entities_job(self, database):
        self.entities_build_calls.append(database)
        return self.entities_job


class _UnsupportedEntitiesAdapter:
    def build_entities_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "entities enrichment is not wired for source secondary_source"
        )


class _FakeEntitiesBackfillCliState:
    def __init__(self) -> None:
        self.ingest_job = None
        self.run_kwargs = None


class _FakeBackfillJob:
    def __init__(self, ingest_job, database) -> None:
        del database
        _LAST_ENTITIES_BACKFILL_CLI_STATE.ingest_job = ingest_job

    async def run(self, **kwargs):
        _LAST_ENTITIES_BACKFILL_CLI_STATE.run_kwargs = dict(kwargs)
        return SimpleNamespace(
            player_ids=(1, 2),
            player_statistics_ids=(1,),
            team_ids=(10,),
            player_overall_requests=(object(),),
            team_overall_requests=(object(),),
            player_heatmap_requests=(),
            team_performance_graph_requests=(object(), object()),
            ingest=SimpleNamespace(written=SimpleNamespace(payload_snapshot_rows=9)),
        )


def _build_args():
    return SimpleNamespace(
        player_limit=25,
        player_offset=4,
        team_limit=15,
        team_offset=2,
        player_request_limit=30,
        player_request_offset=5,
        team_request_limit=12,
        team_request_offset=3,
        all_seeds=False,
        skip_player_statistics=False,
        skip_player_overall=True,
        skip_team_overall=False,
        skip_player_heatmaps=True,
        skip_team_performance_graphs=False,
        skip_player_statistics_seasons=True,
        skip_player_transfer_history=False,
        skip_team_statistics_seasons=True,
        skip_team_player_statistics_seasons=False,
        date_from="2026-04-20",
        date_to="2026-04-21",
        timezone_name="Europe/Kiev",
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


_LAST_ENTITIES_BACKFILL_CLI_STATE = None


if __name__ == "__main__":
    unittest.main()
