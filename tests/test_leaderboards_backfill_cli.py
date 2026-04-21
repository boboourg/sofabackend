from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import patch

from schema_inspector.leaderboards_backfill_cli import _run


class LeaderboardsBackfillCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_uses_adapter_built_leaderboards_job_and_passes_expected_args(self) -> None:
        fake_adapter = _FakeLeaderboardsAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.leaderboards_backfill_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.leaderboards_backfill_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.leaderboards_backfill_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.leaderboards_backfill_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
            patch(
                "schema_inspector.leaderboards_backfill_cli.LeaderboardsBackfillJob",
                new=_FakeBackfillJob,
            ),
        ):
            global _LAST_LEADERBOARDS_BACKFILL_STATE
            _LAST_LEADERBOARDS_BACKFILL_STATE = _FakeLeaderboardsBackfillState()
            exit_code = await _run(_build_args(team_events_scope=["home", "away"]))

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.leaderboards_build_calls, [fake_database])
        self.assertIs(_LAST_LEADERBOARDS_BACKFILL_STATE.ingest_job, fake_adapter.leaderboards_job)
        self.assertEqual(
            _LAST_LEADERBOARDS_BACKFILL_STATE.run_kwargs,
            {
                "season_limit": 25,
                "season_offset": 4,
                "only_missing": True,
                "team_limit_per_season": 12,
                "include_top_players": True,
                "include_top_ratings": False,
                "include_top_players_per_game": True,
                "include_top_teams": False,
                "include_player_of_the_season_race": True,
                "include_player_of_the_season": False,
                "include_venues": True,
                "include_groups": False,
                "include_team_of_the_week": True,
                "include_statistics_types": False,
                "include_trending_top_players": True,
                "team_event_scopes": ("home", "away"),
                "concurrency": 6,
                "timeout": 15.0,
            },
        )
        _LAST_LEADERBOARDS_BACKFILL_STATE = None

    async def test_run_uses_default_team_event_scopes_when_not_passed(self) -> None:
        with (
            patch(
                "schema_inspector.leaderboards_backfill_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.leaderboards_backfill_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.leaderboards_backfill_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.leaderboards_backfill_cli.build_source_adapter",
                create=True,
                return_value=_FakeLeaderboardsAdapter(),
            ),
            patch(
                "schema_inspector.leaderboards_backfill_cli.LeaderboardsBackfillJob",
                new=_FakeBackfillJob,
            ),
        ):
            global _LAST_LEADERBOARDS_BACKFILL_STATE
            _LAST_LEADERBOARDS_BACKFILL_STATE = _FakeLeaderboardsBackfillState()
            exit_code = await _run(_build_args(team_events_scope=[]))

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            _LAST_LEADERBOARDS_BACKFILL_STATE.run_kwargs["team_event_scopes"],
            ("home", "away", "total"),
        )
        _LAST_LEADERBOARDS_BACKFILL_STATE = None

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.leaderboards_backfill_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.leaderboards_backfill_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.leaderboards_backfill_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.leaderboards_backfill_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedLeaderboardsAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "leaderboards ingestion is not wired for source secondary_source",
            ):
                await _run(_build_args(team_events_scope=["home"]))


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeLeaderboardsAdapter:
    def __init__(self) -> None:
        self.leaderboards_job = object()
        self.leaderboards_build_calls: list[object] = []

    def build_leaderboards_job(self, database):
        self.leaderboards_build_calls.append(database)
        return self.leaderboards_job


class _UnsupportedLeaderboardsAdapter:
    def build_leaderboards_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "leaderboards ingestion is not wired for source secondary_source"
        )


class _FakeLeaderboardsBackfillState:
    def __init__(self) -> None:
        self.ingest_job = None
        self.run_kwargs = None


class _FakeBackfillJob:
    def __init__(self, ingest_job, database) -> None:
        del database
        _LAST_LEADERBOARDS_BACKFILL_STATE.ingest_job = ingest_job

    async def run(self, **kwargs):
        _LAST_LEADERBOARDS_BACKFILL_STATE.run_kwargs = dict(kwargs)
        return SimpleNamespace(total_candidates=10, processed=10, succeeded=10, failed=0)


def _build_args(*, team_events_scope):
    return SimpleNamespace(
        season_limit=25,
        season_offset=4,
        team_limit_per_season=12,
        timeout=15.0,
        concurrency=6,
        all_seasons=False,
        skip_top_players=False,
        skip_top_ratings=True,
        skip_top_players_per_game=False,
        skip_top_teams=True,
        skip_player_of_the_season_race=False,
        skip_player_of_the_season=True,
        skip_venues=False,
        skip_groups=True,
        skip_team_of_the_week=False,
        skip_statistics_types=True,
        include_trending_top_players=True,
        team_events_scope=team_events_scope,
        proxy=[],
        user_agent=None,
        max_attempts=None,
        database_url=None,
        db_min_size=None,
        db_max_size=None,
        db_timeout=None,
    )


_LAST_LEADERBOARDS_BACKFILL_STATE = None


if __name__ == "__main__":
    unittest.main()
