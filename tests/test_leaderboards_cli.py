from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from schema_inspector.leaderboards_cli import _run


class LeaderboardsCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_uses_adapter_built_leaderboards_job_and_passes_expected_args(self) -> None:
        fake_adapter = _FakeLeaderboardsAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.leaderboards_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.leaderboards_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.leaderboards_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.leaderboards_cli.build_source_adapter",
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
        self.assertEqual(fake_adapter.leaderboards_build_calls, [fake_database])
        fake_adapter.leaderboards_job.run.assert_awaited_once_with(
            17,
            7001,
            sport_slug="football",
            include_top_players=True,
            include_top_ratings=False,
            include_top_players_per_game=True,
            include_top_teams=False,
            include_player_of_the_season_race=True,
            include_player_of_the_season=False,
            include_venues=True,
            include_groups=False,
            include_team_of_the_week=True,
            team_of_the_week_period_ids=[10, 11],
            include_statistics_types=False,
            team_event_scopes=["home", "away"],
            team_top_players_team_ids=[41, 42],
            include_trending_top_players=True,
            timeout=15.0,
        )

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.leaderboards_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.leaderboards_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.leaderboards_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.leaderboards_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedLeaderboardsAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "leaderboards ingestion is not wired for source secondary_source",
            ):
                await _run(_build_args())


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeLeaderboardsAdapter:
    def __init__(self) -> None:
        self.leaderboards_job = _FakeLeaderboardsJob()
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


class _FakeLeaderboardsJob:
    def __init__(self) -> None:
        self.run = AsyncMock(
            return_value=SimpleNamespace(
                written=SimpleNamespace(
                    top_player_snapshot_rows=1,
                    top_player_entry_rows=2,
                    top_team_snapshot_rows=3,
                    top_team_entry_rows=4,
                    period_rows=5,
                    team_of_the_week_rows=6,
                    team_of_the_week_player_rows=7,
                    season_group_rows=8,
                    season_player_of_the_season_rows=9,
                    venue_rows=10,
                    event_rows=11,
                    payload_snapshot_rows=12,
                )
            )
        )


def _build_args():
    return SimpleNamespace(
        unique_tournament_id=17,
        season_id=7001,
        sport_slug="football",
        team_top_players_team_id=[41, 42],
        team_of_week_period_id=[10, 11],
        team_events_scope=["home", "away"],
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
