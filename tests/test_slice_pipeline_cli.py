from __future__ import annotations

from contextlib import ExitStack
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from schema_inspector.entities_parser import (
    PlayerHeatmapRequest,
    PlayerOverallRequest,
    TeamOverallRequest,
    TeamPerformanceGraphRequest,
)
from schema_inspector.slice_pipeline_cli import _run
from schema_inspector.statistics_parser import StatisticsQuery


class SlicePipelineCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_and_uses_adapter_built_jobs(self) -> None:
        fake_adapter = _FakeSliceAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = _FakeDatabase()
        sport_profile = _build_sport_profile()

        with ExitStack() as stack:
            stack.enter_context(
                patch("schema_inspector.slice_pipeline_cli.load_runtime_config", return_value=runtime_config)
            )
            stack.enter_context(patch("schema_inspector.slice_pipeline_cli.load_database_config", return_value=object()))
            stack.enter_context(
                patch(
                    "schema_inspector.slice_pipeline_cli.AsyncpgDatabase",
                    return_value=_FakeAsyncpgDatabaseContext(fake_database),
                )
            )
            adapter_factory = stack.enter_context(
                patch(
                    "schema_inspector.slice_pipeline_cli.build_source_adapter",
                    create=True,
                    return_value=fake_adapter,
                )
            )
            stack.enter_context(
                patch("schema_inspector.slice_pipeline_cli.resolve_sport_profile", return_value=sport_profile)
            )
            stack.enter_context(
                patch("schema_inspector.slice_pipeline_cli._load_round_numbers", AsyncMock(return_value=(4, 5)))
            )
            stack.enter_context(
                patch("schema_inspector.slice_pipeline_cli._load_team_event_ids", AsyncMock(return_value=(4001, 4002)))
            )
            stack.enter_context(
                patch("schema_inspector.slice_pipeline_cli._load_team_player_ids", AsyncMock(return_value=(7001, 7002)))
            )
            stack.enter_context(
                patch("schema_inspector.slice_pipeline_cli._load_existing_manager_ids", AsyncMock(return_value=(91,)))
            )
            run_event_detail_batch = stack.enter_context(
                patch(
                    "schema_inspector.slice_pipeline_cli._run_event_detail_batch",
                    AsyncMock(
                        return_value=(
                            SimpleNamespace(event_id=4001, success=True),
                            SimpleNamespace(event_id=4002, success=True),
                        )
                    ),
                )
            )
            stack.enter_context(patch("schema_inspector.slice_pipeline_cli._progress"))
            stack.enter_context(patch("builtins.print"))
            exit_code = await _run(_build_args())

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.build_calls, [fake_database] * 7)
        fake_adapter.competition_job.run.assert_awaited_once_with(77, season_id=2025, timeout=15.0)
        fake_adapter.event_list_job.run_featured.assert_awaited_once_with(77, sport_slug="football", timeout=15.0)
        fake_adapter.event_list_job.run_round.assert_has_awaits(
            [
                unittest.mock.call(77, 2025, 4, sport_slug="football", timeout=15.0),
                unittest.mock.call(77, 2025, 5, sport_slug="football", timeout=15.0),
            ]
        )

        statistics_kwargs = fake_adapter.statistics_job.run.await_args.kwargs
        self.assertEqual(statistics_kwargs["queries"], (_build_statistics_query(),))
        self.assertTrue(statistics_kwargs["include_info"])

        fake_adapter.standings_job.run_for_unique_tournament.assert_awaited_once_with(
            77,
            2025,
            scopes=("away", "total"),
            timeout=15.0,
        )

        leaderboards_kwargs = fake_adapter.leaderboards_job.run.await_args.kwargs
        self.assertEqual(leaderboards_kwargs["team_event_scopes"], ("home", "away", "total"))
        self.assertEqual(leaderboards_kwargs["team_top_players_team_ids"], (11, 12))

        run_event_detail_batch.assert_awaited_once_with(
            fake_adapter.event_detail_job,
            event_ids=(4001, 4002, 9001),
            provider_ids=(3, 5),
            concurrency=4,
            timeout=15.0,
        )

        entities_kwargs = fake_adapter.entities_job.run.await_args.kwargs
        self.assertEqual(entities_kwargs["player_ids"], (41, 7001, 7002))
        self.assertEqual(
            entities_kwargs["player_overall_requests"],
            (
                PlayerOverallRequest(player_id=41, unique_tournament_id=77, season_id=2025),
                PlayerOverallRequest(player_id=7001, unique_tournament_id=77, season_id=2025),
                PlayerOverallRequest(player_id=7002, unique_tournament_id=77, season_id=2025),
            ),
        )
        self.assertEqual(
            entities_kwargs["team_overall_requests"],
            (
                TeamOverallRequest(team_id=11, unique_tournament_id=77, season_id=2025),
                TeamOverallRequest(team_id=12, unique_tournament_id=77, season_id=2025),
            ),
        )
        self.assertEqual(
            entities_kwargs["player_heatmap_requests"],
            (
                PlayerHeatmapRequest(player_id=41, unique_tournament_id=77, season_id=2025),
                PlayerHeatmapRequest(player_id=7001, unique_tournament_id=77, season_id=2025),
                PlayerHeatmapRequest(player_id=7002, unique_tournament_id=77, season_id=2025),
            ),
        )
        self.assertEqual(
            entities_kwargs["team_performance_graph_requests"],
            (
                TeamPerformanceGraphRequest(team_id=11, unique_tournament_id=77, season_id=2025),
                TeamPerformanceGraphRequest(team_id=12, unique_tournament_id=77, season_id=2025),
            ),
        )

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        fake_database = _FakeDatabase()

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "schema_inspector.slice_pipeline_cli.load_runtime_config",
                    return_value=SimpleNamespace(source_slug="secondary_source"),
                )
            )
            stack.enter_context(patch("schema_inspector.slice_pipeline_cli.load_database_config", return_value=object()))
            stack.enter_context(
                patch(
                    "schema_inspector.slice_pipeline_cli.AsyncpgDatabase",
                    return_value=_FakeAsyncpgDatabaseContext(fake_database),
                )
            )
            stack.enter_context(
                patch(
                    "schema_inspector.slice_pipeline_cli.build_source_adapter",
                    create=True,
                    return_value=_UnsupportedSliceAdapter(),
                )
            )
            stack.enter_context(
                patch("schema_inspector.slice_pipeline_cli.resolve_sport_profile", return_value=_build_sport_profile())
            )
            stack.enter_context(
                patch("schema_inspector.slice_pipeline_cli._load_round_numbers", AsyncMock(return_value=()))
            )
            stack.enter_context(
                patch("schema_inspector.slice_pipeline_cli._load_team_event_ids", AsyncMock(return_value=()))
            )
            stack.enter_context(
                patch("schema_inspector.slice_pipeline_cli._load_team_player_ids", AsyncMock(return_value=()))
            )
            stack.enter_context(
                patch("schema_inspector.slice_pipeline_cli._load_existing_manager_ids", AsyncMock(return_value=()))
            )
            stack.enter_context(
                patch("schema_inspector.slice_pipeline_cli._run_event_detail_batch", AsyncMock(return_value=()))
            )
            stack.enter_context(patch("schema_inspector.slice_pipeline_cli._progress"))
            stack.enter_context(patch("builtins.print"))
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "competition ingestion is not wired for source secondary_source",
            ):
                await _run(_build_args())


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeDatabase:
    def connection(self):
        return _FakeConnectionContext()


class _FakeConnectionContext:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeSliceAdapter:
    def __init__(self) -> None:
        self.build_calls: list[object] = []
        self.competition_job = _FakeCompetitionJob()
        self.event_list_job = _FakeEventListJob()
        self.statistics_job = _FakeStatisticsJob()
        self.standings_job = _FakeStandingsJob()
        self.leaderboards_job = _FakeLeaderboardsJob()
        self.event_detail_job = object()
        self.entities_job = _FakeEntitiesJob()

    def build_competition_job(self, database):
        self.build_calls.append(database)
        return self.competition_job

    def build_event_list_job(self, database):
        self.build_calls.append(database)
        return self.event_list_job

    def build_statistics_job(self, database):
        self.build_calls.append(database)
        return self.statistics_job

    def build_standings_job(self, database):
        self.build_calls.append(database)
        return self.standings_job

    def build_leaderboards_job(self, database):
        self.build_calls.append(database)
        return self.leaderboards_job

    def build_entities_job(self, database):
        self.build_calls.append(database)
        return self.entities_job

    def build_event_detail_job(self, database):
        self.build_calls.append(database)
        return self.event_detail_job


class _UnsupportedSliceAdapter:
    def build_competition_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "competition ingestion is not wired for source secondary_source"
        )


class _FakeCompetitionJob:
    def __init__(self) -> None:
        self.run = AsyncMock(
            return_value=SimpleNamespace(
                written=SimpleNamespace(payload_snapshot_rows=2),
            )
        )


class _FakeEventListJob:
    def __init__(self) -> None:
        written = SimpleNamespace(event_rows=3, payload_snapshot_rows=4)
        self.run_featured = AsyncMock(return_value=SimpleNamespace(written=written))
        self.run_round = AsyncMock(return_value=SimpleNamespace(written=written))


class _FakeStatisticsJob:
    def __init__(self) -> None:
        self.run = AsyncMock(
            return_value=SimpleNamespace(
                written=SimpleNamespace(snapshot_rows=5, result_rows=6),
            )
        )


class _FakeStandingsJob:
    def __init__(self) -> None:
        self.run_for_unique_tournament = AsyncMock(
            return_value=SimpleNamespace(
                written=SimpleNamespace(standing_rows=7, standing_row_rows=8),
            )
        )


class _FakeLeaderboardsJob:
    def __init__(self) -> None:
        self.run = AsyncMock(
            return_value=SimpleNamespace(
                written=SimpleNamespace(payload_snapshot_rows=9),
            )
        )


class _FakeEntitiesJob:
    def __init__(self) -> None:
        self.run = AsyncMock(
            return_value=SimpleNamespace(
                written=SimpleNamespace(
                    player_rows=10,
                    player_season_statistics_rows=11,
                    payload_snapshot_rows=12,
                ),
            )
        )


def _build_sport_profile():
    return SimpleNamespace(
        standings_scopes=("total", "home"),
        include_top_ratings=True,
        include_player_of_the_season_race=True,
        include_player_of_the_season=True,
        include_venues=True,
        include_groups=True,
        include_team_of_the_week=True,
        include_statistics_types=True,
        team_event_scopes=("home", "away", "total"),
    )


def _build_statistics_query() -> StatisticsQuery:
    return StatisticsQuery(
        limit=25,
        offset=5,
        order="-rating",
        accumulation="total",
        group="summary",
        fields=("goals", "assists"),
        filters=("position:forward",),
    )


def _build_args():
    return SimpleNamespace(
        sport_slug=" football ",
        unique_tournament_id=77,
        season_id=2025,
        team_id=[11, 12, 11],
        player_id=[41],
        manager_id=[91],
        event_id=[9001],
        provider_id=[3, 5, 3],
        standings_scope=["away", "total", "away"],
        statistics_limit=25,
        statistics_offset=5,
        statistics_order="-rating",
        statistics_accumulation="total",
        statistics_group="summary",
        statistics_field=["goals", "assists", "goals"],
        statistics_filter=["position:forward"],
        event_concurrency=4,
        skip_featured_events=False,
        skip_round_events=False,
        skip_event_detail=False,
        skip_entities=False,
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


if __name__ == "__main__":
    unittest.main()
