from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from schema_inspector.targeted_pipeline_cli import _run, _validate_args


class TargetedPipelineCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_and_uses_adapter_built_jobs(self) -> None:
        fake_adapter = _FakeTargetedAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()

        with (
            patch("schema_inspector.targeted_pipeline_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.targeted_pipeline_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.targeted_pipeline_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.targeted_pipeline_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
            patch("schema_inspector.targeted_pipeline_cli._progress"),
        ):
            exit_code = await _run(_build_args())

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.competition_build_calls, [fake_database])
        self.assertEqual(fake_adapter.statistics_build_calls, [fake_database])
        self.assertEqual(fake_adapter.standings_build_calls, [fake_database])
        self.assertEqual(fake_adapter.leaderboards_build_calls, [fake_database])
        self.assertEqual(fake_adapter.entities_build_calls, [fake_database])
        self.assertEqual(fake_adapter.event_detail_build_calls, [fake_database])

        fake_adapter.competition_job.run.assert_awaited_once_with(
            17,
            season_id=7001,
            timeout=15.0,
        )
        stats_call = fake_adapter.statistics_job.run.await_args
        self.assertEqual(stats_call.args, (17, 7001))
        self.assertEqual(stats_call.kwargs["include_info"], True)
        self.assertEqual(stats_call.kwargs["timeout"], 15.0)
        self.assertEqual(len(stats_call.kwargs["queries"]), 1)
        stats_query = stats_call.kwargs["queries"][0]
        self.assertEqual(stats_query.limit, 25)
        self.assertEqual(stats_query.offset, 5)
        self.assertEqual(stats_query.order, "-rating")
        self.assertEqual(stats_query.accumulation, "total")
        self.assertEqual(stats_query.group, "summary")
        self.assertEqual(stats_query.fields, ("goals", "assists"))
        self.assertEqual(stats_query.filters, ("appearances.GT.4", "team.in.42"))

        fake_adapter.standings_job.run_for_unique_tournament.assert_awaited_once_with(
            17,
            7001,
            scopes=("total", "home", "away"),
            timeout=15.0,
        )
        fake_adapter.leaderboards_job.run.assert_awaited_once_with(
            17,
            7001,
            team_top_players_team_ids=(41, 42),
            team_event_scopes=("home", "away", "total"),
            include_trending_top_players=False,
            timeout=15.0,
        )

        entities_call = fake_adapter.entities_job.run.await_args
        self.assertEqual(entities_call.kwargs["player_ids"], (7, 8))
        self.assertEqual(entities_call.kwargs["player_statistics_ids"], (7, 8))
        self.assertEqual(entities_call.kwargs["team_ids"], (41, 42))
        player_overall = entities_call.kwargs["player_overall_requests"][0]
        self.assertEqual((player_overall.player_id, player_overall.unique_tournament_id, player_overall.season_id), (7, 17, 7001))
        team_graph = entities_call.kwargs["team_performance_graph_requests"][0]
        self.assertEqual((team_graph.team_id, team_graph.unique_tournament_id, team_graph.season_id), (41, 17, 7001))
        self.assertEqual(entities_call.kwargs["timeout"], 15.0)

        event_detail_calls = fake_adapter.event_detail_job.run.await_args_list
        self.assertEqual(len(event_detail_calls), 2)
        self.assertEqual(event_detail_calls[0].args, (1001,))
        self.assertEqual(event_detail_calls[0].kwargs["provider_ids"], (1, 9))
        self.assertEqual(event_detail_calls[0].kwargs["timeout"], 15.0)

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.targeted_pipeline_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.targeted_pipeline_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.targeted_pipeline_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.targeted_pipeline_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedTargetedAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "competition ingestion is not wired for source secondary_source",
            ):
                await _run(_build_args())


class TargetedPipelineCliValidationTests(unittest.TestCase):
    def test_validate_args_requires_at_least_one_target(self) -> None:
        with self.assertRaisesRegex(SystemExit, "Pass at least one target"):
            _validate_args(
                SimpleNamespace(
                    unique_tournament_id=None,
                    team_id=[],
                    player_id=[],
                    event_id=[],
                    season_id=None,
                )
            )


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeTargetedAdapter:
    def __init__(self) -> None:
        self.competition_job = _FakeCompetitionJob()
        self.statistics_job = _FakeStatisticsJob()
        self.standings_job = _FakeStandingsJob()
        self.leaderboards_job = _FakeLeaderboardsJob()
        self.entities_job = _FakeEntitiesJob()
        self.event_detail_job = _FakeEventDetailJob()
        self.competition_build_calls: list[object] = []
        self.statistics_build_calls: list[object] = []
        self.standings_build_calls: list[object] = []
        self.leaderboards_build_calls: list[object] = []
        self.entities_build_calls: list[object] = []
        self.event_detail_build_calls: list[object] = []

    def build_competition_job(self, database):
        self.competition_build_calls.append(database)
        return self.competition_job

    def build_statistics_job(self, database):
        self.statistics_build_calls.append(database)
        return self.statistics_job

    def build_standings_job(self, database):
        self.standings_build_calls.append(database)
        return self.standings_job

    def build_leaderboards_job(self, database):
        self.leaderboards_build_calls.append(database)
        return self.leaderboards_job

    def build_entities_job(self, database):
        self.entities_build_calls.append(database)
        return self.entities_job

    def build_event_detail_job(self, database):
        self.event_detail_build_calls.append(database)
        return self.event_detail_job


class _UnsupportedTargetedAdapter:
    def build_competition_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "competition ingestion is not wired for source secondary_source"
        )


class _FakeCompetitionJob:
    def __init__(self) -> None:
        self.run = AsyncMock(return_value=SimpleNamespace(written=SimpleNamespace(payload_snapshot_rows=2)))


class _FakeStatisticsJob:
    def __init__(self) -> None:
        self.run = AsyncMock(return_value=SimpleNamespace(written=SimpleNamespace(snapshot_rows=3, result_rows=4)))


class _FakeStandingsJob:
    def __init__(self) -> None:
        self.run_for_unique_tournament = AsyncMock(return_value=SimpleNamespace(written=SimpleNamespace(standing_rows=5, standing_row_rows=6)))


class _FakeLeaderboardsJob:
    def __init__(self) -> None:
        self.run = AsyncMock(return_value=SimpleNamespace(written=SimpleNamespace(payload_snapshot_rows=7)))


class _FakeEntitiesJob:
    def __init__(self) -> None:
        self.run = AsyncMock(return_value=SimpleNamespace(written=SimpleNamespace(player_rows=1, player_season_statistics_rows=8, payload_snapshot_rows=9)))


class _FakeEventDetailJob:
    def __init__(self) -> None:
        self.run = AsyncMock(return_value=SimpleNamespace(written=SimpleNamespace(payload_snapshot_rows=1)))


def _build_args():
    return SimpleNamespace(
        unique_tournament_id=17,
        season_id=7001,
        team_id=[41, 42, 41],
        player_id=[7, 8, 7],
        event_id=[1001, 1002, 1001],
        standings_scope=["total", "home", "away", "home"],
        provider_id=[1, 9, 1],
        skip_competition=False,
        skip_statistics=False,
        skip_standings=False,
        skip_leaderboards=False,
        skip_entities=False,
        skip_event_detail=False,
        statistics_limit=25,
        statistics_offset=5,
        statistics_order="-rating",
        statistics_accumulation="total",
        statistics_group="summary",
        statistics_field=["goals", "assists", "goals"],
        statistics_filter=["appearances.GT.4", "team.in.42", "appearances.GT.4"],
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
