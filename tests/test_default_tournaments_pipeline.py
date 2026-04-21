from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from schema_inspector.competition_parser import ApiPayloadSnapshotRecord, UniqueTournamentSeasonRecord
from schema_inspector.default_tournaments_pipeline_cli import (
    _run,
    _load_season_event_ids,
    _select_season_ids,
    _select_unique_tournament_ids,
)
from schema_inspector.endpoints import UNIQUE_TOURNAMENT_SEASONS_ENDPOINT


class DefaultTournamentsPipelineTests(unittest.TestCase):
    def test_select_unique_tournament_ids_applies_filter_offset_and_limit(self) -> None:
        result = _select_unique_tournament_ids(
            (17, 8, 35, 17, 34),
            offset=1,
            limit=2,
            include_ids=(8, 35, 34),
        )

        self.assertEqual(result, (35, 34))

    def test_select_season_ids_prefers_payload_order(self) -> None:
        competition_result = SimpleNamespace(
            unique_tournament_id=17,
            parsed=SimpleNamespace(
                payload_snapshots=(
                    ApiPayloadSnapshotRecord(
                        endpoint_pattern=UNIQUE_TOURNAMENT_SEASONS_ENDPOINT.pattern,
                        source_url="https://example.test/seasons",
                        envelope_key="seasons",
                        context_entity_type="unique_tournament",
                        context_entity_id=17,
                        payload={"seasons": [{"id": 76986}, {"id": 72958}, {"id": 63515}]},
                        fetched_at="2026-04-12T10:00:00+00:00",
                    ),
                ),
                unique_tournament_seasons=(
                    UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=63515),
                    UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=72958),
                    UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=76986),
                ),
            ),
        )

        self.assertEqual(
            _select_season_ids(competition_result, seasons_per_tournament=2),
            (76986, 72958),
        )

    def test_select_season_ids_falls_back_to_all_when_limit_disabled(self) -> None:
        competition_result = SimpleNamespace(
            unique_tournament_id=17,
            parsed=SimpleNamespace(
                payload_snapshots=(),
                unique_tournament_seasons=(
                    UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=3),
                    UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=9),
                    UniqueTournamentSeasonRecord(unique_tournament_id=17, season_id=5),
                ),
            ),
        )

        self.assertEqual(
            _select_season_ids(competition_result, seasons_per_tournament=0),
            (9, 5, 3),
        )


class DefaultTournamentsPipelineRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_and_uses_adapter_built_parser_and_jobs(self) -> None:
        fake_adapter = _FakeDefaultPipelineAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()
        args = _build_args()

        with (
            patch("schema_inspector.default_tournaments_pipeline_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.default_tournaments_pipeline_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.default_tournaments_pipeline_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch(
                "schema_inspector.default_tournaments_pipeline_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
            patch(
                "schema_inspector.default_tournaments_pipeline_cli._run_tournament_workers",
                new=AsyncMock(return_value=(_worker_result(),)),
            ) as worker_runner,
            patch("schema_inspector.default_tournaments_pipeline_cli._progress"),
        ):
            exit_code = await _run(args)

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.default_list_parser_build_calls, 1)
        self.assertEqual(fake_adapter.competition_build_calls, [fake_database])
        self.assertEqual(fake_adapter.event_list_build_calls, [fake_database])
        self.assertEqual(fake_adapter.statistics_build_calls, [fake_database])
        self.assertEqual(fake_adapter.standings_build_calls, [fake_database])
        self.assertEqual(fake_adapter.leaderboards_build_calls, [fake_database])
        self.assertEqual(fake_adapter.event_detail_build_calls, [fake_database])
        self.assertEqual(fake_adapter.entities_build_calls, [fake_database])
        worker_runner.assert_awaited_once()
        self.assertIs(worker_runner.await_args.kwargs["competition_job"], fake_adapter.competition_job)
        self.assertIs(worker_runner.await_args.kwargs["event_list_job"], fake_adapter.event_list_job)
        self.assertIs(worker_runner.await_args.kwargs["statistics_job"], fake_adapter.statistics_job)
        self.assertIs(worker_runner.await_args.kwargs["standings_job"], fake_adapter.standings_job)
        self.assertIs(worker_runner.await_args.kwargs["leaderboards_job"], fake_adapter.leaderboards_job)
        self.assertIs(worker_runner.await_args.kwargs["event_detail_job"], fake_adapter.event_detail_job)
        self.assertIs(worker_runner.await_args.kwargs["entities_job"], fake_adapter.entities_job)

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.default_tournaments_pipeline_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.default_tournaments_pipeline_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.default_tournaments_pipeline_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.default_tournaments_pipeline_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedDefaultPipelineAdapter(),
            ),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "default tournament list is not wired for source secondary_source",
            ):
                await _run(_build_args())


class _FakeConnection:
    def __init__(self, rows) -> None:
        self.rows = rows
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args):
        self.calls.append((sql, args))
        return self.rows


class DefaultTournamentsPipelineSqlTests(unittest.IsolatedAsyncioTestCase):
    async def test_load_season_event_ids_orders_via_subquery(self) -> None:
        connection = _FakeConnection(rows=[{"id": 14025026}, {"id": 14025027}])

        result = await _load_season_event_ids(
            connection,
            unique_tournament_id=17,
            season_id=76986,
        )

        self.assertEqual(result, (14025026, 14025027))
        self.assertEqual(connection.calls[0][1], (17, 76986))
        self.assertIn("SELECT seed.id", connection.calls[0][0])
        self.assertIn("ORDER BY seed.start_timestamp NULLS LAST, seed.id", connection.calls[0][0])


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeDefaultListParser:
    def __init__(self) -> None:
        self.fetch = AsyncMock(
            return_value=SimpleNamespace(unique_tournament_ids=(17, 8))
        )


class _FakeDefaultPipelineAdapter:
    def __init__(self) -> None:
        self.default_list_parser = _FakeDefaultListParser()
        self.competition_job = object()
        self.event_list_job = object()
        self.statistics_job = object()
        self.standings_job = object()
        self.leaderboards_job = object()
        self.event_detail_job = object()
        self.entities_job = object()
        self.default_list_parser_build_calls = 0
        self.competition_build_calls: list[object] = []
        self.event_list_build_calls: list[object] = []
        self.statistics_build_calls: list[object] = []
        self.standings_build_calls: list[object] = []
        self.leaderboards_build_calls: list[object] = []
        self.event_detail_build_calls: list[object] = []
        self.entities_build_calls: list[object] = []

    def build_default_tournament_list_parser(self):
        self.default_list_parser_build_calls += 1
        return self.default_list_parser

    def build_competition_job(self, database):
        self.competition_build_calls.append(database)
        return self.competition_job

    def build_event_list_job(self, database):
        self.event_list_build_calls.append(database)
        return self.event_list_job

    def build_statistics_job(self, database):
        self.statistics_build_calls.append(database)
        return self.statistics_job

    def build_standings_job(self, database):
        self.standings_build_calls.append(database)
        return self.standings_job

    def build_leaderboards_job(self, database):
        self.leaderboards_build_calls.append(database)
        return self.leaderboards_job

    def build_event_detail_job(self, database):
        self.event_detail_build_calls.append(database)
        return self.event_detail_job

    def build_entities_job(self, database):
        self.entities_build_calls.append(database)
        return self.entities_job


class _UnsupportedDefaultPipelineAdapter:
    def build_default_tournament_list_parser(self):
        from schema_inspector.sources import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "default tournament list is not wired for source secondary_source"
        )


def _build_args():
    return SimpleNamespace(
        country_code="UA",
        sport_slug="football",
        unique_tournament_id=[],
        tournament_limit=None,
        tournament_offset=0,
        tournament_concurrency=3,
        seasons_per_tournament=2,
        statistics_limit=20,
        statistics_offset=0,
        statistics_order="-rating",
        statistics_accumulation="total",
        statistics_group="summary",
        statistics_field=[],
        statistics_filter=[],
        standings_scope=[],
        provider_id=[],
        event_concurrency=3,
        skip_featured_events=False,
        skip_round_events=False,
        skip_event_detail=False,
        skip_entities=False,
        skip_statistics=False,
        skip_standings=False,
        skip_leaderboards=False,
        progress_every=1,
        timeout=20.0,
        proxy=[],
        user_agent=None,
        max_attempts=None,
        database_url=None,
        db_min_size=None,
        db_max_size=None,
        db_timeout=None,
    )


def _worker_result():
    return SimpleNamespace(
        unique_tournament_id=17,
        success=True,
        season_ids=(701, 702),
        completed_seasons=2,
        discovered_team_ids=3,
        discovered_player_ids=4,
        discovered_event_ids=5,
        stage_failures=0,
    )


if __name__ == "__main__":
    unittest.main()
