from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from schema_inspector.current_year_pipeline_cli import _run


class CurrentYearPipelineCliRunTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_builds_source_adapter_and_uses_all_adapter_built_jobs(self) -> None:
        fake_adapter = _FakeCurrentYearAdapter()
        runtime_config = SimpleNamespace(source_slug="secondary_source")
        fake_database = object()
        sport_profile = SimpleNamespace(
            use_daily_categories_seed=True,
            use_scheduled_tournaments=True,
            discovery_category_seed_ids=(301,),
            include_categories_all_discovery=False,
        )
        event_detail_wrapper = _FakeEventDetailBackfillWrapper()
        entities_wrapper = _FakeEntitiesBackfillWrapper()

        with (
            patch("schema_inspector.current_year_pipeline_cli.load_runtime_config", return_value=runtime_config),
            patch("schema_inspector.current_year_pipeline_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.current_year_pipeline_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(fake_database),
            ),
            patch("schema_inspector.current_year_pipeline_cli.resolve_sport_profile", return_value=sport_profile),
            patch(
                "schema_inspector.current_year_pipeline_cli._resolve_dates",
                return_value=["2026-01-02", "2026-01-03"],
            ),
            patch(
                "schema_inspector.current_year_pipeline_cli.resolve_timestamp_bounds",
                return_value=(1704153600, 1704326399),
            ),
            patch(
                "schema_inspector.current_year_pipeline_cli.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
            patch(
                "schema_inspector.current_year_pipeline_cli.EventDetailBackfillJob",
                side_effect=lambda ingest_job, database: event_detail_wrapper.bind(ingest_job, database),
            ),
            patch(
                "schema_inspector.current_year_pipeline_cli.EntitiesBackfillJob",
                side_effect=lambda ingest_job, database: entities_wrapper.bind(ingest_job, database),
            ),
            patch(
                "schema_inspector.current_year_pipeline_cli._discover_from_daily_categories",
                AsyncMock(return_value=((1001, 1002), 0)),
            ) as discover_daily,
            patch(
                "schema_inspector.current_year_pipeline_cli._discover_from_scheduled_tournaments",
                AsyncMock(return_value=((1002, 1003), 4, 0)),
            ) as discover_scheduled_tournaments,
            patch(
                "schema_inspector.current_year_pipeline_cli._discover_from_category_tournaments",
                AsyncMock(return_value=((1004,), 0)),
            ) as discover_category_tournaments,
            patch(
                "schema_inspector.current_year_pipeline_cli._run_competitions",
                AsyncMock(
                    return_value=(
                        SimpleNamespace(unique_tournament_id=1002, success=True),
                        SimpleNamespace(unique_tournament_id=1003, success=True),
                    )
                ),
            ) as run_competitions,
            patch(
                "schema_inspector.current_year_pipeline_cli._run_scheduled_events",
                AsyncMock(
                    return_value=(
                        (
                            SimpleNamespace(written=SimpleNamespace(event_rows=7)),
                            SimpleNamespace(written=SimpleNamespace(event_rows=8)),
                        ),
                        0,
                    )
                ),
            ) as run_scheduled_events,
            patch("schema_inspector.current_year_pipeline_cli._progress"),
            patch("builtins.print"),
        ):
            exit_code = await _run(_build_args())

        self.assertEqual(exit_code, 0)
        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=runtime_config,
        )
        self.assertEqual(fake_adapter.build_calls, [fake_database] * 7)

        discover_daily.assert_awaited_once_with(
            categories_job=fake_adapter.categories_job,
            sport_slug="football",
            dates=["2026-01-02", "2026-01-03"],
            discovery_date_concurrency=3,
            timeout=15.0,
            timezone_name="Europe/Kiev",
            timezone_offset_seconds=None,
        )
        discover_scheduled_tournaments.assert_awaited_once_with(
            scheduled_tournaments_job=fake_adapter.scheduled_tournaments_job,
            sport_slug="football",
            dates=["2026-01-02", "2026-01-03"],
            discovery_date_concurrency=3,
            scheduled_page_limit=2,
            timeout=15.0,
        )
        discover_category_tournaments.assert_awaited_once_with(
            category_tournaments_job=fake_adapter.category_tournaments_job,
            sport_slug="football",
            seed_category_ids=(901,),
            profile_category_ids=(301,),
            include_all_categories=True,
            timeout=15.0,
        )
        run_competitions.assert_awaited_once_with(
            competition_job=fake_adapter.competition_job,
            unique_tournament_ids=(1002, 1003),
            concurrency=5,
            timeout=15.0,
        )
        run_scheduled_events.assert_awaited_once_with(
            event_list_job=fake_adapter.event_list_job,
            sport_slug="football",
            dates=["2026-01-02", "2026-01-03"],
            concurrency=4,
            timeout=15.0,
        )

        self.assertIs(event_detail_wrapper.ingest_job, fake_adapter.event_detail_ingest_job)
        self.assertIs(event_detail_wrapper.database, fake_database)
        event_detail_wrapper.run.assert_awaited_once_with(
            limit=11,
            only_missing=True,
            unique_tournament_ids=(1002, 1003),
            start_timestamp_from=1704153600,
            start_timestamp_to=1704326399,
            provider_ids=(7, 9),
            concurrency=6,
            timeout=15.0,
        )

        self.assertIs(entities_wrapper.ingest_job, fake_adapter.entities_ingest_job)
        self.assertIs(entities_wrapper.database, fake_database)
        entities_wrapper.run.assert_awaited_once_with(
            player_limit=12,
            team_limit=13,
            player_request_limit=14,
            team_request_limit=15,
            only_missing=True,
            unique_tournament_ids=(1002, 1003),
            event_timestamp_from=1704153600,
            event_timestamp_to=1704326399,
            timeout=15.0,
        )

    async def test_run_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        with (
            patch(
                "schema_inspector.current_year_pipeline_cli.load_runtime_config",
                return_value=SimpleNamespace(source_slug="secondary_source"),
            ),
            patch("schema_inspector.current_year_pipeline_cli.load_database_config", return_value=object()),
            patch(
                "schema_inspector.current_year_pipeline_cli.AsyncpgDatabase",
                return_value=_FakeAsyncpgDatabaseContext(object()),
            ),
            patch(
                "schema_inspector.current_year_pipeline_cli.resolve_sport_profile",
                return_value=SimpleNamespace(
                    use_daily_categories_seed=True,
                    use_scheduled_tournaments=False,
                    discovery_category_seed_ids=(),
                    include_categories_all_discovery=False,
                ),
            ),
            patch(
                "schema_inspector.current_year_pipeline_cli._resolve_dates",
                return_value=["2026-01-02"],
            ),
            patch(
                "schema_inspector.current_year_pipeline_cli.resolve_timestamp_bounds",
                return_value=(1704153600, 1704239999),
            ),
            patch(
                "schema_inspector.current_year_pipeline_cli.build_source_adapter",
                create=True,
                return_value=_UnsupportedCurrentYearAdapter(),
            ),
            patch("schema_inspector.current_year_pipeline_cli._progress"),
            patch("builtins.print"),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "categories bootstrap is not wired for source secondary_source",
            ):
                await _run(_build_args())


class _FakeAsyncpgDatabaseContext:
    def __init__(self, database) -> None:
        self.database = database

    async def __aenter__(self):
        return self.database

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeCurrentYearAdapter:
    def __init__(self) -> None:
        self.build_calls: list[object] = []
        self.categories_job = object()
        self.category_tournaments_job = object()
        self.scheduled_tournaments_job = object()
        self.competition_job = object()
        self.event_list_job = object()
        self.event_detail_ingest_job = object()
        self.entities_ingest_job = object()

    def build_categories_seed_job(self, database):
        self.build_calls.append(database)
        return self.categories_job

    def build_category_tournaments_job(self, database):
        self.build_calls.append(database)
        return self.category_tournaments_job

    def build_scheduled_tournaments_job(self, database):
        self.build_calls.append(database)
        return self.scheduled_tournaments_job

    def build_competition_job(self, database):
        self.build_calls.append(database)
        return self.competition_job

    def build_event_list_job(self, database):
        self.build_calls.append(database)
        return self.event_list_job

    def build_event_detail_job(self, database):
        self.build_calls.append(database)
        return self.event_detail_ingest_job

    def build_entities_job(self, database):
        self.build_calls.append(database)
        return self.entities_ingest_job


class _UnsupportedCurrentYearAdapter:
    def build_categories_seed_job(self, database):
        del database
        from schema_inspector.sources.base import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "categories bootstrap is not wired for source secondary_source"
        )


class _FakeEventDetailBackfillWrapper:
    def __init__(self) -> None:
        self.ingest_job = None
        self.database = None
        self.run = AsyncMock(
            return_value=SimpleNamespace(
                total_candidates=10,
                succeeded=10,
                failed=0,
            )
        )

    def bind(self, ingest_job, database):
        self.ingest_job = ingest_job
        self.database = database
        return self


class _FakeEntitiesBackfillWrapper:
    def __init__(self) -> None:
        self.ingest_job = None
        self.database = None
        self.run = AsyncMock(
            return_value=SimpleNamespace(
                player_ids=(1, 2),
                team_ids=(3,),
                player_overall_requests=(1,),
                team_overall_requests=(2,),
                ingest=SimpleNamespace(
                    written=SimpleNamespace(payload_snapshot_rows=16),
                ),
            )
        )

    def bind(self, ingest_job, database):
        self.ingest_job = ingest_job
        self.database = database
        return self


def _build_args():
    return SimpleNamespace(
        sport_slug=" football ",
        year=2026,
        date_from="2026-01-02",
        date_to="2026-01-03",
        timezone_offset_seconds=None,
        timezone_name="Europe/Kiev",
        tournament_limit=2,
        tournament_offset=1,
        discovery_date_concurrency=3,
        scheduled_page_limit=2,
        competition_concurrency=5,
        scheduled_events_concurrency=4,
        event_detail_concurrency=6,
        category_id=[901],
        disable_category_discovery=False,
        include_all_categories=True,
        skip_competition=False,
        skip_scheduled_events=False,
        skip_event_detail=False,
        skip_entities=False,
        event_detail_limit=11,
        all_event_detail=False,
        entity_player_limit=12,
        entity_team_limit=13,
        entity_player_request_limit=14,
        entity_team_request_limit=15,
        all_entities=False,
        provider_id=[7, 9, 7],
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
