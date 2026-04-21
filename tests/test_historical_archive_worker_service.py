from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from schema_inspector.queue.streams import (
    STREAM_HISTORICAL_ENRICHMENT,
    STREAM_HISTORICAL_TOURNAMENT,
    StreamEntry,
)


class HistoricalTournamentWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_tournament_worker_runs_archive_and_publishes_enrichment_job(self) -> None:
        from schema_inspector.workers.historical_archive_worker import HistoricalTournamentWorker

        orchestrator = _FakeArchiveOrchestrator()
        queue = _FakeQueue()
        worker = HistoricalTournamentWorker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="worker-historical-tournament-1",
        )

        result = await worker.handle(
            StreamEntry(
                stream=STREAM_HISTORICAL_TOURNAMENT,
                message_id="1-1",
                values={
                    "job_id": "job-1",
                    "job_type": "sync_tournament_archive",
                    "sport_slug": "football",
                    "entity_type": "unique_tournament",
                    "entity_id": "17",
                    "scope": "historical",
                    "params_json": "{}",
                    "attempt": "1",
                    "idempotency_key": "key-1",
                },
            )
        )

        self.assertEqual(result, "completed")
        self.assertEqual(orchestrator.archive_calls, [(17, "football")])
        self.assertEqual(queue.published_streams, [STREAM_HISTORICAL_ENRICHMENT])
        payload = queue.published_payloads[0]
        self.assertEqual(payload["job_type"], "enrich_tournament_archive")
        self.assertEqual(int(payload["entity_id"]), 17)
        self.assertEqual(json.loads(str(payload["params_json"])), {"season_ids": [701, 702]})


class HistoricalEnrichmentWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_enrichment_worker_runs_archive_enrichment(self) -> None:
        from schema_inspector.workers.historical_archive_worker import HistoricalEnrichmentWorker

        orchestrator = _FakeArchiveOrchestrator()
        worker = HistoricalEnrichmentWorker(
            orchestrator=orchestrator,
            queue=_FakeQueue(),
            consumer="worker-historical-enrichment-1",
        )

        result = await worker.handle(
            StreamEntry(
                stream=STREAM_HISTORICAL_ENRICHMENT,
                message_id="1-2",
                values={
                    "job_id": "job-2",
                    "job_type": "enrich_tournament_archive",
                    "sport_slug": "football",
                    "entity_type": "unique_tournament",
                    "entity_id": "17",
                    "scope": "historical",
                    "params_json": '{"season_ids":[701,702]}',
                    "attempt": "1",
                    "idempotency_key": "key-2",
                },
            )
        )

        self.assertEqual(result, "completed")
        self.assertEqual(orchestrator.enrichment_calls, [(17, "football", (701, 702))])


class HistoricalArchiveServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_archive_builds_source_adapter_and_passes_adapter_jobs_to_worker(self) -> None:
        from schema_inspector.services.historical_archive_service import (
            run_historical_tournament_archive,
        )

        fake_adapter = _FakeHistoricalSourceAdapter()
        app = _FakeApp()
        run_result = SimpleNamespace(
            season_ids=(701, 702),
            completed_seasons=2,
            discovered_event_ids=14,
            stage_failures=0,
            success=True,
        )

        with (
            patch(
                "schema_inspector.services.historical_archive_service.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
            patch(
                "schema_inspector.services.historical_archive_service._run_tournament_worker",
                new=_fake_run_tournament_worker,
            ),
        ):
            global _LAST_ARCHIVE_APP, _LAST_TOURNAMENT_WORKER_RESULT
            _LAST_ARCHIVE_APP = app
            _LAST_TOURNAMENT_WORKER_RESULT = run_result

            payload = await run_historical_tournament_archive(
                app,
                unique_tournament_id=17,
                sport_slug="football",
            )

        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=app.runtime_config,
            transport=app.transport,
        )
        self.assertEqual(fake_adapter.competition_build_calls, [app.database])
        self.assertEqual(fake_adapter.event_list_build_calls, [app.database])
        self.assertEqual(fake_adapter.statistics_build_calls, [app.database])
        self.assertEqual(fake_adapter.standings_build_calls, [app.database])
        self.assertEqual(fake_adapter.leaderboards_build_calls, [app.database])
        self.assertEqual(fake_adapter.event_detail_build_calls, [app.database])
        self.assertEqual(fake_adapter.entities_build_calls, [app.database])
        self.assertIs(_LAST_TOURNAMENT_WORKER_KWARGS["competition_job"], fake_adapter.competition_job)
        self.assertIs(_LAST_TOURNAMENT_WORKER_KWARGS["event_list_job"], fake_adapter.event_list_job)
        self.assertIs(_LAST_TOURNAMENT_WORKER_KWARGS["statistics_job"], fake_adapter.statistics_job)
        self.assertIs(_LAST_TOURNAMENT_WORKER_KWARGS["standings_job"], fake_adapter.standings_job)
        self.assertIs(_LAST_TOURNAMENT_WORKER_KWARGS["leaderboards_job"], fake_adapter.leaderboards_job)
        self.assertIs(_LAST_TOURNAMENT_WORKER_KWARGS["event_detail_job"], fake_adapter.event_detail_job)
        self.assertIs(_LAST_TOURNAMENT_WORKER_KWARGS["entities_job"], fake_adapter.entities_job)
        self.assertEqual(
            payload,
            {
                "season_ids": (701, 702),
                "completed_seasons": 2,
                "discovered_event_ids": 14,
                "stage_failures": 0,
                "success": True,
            },
        )
        _LAST_ARCHIVE_APP = None
        _LAST_TOURNAMENT_WORKER_RESULT = None

    async def test_archive_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.services.historical_archive_service import (
            run_historical_tournament_archive,
        )
        from schema_inspector.sources import UnsupportedSourceAdapterError

        with patch(
            "schema_inspector.services.historical_archive_service.build_source_adapter",
            create=True,
            return_value=_UnsupportedArchiveSourceAdapter(),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "statistics ingestion is not wired for source secondary_source",
            ):
                await run_historical_tournament_archive(
                    _FakeApp(),
                    unique_tournament_id=17,
                    sport_slug="football",
                )

    async def test_enrichment_builds_source_adapter_and_uses_adapter_jobs(self) -> None:
        from schema_inspector.services.historical_archive_service import (
            run_historical_tournament_enrichment,
        )
        from schema_inspector.services.historical_planner import (
            choose_event_detail_budget,
            choose_saturation_budget,
        )

        fixed_now = datetime(2026, 4, 21, 12, 0, tzinfo=timezone.utc)
        expected_from = int((fixed_now - timedelta(days=730)).timestamp())
        expected_to = int(fixed_now.timestamp())
        expected_budget = choose_saturation_budget("football")
        expected_event_detail_limit = choose_event_detail_budget("football")
        fake_adapter = _FakeHistoricalSourceAdapter()

        with (
            patch(
                "schema_inspector.services.historical_archive_service.build_source_adapter",
                create=True,
                return_value=fake_adapter,
            ) as adapter_factory,
            patch(
                "schema_inspector.services.historical_archive_service.EventDetailBackfillJob",
                new=_FakeEventDetailBackfillJob,
            ),
            patch(
                "schema_inspector.services.historical_archive_service.EntitiesBackfillJob",
                new=_FakeEntitiesBackfillJob,
            ),
        ):
            app = _FakeApp()
            global _LAST_FAKE_APP
            _LAST_FAKE_APP = app

            await run_historical_tournament_enrichment(
                app,
                unique_tournament_id=17,
                sport_slug="football",
                now_factory=lambda: fixed_now,
            )

        adapter_factory.assert_called_once_with(
            "secondary_source",
            runtime_config=app.runtime_config,
            transport=app.transport,
        )
        self.assertEqual(fake_adapter.event_detail_build_calls, [app.database])
        self.assertEqual(fake_adapter.entities_build_calls, [app.database])
        self.assertIs(app.event_detail_backfill_job, fake_adapter.event_detail_job)
        self.assertIs(app.entities_backfill_job, fake_adapter.entities_job)
        self.assertEqual(app.event_detail_run_kwargs["unique_tournament_ids"], (17,))
        self.assertEqual(app.event_detail_run_kwargs["start_timestamp_from"], expected_from)
        self.assertEqual(app.event_detail_run_kwargs["start_timestamp_to"], expected_to)
        self.assertEqual(app.event_detail_run_kwargs["limit"], expected_event_detail_limit)
        self.assertEqual(app.entities_run_kwargs["unique_tournament_ids"], (17,))
        self.assertEqual(app.entities_run_kwargs["event_timestamp_from"], expected_from)
        self.assertEqual(app.entities_run_kwargs["event_timestamp_to"], expected_to)
        self.assertEqual(app.entities_run_kwargs["player_limit"], expected_budget.player_limit)
        self.assertEqual(app.entities_run_kwargs["team_limit"], expected_budget.team_limit)
        self.assertEqual(app.entities_run_kwargs["player_request_limit"], expected_budget.player_request_limit)
        self.assertEqual(app.entities_run_kwargs["team_request_limit"], expected_budget.team_request_limit)
        _LAST_FAKE_APP = None

    async def test_enrichment_surfaces_unsupported_adapter_error(self) -> None:
        from schema_inspector.services.historical_archive_service import (
            run_historical_tournament_enrichment,
        )
        from schema_inspector.sources import UnsupportedSourceAdapterError

        with patch(
            "schema_inspector.services.historical_archive_service.build_source_adapter",
            create=True,
            return_value=_UnsupportedHistoricalSourceAdapter(),
        ):
            with self.assertRaisesRegex(
                UnsupportedSourceAdapterError,
                "event-detail enrichment is not wired for source secondary_source",
            ):
                await run_historical_tournament_enrichment(
                    _FakeApp(),
                    unique_tournament_id=17,
                    sport_slug="football",
                )


class _FakeArchiveOrchestrator:
    def __init__(self) -> None:
        self.archive_calls: list[tuple[int, str]] = []
        self.enrichment_calls: list[tuple[int, str, tuple[int, ...]]] = []

    async def run_historical_tournament_archive(self, *, unique_tournament_id: int, sport_slug: str):
        self.archive_calls.append((unique_tournament_id, sport_slug))
        return {"season_ids": (701, 702)}

    async def run_historical_tournament_enrichment(
        self,
        *,
        unique_tournament_id: int,
        sport_slug: str,
        season_ids: tuple[int, ...],
    ):
        self.enrichment_calls.append((unique_tournament_id, sport_slug, season_ids))
        return {"ok": True}


class _FakeQueue:
    def __init__(self) -> None:
        self.published_streams: list[str] = []
        self.published_payloads: list[dict[str, object]] = []

    def publish(self, stream: str, values: dict[str, object]) -> str:
        self.published_streams.append(stream)
        self.published_payloads.append(dict(values))
        return f"{stream}:{len(self.published_streams)}"

    def read_group(self, *args, **kwargs):
        del args, kwargs
        return ()

    def ack(self, *args, **kwargs):
        del args, kwargs
        return 0


class _FakeApp:
    def __init__(self) -> None:
        self.runtime_config = SimpleNamespace(source_slug="secondary_source")
        self.transport = object()
        self.database = object()
        self.event_detail_run_kwargs: dict[str, object] = {}
        self.entities_run_kwargs: dict[str, object] = {}
        self.event_detail_backfill_job = None
        self.entities_backfill_job = None


class _FakeEventDetailBackfillJob:
    def __init__(self, detail_job, database) -> None:
        del database
        _LAST_FAKE_APP.event_detail_backfill_job = detail_job

    async def run(self, **kwargs):
        _LAST_FAKE_APP.event_detail_run_kwargs = dict(kwargs)
        return SimpleNamespace(total_candidates=3, succeeded=3, failed=0)


class _FakeEntitiesBackfillJob:
    def __init__(self, ingest_job, database) -> None:
        del database
        _LAST_FAKE_APP.entities_backfill_job = ingest_job

    async def run(self, **kwargs):
        _LAST_FAKE_APP.entities_run_kwargs = dict(kwargs)
        return SimpleNamespace(
            player_ids=(1,),
            team_ids=(2,),
            ingest=SimpleNamespace(written=SimpleNamespace(payload_snapshot_rows=4)),
        )


class _FakeHistoricalSourceAdapter:
    def __init__(self) -> None:
        self.competition_job = object()
        self.event_list_job = object()
        self.statistics_job = object()
        self.standings_job = object()
        self.leaderboards_job = object()
        self.event_detail_job = object()
        self.entities_job = object()
        self.competition_build_calls: list[object] = []
        self.event_list_build_calls: list[object] = []
        self.statistics_build_calls: list[object] = []
        self.standings_build_calls: list[object] = []
        self.leaderboards_build_calls: list[object] = []
        self.event_detail_build_calls: list[object] = []
        self.entities_build_calls: list[object] = []

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


class _UnsupportedHistoricalSourceAdapter:
    def build_event_detail_job(self, database):
        del database
        from schema_inspector.sources import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "event-detail enrichment is not wired for source secondary_source"
        )

    def build_entities_job(self, database):
        del database
        return object()


class _UnsupportedArchiveSourceAdapter:
    def build_competition_job(self, database):
        del database
        return object()

    def build_event_list_job(self, database):
        del database
        return object()

    def build_statistics_job(self, database):
        del database
        from schema_inspector.sources import UnsupportedSourceAdapterError

        raise UnsupportedSourceAdapterError(
            "statistics ingestion is not wired for source secondary_source"
        )

    def build_standings_job(self, database):
        del database
        return object()

    def build_leaderboards_job(self, database):
        del database
        return object()

    def build_event_detail_job(self, database):
        del database
        return object()

    def build_entities_job(self, database):
        del database
        return object()


async def _fake_run_tournament_worker(database, **kwargs):
    del database
    global _LAST_TOURNAMENT_WORKER_KWARGS, _LAST_TOURNAMENT_WORKER_RESULT
    _LAST_TOURNAMENT_WORKER_KWARGS = dict(kwargs)
    return _LAST_TOURNAMENT_WORKER_RESULT


_LAST_FAKE_APP = None
_LAST_ARCHIVE_APP = None
_LAST_TOURNAMENT_WORKER_KWARGS = {}
_LAST_TOURNAMENT_WORKER_RESULT = None


if __name__ == "__main__":
    unittest.main()
