from __future__ import annotations

import json
import unittest

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


if __name__ == "__main__":
    unittest.main()
