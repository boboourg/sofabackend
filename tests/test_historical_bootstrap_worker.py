from __future__ import annotations

import unittest
from types import SimpleNamespace

from schema_inspector.queue.streams import (
    GROUP_HISTORICAL_BOOTSTRAP,
    STREAM_HISTORICAL_BOOTSTRAP,
    StreamEntry,
)


class HistoricalBootstrapWorkerFactoryTests(unittest.IsolatedAsyncioTestCase):
    def test_factory_wires_bootstrap_stream_and_group(self) -> None:
        from schema_inspector.workers.historical_bootstrap_worker import (
            make_historical_bootstrap_worker,
        )

        orchestrator = _FakeArchiveOrchestrator()
        queue = _FakeQueue()

        worker = make_historical_bootstrap_worker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="historical-bootstrap-1",
        )

        self.assertEqual(worker.stream, STREAM_HISTORICAL_BOOTSTRAP)
        self.assertEqual(worker.group, GROUP_HISTORICAL_BOOTSTRAP)
        self.assertEqual(worker.consumer, "historical-bootstrap-1")

    async def test_handle_emits_claim_and_done_logs(self) -> None:
        from schema_inspector.workers.historical_bootstrap_worker import (
            make_historical_bootstrap_worker,
        )

        orchestrator = _FakeArchiveOrchestrator()
        queue = _FakeQueue()
        worker = make_historical_bootstrap_worker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="historical-bootstrap-1",
        )

        entry = StreamEntry(
            stream=STREAM_HISTORICAL_BOOTSTRAP,
            message_id="1-1",
            values={
                "job_id": "job-bootstrap-1",
                "job_type": "sync_tournament_archive",
                "sport_slug": "football",
                "entity_type": "unique_tournament",
                "entity_id": "17",
                "scope": "historical",
                "params_json": '{"target_season_id": 701}',
                "attempt": "1",
                "idempotency_key": "key-1",
            },
        )

        with self.assertLogs(
            "schema_inspector.workers.historical_bootstrap_worker",
            level="INFO",
        ) as captured:
            result = await worker.handle(entry)

        self.assertEqual(result, "completed")
        messages = [r.getMessage() for r in captured.records]
        claim = [m for m in messages if "bootstrap_claim" in m]
        done = [m for m in messages if "bootstrap_job_done" in m]
        self.assertEqual(len(claim), 1, msg=f"claim log missing: {messages}")
        self.assertEqual(len(done), 1, msg=f"done log missing: {messages}")
        self.assertIn("ut=17", claim[0])
        self.assertIn("season=701", claim[0])
        self.assertIn("ut=17", done[0])
        self.assertIn("duration_ms=", done[0])

    async def test_handle_emits_failed_log_on_exception(self) -> None:
        from schema_inspector.workers.historical_bootstrap_worker import (
            make_historical_bootstrap_worker,
        )

        orchestrator = _FakeArchiveOrchestrator(raise_on_archive=RuntimeError("boom"))
        queue = _FakeQueue()
        worker = make_historical_bootstrap_worker(
            orchestrator=orchestrator,
            queue=queue,
            consumer="historical-bootstrap-1",
        )

        entry = StreamEntry(
            stream=STREAM_HISTORICAL_BOOTSTRAP,
            message_id="1-2",
            values={
                "job_id": "job-bootstrap-2",
                "job_type": "sync_tournament_archive",
                "sport_slug": "football",
                "entity_type": "unique_tournament",
                "entity_id": "19",
                "scope": "historical",
                "params_json": '{"target_season_id": 702}',
                "attempt": "1",
                "idempotency_key": "key-2",
            },
        )

        with self.assertLogs(
            "schema_inspector.workers.historical_bootstrap_worker",
            level="WARNING",
        ) as captured:
            with self.assertRaises(RuntimeError):
                await worker.handle(entry)

        failed = [
            r.getMessage() for r in captured.records
            if "bootstrap_job_failed" in r.getMessage()
        ]
        self.assertEqual(len(failed), 1, msg=f"failed log missing: {[r.getMessage() for r in captured.records]}")
        self.assertIn("ut=19", failed[0])
        self.assertIn("season=702", failed[0])


class _FakeQueue:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict]] = []

    def publish(self, stream: str, values: dict) -> str:
        self.published.append((stream, dict(values)))
        return f"1-{len(self.published)}"


class _FakeArchiveOrchestrator:
    """Minimal orchestrator stub matching what HistoricalTournamentWorker.handle calls."""

    def __init__(self, *, raise_on_archive: Exception | None = None) -> None:
        self.archive_calls: list[tuple[int, str, int | None, bool]] = []
        self._raise = raise_on_archive
        self.database = None  # _fetch_catalog_state returns None on missing database → bootstrap_mode=False

    async def run_historical_tournament_archive(
        self,
        *,
        unique_tournament_id: int,
        sport_slug: str,
        target_season_id: int | None,
        bootstrap_mode: bool,
    ) -> dict:
        self.archive_calls.append(
            (unique_tournament_id, sport_slug, target_season_id, bootstrap_mode)
        )
        if self._raise is not None:
            raise self._raise
        return {
            "season_ids": (target_season_id,) if target_season_id else (),
            "capabilities_completed": ("events",),
            "discovered_event_ids": 5,
        }


if __name__ == "__main__":
    unittest.main()
