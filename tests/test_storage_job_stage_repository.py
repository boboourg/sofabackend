from __future__ import annotations

from datetime import datetime
import unittest


class _FakeExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"


class JobStageRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_repository_writes_stage_run_rows(self) -> None:
        from schema_inspector.storage.job_stage_repository import (
            JobStageRepository,
            JobStageRunRecord,
        )

        repository = JobStageRepository()
        executor = _FakeExecutor()

        await repository.insert_stage_run(
            executor,
            JobStageRunRecord(
                job_run_id="run-1",
                job_id="job-1",
                trace_id="trace-1",
                worker_id="worker-historical-enrichment-1",
                stage_name="historical.enrichment.entities",
                status="succeeded",
                started_at="2026-04-21T10:00:00+00:00",
                finished_at="2026-04-21T10:00:02+00:00",
                duration_ms=2000,
                endpoint_pattern="/api/v1/event/{event_id}",
                proxy_name="proxy_1",
                http_status=200,
                payload_bytes=512,
                rows_written=12,
                rows_deleted=0,
                lock_wait_ms=15,
                db_time_ms=180,
                error_class=None,
                error_message=None,
                meta_json='{"unique_tournament_id":17}',
            ),
        )

        self.assertEqual(len(executor.execute_calls), 1)
        query, args = executor.execute_calls[0]
        self.assertIn("INSERT INTO etl_job_stage_run", query)
        self.assertIsInstance(args[6], datetime)
        self.assertIsInstance(args[7], datetime)
        self.assertEqual(args[4], "historical.enrichment.entities")
        self.assertEqual(args[9], "/api/v1/event/{event_id}")


if __name__ == "__main__":
    unittest.main()
