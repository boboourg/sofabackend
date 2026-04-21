from __future__ import annotations

from contextlib import asynccontextmanager
import unittest


class StageAuditLoggerTests(unittest.IsolatedAsyncioTestCase):
    async def test_stage_logger_records_succeeded_stage_from_job_context(self) -> None:
        from schema_inspector.services.job_execution_context import (
            JobExecutionContext,
            push_job_execution_context,
            reset_job_execution_context,
        )
        from schema_inspector.services.stage_audit_logger import StageAuditLogger

        repository = _FakeStageRepository()
        logger = StageAuditLogger(database=_FakeDatabase(), repository=repository)
        token = push_job_execution_context(
            JobExecutionContext(
                job_run_id="run-1",
                job_id="job-1",
                job_type="enrich_tournament_archive",
                trace_id="trace-1",
                worker_id="worker-historical-enrichment-1",
            )
        )
        try:
            async with logger.stage(
                stage_name="historical.enrichment.entities",
                meta={"unique_tournament_id": 17, "sport_slug": "football"},
                rows_written=4,
            ):
                pass
        finally:
            reset_job_execution_context(token)

        self.assertEqual(len(repository.records), 1)
        record = repository.records[0]
        self.assertEqual(record.job_run_id, "run-1")
        self.assertEqual(record.job_id, "job-1")
        self.assertEqual(record.trace_id, "trace-1")
        self.assertEqual(record.worker_id, "worker-historical-enrichment-1")
        self.assertEqual(record.stage_name, "historical.enrichment.entities")
        self.assertEqual(record.status, "succeeded")
        self.assertEqual(record.rows_written, 4)
        self.assertIn('"unique_tournament_id": 17', record.meta_json)

    async def test_stage_logger_records_failed_stage(self) -> None:
        from schema_inspector.services.job_execution_context import (
            JobExecutionContext,
            push_job_execution_context,
            reset_job_execution_context,
        )
        from schema_inspector.services.stage_audit_logger import StageAuditLogger

        repository = _FakeStageRepository()
        logger = StageAuditLogger(database=_FakeDatabase(), repository=repository)
        token = push_job_execution_context(
            JobExecutionContext(
                job_run_id="run-2",
                job_id="job-2",
                job_type="enrich_tournament_archive",
                trace_id="trace-2",
                worker_id="worker-historical-enrichment-2",
            )
        )
        try:
            with self.assertRaisesRegex(RuntimeError, "boom"):
                async with logger.stage(
                    stage_name="historical.enrichment.event_detail",
                    meta={"unique_tournament_id": 18},
                ):
                    raise RuntimeError("boom")
        finally:
            reset_job_execution_context(token)

        self.assertEqual(len(repository.records), 1)
        record = repository.records[0]
        self.assertEqual(record.status, "failed")
        self.assertEqual(record.stage_name, "historical.enrichment.event_detail")
        self.assertEqual(record.error_class, "RuntimeError")
        self.assertEqual(record.error_message, "boom")


class _FakeDatabase:
    @asynccontextmanager
    async def connection(self):
        yield object()


class _FakeStageRepository:
    def __init__(self) -> None:
        self.records = []

    async def insert_stage_run(self, executor, record) -> None:
        del executor
        self.records.append(record)


if __name__ == "__main__":
    unittest.main()
