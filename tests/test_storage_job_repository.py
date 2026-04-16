from __future__ import annotations

import unittest

from schema_inspector.storage.job_repository import (
    JobEffectRecord,
    JobRepository,
    JobRunRecord,
    ReplayLogRecord,
)


class _FakeExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"


class JobRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_repository_writes_run_effect_and_replay_rows(self) -> None:
        repository = JobRepository()
        executor = _FakeExecutor()

        await repository.insert_job_run(
            executor,
            JobRunRecord(
                job_run_id="run-1",
                job_id="job-1",
                job_type="discover_sport_surface",
                parent_job_id=None,
                trace_id="trace-1",
                sport_slug="football",
                entity_type="sport",
                entity_id=None,
                scope="scheduled",
                priority=1,
                attempt=1,
                worker_id="worker-a",
                status="succeeded",
                started_at="2026-04-16T09:00:00+00:00",
                finished_at="2026-04-16T09:00:03+00:00",
                duration_ms=3000,
                error_class=None,
                error_message=None,
                retry_scheduled_for=None,
                parser_version=None,
                normalizer_version=None,
                schema_version="v1",
            ),
        )
        await repository.upsert_job_effect(
            executor,
            JobEffectRecord(
                job_run_id="run-1",
                created_job_count=5,
                created_snapshot_count=2,
                created_normalized_rows=9,
                capability_updates=1,
                live_state_transition="cold_to_warm",
            ),
        )
        await repository.insert_replay_log(
            executor,
            ReplayLogRecord(
                replay_id="replay-1",
                source_snapshot_id=42,
                replay_reason="parser_upgrade",
                parser_version="v2",
                started_at="2026-04-16T09:10:00+00:00",
                finished_at="2026-04-16T09:10:01+00:00",
                status="succeeded",
            ),
        )

        statements = [sql for sql, _ in executor.execute_calls]
        self.assertTrue(any("INSERT INTO etl_job_run" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO etl_job_effect" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO etl_replay_log" in sql for sql in statements))


if __name__ == "__main__":
    unittest.main()
