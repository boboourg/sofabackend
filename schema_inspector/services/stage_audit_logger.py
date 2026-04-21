"""Stage-level telemetry logger for long-running ETL jobs."""

from __future__ import annotations

import json
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from ..storage.job_stage_repository import JobStageRepository, JobStageRunRecord
from .job_execution_context import current_job_execution_context


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class StageAuditLogger:
    def __init__(self, *, database, repository: JobStageRepository | None = None) -> None:
        self.database = database
        self.repository = repository or JobStageRepository()

    @asynccontextmanager
    async def stage(
        self,
        *,
        stage_name: str,
        meta: dict[str, object] | None = None,
        endpoint_pattern: str | None = None,
        proxy_name: str | None = None,
        http_status: int | None = None,
        payload_bytes: int | None = None,
        rows_written: int | None = None,
        rows_deleted: int | None = None,
        lock_wait_ms: int | None = None,
        db_time_ms: int | None = None,
    ):
        context = current_job_execution_context()
        if context is None:
            yield
            return
        started_perf = time.perf_counter()
        started_at = utc_now_iso()
        error: Exception | None = None
        try:
            yield
        except Exception as exc:
            error = exc
            raise
        finally:
            finished_at = utc_now_iso()
            duration_ms = max(0, int((time.perf_counter() - started_perf) * 1000))
            record = JobStageRunRecord(
                job_run_id=context.job_run_id,
                job_id=context.job_id,
                trace_id=context.trace_id,
                worker_id=context.worker_id,
                stage_name=stage_name,
                status="failed" if error is not None else "succeeded",
                started_at=started_at,
                finished_at=finished_at,
                duration_ms=duration_ms,
                endpoint_pattern=endpoint_pattern,
                proxy_name=proxy_name,
                http_status=http_status,
                payload_bytes=payload_bytes,
                rows_written=rows_written,
                rows_deleted=rows_deleted,
                lock_wait_ms=lock_wait_ms,
                db_time_ms=db_time_ms,
                error_class=None if error is None else type(error).__name__,
                error_message=None if error is None else str(error),
                meta_json=json.dumps(meta or {}, ensure_ascii=False, sort_keys=True),
            )
            async with self.database.connection() as connection:
                await self.repository.insert_stage_run(connection, record)
