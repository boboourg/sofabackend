"""Persists worker outcomes into ETL job audit tables."""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timezone

from ..storage.job_repository import JobRepository, JobRunRecord
from ..workers._stream_jobs import decode_stream_payload


class JobAuditLogger:
    def __init__(self, *, database, repository: JobRepository | None = None) -> None:
        self.database = database
        self.repository = repository or JobRepository()

    async def record_run(
        self,
        *,
        values: dict[str, object] | dict[str, str],
        fallback_job_id: str | None,
        job_run_id: str | None,
        worker_id: str,
        status: str,
        started_at: str,
        finished_at: str,
        duration_ms: int,
        error: Exception | None = None,
        retry_scheduled_for: str | None = None,
    ) -> None:
        job = decode_stream_payload(values, fallback_job_id=fallback_job_id)
        record = JobRunRecord(
            job_run_id=str(job_run_id or uuid.uuid4()),
            job_id=job.job_id,
            job_type=job.job_type,
            parent_job_id=job.parent_job_id,
            trace_id=job.trace_id,
            sport_slug=job.sport_slug,
            entity_type=job.entity_type,
            entity_id=job.entity_id,
            scope=job.scope,
            priority=job.priority,
            attempt=job.attempt,
            worker_id=worker_id,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            duration_ms=int(duration_ms),
            error_class=None if error is None else type(error).__name__,
            error_message=None if error is None else str(error),
            retry_scheduled_for=retry_scheduled_for,
            parser_version=None,
            normalizer_version=None,
            schema_version=None,
        )
        async with self.database.connection() as connection:
            await self.repository.insert_job_run(connection, record)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def duration_ms_since(started_monotonic: float) -> int:
    return max(0, int((time.perf_counter() - float(started_monotonic)) * 1000))
