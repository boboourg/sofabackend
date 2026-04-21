"""PostgreSQL repository for ETL stage-level observability rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from ._temporal import coerce_timestamptz


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


@dataclass(frozen=True)
class JobStageRunRecord:
    job_run_id: str
    job_id: str
    trace_id: str | None
    worker_id: str
    stage_name: str
    status: str
    started_at: str
    finished_at: str | None
    duration_ms: int | None
    endpoint_pattern: str | None = None
    proxy_name: str | None = None
    http_status: int | None = None
    payload_bytes: int | None = None
    rows_written: int | None = None
    rows_deleted: int | None = None
    lock_wait_ms: int | None = None
    db_time_ms: int | None = None
    error_class: str | None = None
    error_message: str | None = None
    meta_json: str = "{}"


class JobStageRepository:
    async def insert_stage_run(self, executor: SqlExecutor, record: JobStageRunRecord) -> None:
        await executor.execute(
            """
            INSERT INTO etl_job_stage_run (
                job_run_id,
                job_id,
                trace_id,
                worker_id,
                stage_name,
                status,
                started_at,
                finished_at,
                duration_ms,
                endpoint_pattern,
                proxy_name,
                http_status,
                payload_bytes,
                rows_written,
                rows_deleted,
                lock_wait_ms,
                db_time_ms,
                error_class,
                error_message,
                meta
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12, $13, $14, $15, $16, $17, $18, $19, $20::jsonb
            )
            """,
            record.job_run_id,
            record.job_id,
            record.trace_id,
            record.worker_id,
            record.stage_name,
            record.status,
            coerce_timestamptz(record.started_at),
            coerce_timestamptz(record.finished_at),
            record.duration_ms,
            record.endpoint_pattern,
            record.proxy_name,
            record.http_status,
            record.payload_bytes,
            record.rows_written,
            record.rows_deleted,
            record.lock_wait_ms,
            record.db_time_ms,
            record.error_class,
            record.error_message,
            record.meta_json,
        )
