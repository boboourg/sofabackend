"""PostgreSQL repository for ETL job audit/control-plane tables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from ._temporal import coerce_timestamptz


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


@dataclass(frozen=True)
class JobRunRecord:
    job_run_id: str
    job_id: str
    job_type: str
    parent_job_id: str | None
    trace_id: str | None
    sport_slug: str | None
    entity_type: str | None
    entity_id: int | None
    scope: str | None
    priority: int | None
    attempt: int
    worker_id: str | None
    status: str
    started_at: str
    finished_at: str | None
    duration_ms: int | None
    error_class: str | None
    error_message: str | None
    retry_scheduled_for: str | None
    parser_version: str | None
    normalizer_version: str | None
    schema_version: str | None


@dataclass(frozen=True)
class JobEffectRecord:
    job_run_id: str
    created_job_count: int
    created_snapshot_count: int
    created_normalized_rows: int
    capability_updates: int
    live_state_transition: str | None


@dataclass(frozen=True)
class ReplayLogRecord:
    replay_id: str
    source_snapshot_id: int
    replay_reason: str
    parser_version: str | None
    started_at: str
    finished_at: str | None
    status: str


class JobRepository:
    """Writes ETL job execution and replay metadata."""

    async def insert_job_run(self, executor: SqlExecutor, record: JobRunRecord) -> None:
        await executor.execute(
            """
            INSERT INTO etl_job_run (
                job_run_id,
                job_id,
                job_type,
                parent_job_id,
                trace_id,
                sport_slug,
                entity_type,
                entity_id,
                scope,
                priority,
                attempt,
                worker_id,
                status,
                started_at,
                finished_at,
                duration_ms,
                error_class,
                error_message,
                retry_scheduled_for,
                parser_version,
                normalizer_version,
                schema_version
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22
            )
            """,
            record.job_run_id,
            record.job_id,
            record.job_type,
            record.parent_job_id,
            record.trace_id,
            record.sport_slug,
            record.entity_type,
            record.entity_id,
            record.scope,
            record.priority,
            record.attempt,
            record.worker_id,
            record.status,
            coerce_timestamptz(record.started_at),
            coerce_timestamptz(record.finished_at),
            record.duration_ms,
            record.error_class,
            record.error_message,
            coerce_timestamptz(record.retry_scheduled_for),
            record.parser_version,
            record.normalizer_version,
            record.schema_version,
        )

    async def upsert_job_effect(self, executor: SqlExecutor, record: JobEffectRecord) -> None:
        await executor.execute(
            """
            INSERT INTO etl_job_effect (
                job_run_id,
                created_job_count,
                created_snapshot_count,
                created_normalized_rows,
                capability_updates,
                live_state_transition
            )
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (job_run_id) DO UPDATE SET
                created_job_count = EXCLUDED.created_job_count,
                created_snapshot_count = EXCLUDED.created_snapshot_count,
                created_normalized_rows = EXCLUDED.created_normalized_rows,
                capability_updates = EXCLUDED.capability_updates,
                live_state_transition = EXCLUDED.live_state_transition
            """,
            record.job_run_id,
            record.created_job_count,
            record.created_snapshot_count,
            record.created_normalized_rows,
            record.capability_updates,
            record.live_state_transition,
        )

    async def insert_replay_log(self, executor: SqlExecutor, record: ReplayLogRecord) -> None:
        await executor.execute(
            """
            INSERT INTO etl_replay_log (
                replay_id,
                source_snapshot_id,
                replay_reason,
                parser_version,
                started_at,
                finished_at,
                status
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            record.replay_id,
            record.source_snapshot_id,
            record.replay_reason,
            record.parser_version,
            coerce_timestamptz(record.started_at),
            coerce_timestamptz(record.finished_at),
            record.status,
        )
