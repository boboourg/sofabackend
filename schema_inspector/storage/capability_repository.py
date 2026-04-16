"""PostgreSQL repository for endpoint capability observations and rollups."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol

from ._temporal import coerce_timestamptz


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


@dataclass(frozen=True)
class CapabilityObservationRecord:
    sport_slug: str
    endpoint_pattern: str
    entity_scope: str | None
    context_type: str | None
    http_status: int | None
    payload_validity: str | None
    payload_root_keys: tuple[str, ...]
    is_empty_payload: bool
    is_soft_error_payload: bool
    observed_at: str
    sample_snapshot_id: int | None


@dataclass(frozen=True)
class CapabilityRollupRecord:
    sport_slug: str
    endpoint_pattern: str
    support_level: str
    confidence: float
    last_success_at: str | None
    last_404_at: str | None
    last_soft_error_at: str | None
    success_count: int
    not_found_count: int
    soft_error_count: int
    empty_count: int
    notes: str | None


class CapabilityRepository:
    """Writes endpoint support observations and planner-facing rollups."""

    async def insert_observation(self, executor: SqlExecutor, record: CapabilityObservationRecord) -> None:
        await executor.execute(
            """
            INSERT INTO endpoint_capability_observation (
                sport_slug,
                endpoint_pattern,
                entity_scope,
                context_type,
                http_status,
                payload_validity,
                payload_root_keys,
                is_empty_payload,
                is_soft_error_payload,
                observed_at,
                sample_snapshot_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10, $11)
            """,
            record.sport_slug,
            record.endpoint_pattern,
            record.entity_scope,
            record.context_type,
            record.http_status,
            record.payload_validity,
            _jsonb(record.payload_root_keys),
            record.is_empty_payload,
            record.is_soft_error_payload,
            coerce_timestamptz(record.observed_at),
            record.sample_snapshot_id,
        )

    async def upsert_rollup(self, executor: SqlExecutor, record: CapabilityRollupRecord) -> None:
        await executor.execute(
            """
            INSERT INTO endpoint_capability_rollup (
                sport_slug,
                endpoint_pattern,
                support_level,
                confidence,
                last_success_at,
                last_404_at,
                last_soft_error_at,
                success_count,
                not_found_count,
                soft_error_count,
                empty_count,
                notes
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT (sport_slug, endpoint_pattern) DO UPDATE SET
                support_level = EXCLUDED.support_level,
                confidence = EXCLUDED.confidence,
                last_success_at = EXCLUDED.last_success_at,
                last_404_at = EXCLUDED.last_404_at,
                last_soft_error_at = EXCLUDED.last_soft_error_at,
                success_count = EXCLUDED.success_count,
                not_found_count = EXCLUDED.not_found_count,
                soft_error_count = EXCLUDED.soft_error_count,
                empty_count = EXCLUDED.empty_count,
                notes = EXCLUDED.notes
            """,
            record.sport_slug,
            record.endpoint_pattern,
            record.support_level,
            record.confidence,
            coerce_timestamptz(record.last_success_at),
            coerce_timestamptz(record.last_404_at),
            coerce_timestamptz(record.last_soft_error_at),
            record.success_count,
            record.not_found_count,
            record.soft_error_count,
            record.empty_count,
            record.notes,
        )


def _jsonb(value: object) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
