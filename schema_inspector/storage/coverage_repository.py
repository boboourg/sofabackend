"""PostgreSQL repository for coverage-ledger control-plane rows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

from ._temporal import coerce_timestamptz


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


@dataclass(frozen=True)
class CoverageLedgerRecord:
    source_slug: str
    sport_slug: str
    surface_name: str
    scope_type: str
    scope_id: int
    freshness_status: str
    completeness_ratio: float
    last_success_at: str | None = None
    last_checked_at: str | None = None


class CoverageRepository:
    """Writes coverage-ledger rows for source/surface scopes."""

    async def upsert_coverage(self, executor: SqlExecutor, record: CoverageLedgerRecord) -> None:
        await executor.execute(
            """
            INSERT INTO coverage_ledger (
                source_slug,
                sport_slug,
                surface_name,
                scope_type,
                scope_id,
                freshness_status,
                completeness_ratio,
                last_success_at,
                last_checked_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (source_slug, sport_slug, surface_name, scope_type, scope_id) DO UPDATE SET
                freshness_status = EXCLUDED.freshness_status,
                completeness_ratio = EXCLUDED.completeness_ratio,
                last_success_at = EXCLUDED.last_success_at,
                last_checked_at = EXCLUDED.last_checked_at
            """,
            record.source_slug,
            record.sport_slug,
            record.surface_name,
            record.scope_type,
            record.scope_id,
            record.freshness_status,
            float(record.completeness_ratio),
            coerce_timestamptz(record.last_success_at),
            coerce_timestamptz(record.last_checked_at),
        )
