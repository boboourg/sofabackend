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


@dataclass(frozen=True)
class CoverageEventScopeStatus:
    scope_id: int
    surface_name: str
    freshness_status: str
    start_timestamp: int | None = None


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
                last_success_at = COALESCE(EXCLUDED.last_success_at, coverage_ledger.last_success_at),
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

    async def select_event_scope_ids(
        self,
        executor,
        *,
        source_slug: str,
        surface_names: tuple[str, ...],
        freshness_statuses: tuple[str, ...],
        sport_slug: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> tuple[int, ...]:
        normalized_surface_names = tuple(str(item) for item in surface_names if str(item))
        normalized_statuses = tuple(str(item) for item in freshness_statuses if str(item))
        if not normalized_surface_names or not normalized_statuses:
            return ()
        normalized_offset = max(0, int(offset or 0))
        normalized_limit = None if limit is None else max(0, int(limit))
        if sport_slug and normalized_limit is None:
            rows = await executor.fetch(
                """
                SELECT scope_id
                FROM coverage_ledger
                WHERE source_slug = $1
                  AND scope_type = 'event'
                  AND surface_name = ANY($2::text[])
                  AND freshness_status = ANY($3::text[])
                  AND sport_slug = $4
                GROUP BY scope_id
                ORDER BY MAX(last_checked_at) DESC NULLS LAST, scope_id DESC
                OFFSET $5
                """,
                source_slug,
                normalized_surface_names,
                normalized_statuses,
                sport_slug,
                normalized_offset,
            )
        elif sport_slug:
            rows = await executor.fetch(
                """
                SELECT scope_id
                FROM coverage_ledger
                WHERE source_slug = $1
                  AND scope_type = 'event'
                  AND surface_name = ANY($2::text[])
                  AND freshness_status = ANY($3::text[])
                  AND sport_slug = $4
                GROUP BY scope_id
                ORDER BY MAX(last_checked_at) DESC NULLS LAST, scope_id DESC
                OFFSET $5 LIMIT $6
                """,
                source_slug,
                normalized_surface_names,
                normalized_statuses,
                sport_slug,
                normalized_offset,
                normalized_limit,
            )
        elif normalized_limit is None:
            rows = await executor.fetch(
                """
                SELECT scope_id
                FROM coverage_ledger
                WHERE source_slug = $1
                  AND scope_type = 'event'
                  AND surface_name = ANY($2::text[])
                  AND freshness_status = ANY($3::text[])
                GROUP BY scope_id
                ORDER BY MAX(last_checked_at) DESC NULLS LAST, scope_id DESC
                OFFSET $4
                """,
                source_slug,
                normalized_surface_names,
                normalized_statuses,
                normalized_offset,
            )
        else:
            rows = await executor.fetch(
                """
                SELECT scope_id
                FROM coverage_ledger
                WHERE source_slug = $1
                  AND scope_type = 'event'
                  AND surface_name = ANY($2::text[])
                  AND freshness_status = ANY($3::text[])
                GROUP BY scope_id
                ORDER BY MAX(last_checked_at) DESC NULLS LAST, scope_id DESC
                OFFSET $4 LIMIT $5
                """,
                source_slug,
                normalized_surface_names,
                normalized_statuses,
                normalized_offset,
                normalized_limit,
            )
        return tuple(
            int(row["scope_id"]) if isinstance(row, dict) else int(row[0])
            for row in rows
        )

    async def fetch_event_scope_statuses(
        self,
        executor,
        *,
        source_slug: str,
        surface_names: tuple[str, ...],
        freshness_statuses: tuple[str, ...],
        sport_slug: str | None = None,
    ) -> tuple[CoverageEventScopeStatus, ...]:
        normalized_surface_names = tuple(str(item) for item in surface_names if str(item))
        normalized_statuses = tuple(str(item) for item in freshness_statuses if str(item))
        if not normalized_surface_names or not normalized_statuses:
            return ()
        if sport_slug:
            rows = await executor.fetch(
                """
                SELECT
                    cl.scope_id,
                    cl.surface_name,
                    cl.freshness_status,
                    e.start_timestamp
                FROM coverage_ledger AS cl
                LEFT JOIN event AS e ON e.id = cl.scope_id
                WHERE cl.source_slug = $1
                  AND cl.scope_type = 'event'
                  AND cl.surface_name = ANY($2::text[])
                  AND cl.freshness_status = ANY($3::text[])
                  AND cl.sport_slug = $4
                ORDER BY cl.last_checked_at DESC NULLS LAST, cl.scope_id DESC, cl.surface_name ASC
                """,
                source_slug,
                normalized_surface_names,
                normalized_statuses,
                sport_slug,
            )
        else:
            rows = await executor.fetch(
                """
                SELECT
                    cl.scope_id,
                    cl.surface_name,
                    cl.freshness_status,
                    e.start_timestamp
                FROM coverage_ledger AS cl
                LEFT JOIN event AS e ON e.id = cl.scope_id
                WHERE cl.source_slug = $1
                  AND cl.scope_type = 'event'
                  AND cl.surface_name = ANY($2::text[])
                  AND cl.freshness_status = ANY($3::text[])
                ORDER BY cl.last_checked_at DESC NULLS LAST, cl.scope_id DESC, cl.surface_name ASC
                """,
                source_slug,
                normalized_surface_names,
                normalized_statuses,
            )
        records: list[CoverageEventScopeStatus] = []
        for row in rows:
            if isinstance(row, dict):
                scope_id = row.get("scope_id")
                surface_name = row.get("surface_name")
                freshness_status = row.get("freshness_status")
                start_timestamp = row.get("start_timestamp")
            else:
                scope_id, surface_name, freshness_status, start_timestamp = row
            if scope_id is None or surface_name is None or freshness_status is None:
                continue
            records.append(
                CoverageEventScopeStatus(
                    scope_id=int(scope_id),
                    surface_name=str(surface_name),
                    freshness_status=str(freshness_status),
                    start_timestamp=int(start_timestamp) if isinstance(start_timestamp, int) else None,
                )
            )
        return tuple(records)
