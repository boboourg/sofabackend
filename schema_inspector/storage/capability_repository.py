"""PostgreSQL repository for endpoint capability observations and rollups."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Mapping, Protocol

import orjson

from ._temporal import coerce_timestamptz


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...
    async def fetchrow(self, query: str, *args: object) -> Any: ...


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
        merged_record = record
        fetchrow = getattr(executor, "fetchrow", None)
        if callable(fetchrow):
            existing = await fetchrow(
                """
                SELECT
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
                FROM endpoint_capability_rollup
                WHERE sport_slug = $1 AND endpoint_pattern = $2
                """,
                record.sport_slug,
                record.endpoint_pattern,
            )
            if existing:
                merged_record = _merge_rollup_record(existing, record)

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
            merged_record.sport_slug,
            merged_record.endpoint_pattern,
            merged_record.support_level,
            merged_record.confidence,
            coerce_timestamptz(merged_record.last_success_at),
            coerce_timestamptz(merged_record.last_404_at),
            coerce_timestamptz(merged_record.last_soft_error_at),
            merged_record.success_count,
            merged_record.not_found_count,
            merged_record.soft_error_count,
            merged_record.empty_count,
            merged_record.notes,
        )


    async def rebuild_rollups_from_observations(
        self,
        executor: SqlExecutor,
        *,
        sport_slug: str | None = None,
        lookback_days: int | None = None,
    ) -> int:
        """Replace ``endpoint_capability_rollup`` rows from observation aggregates.

        Out-of-band batch rebuilder used by the ``rebuild-capability-rollup``
        CLI command (and any future scheduled maintenance). Reads aggregates
        directly from ``endpoint_capability_observation`` and writes the
        canonical state per ``(sport_slug, endpoint_pattern)`` key.

        IMPORTANT: this does **not** use the legacy ``upsert_rollup`` path.
        That path is a read-modify-write race against a small PK space hit
        by many concurrent live workers. Here we *replace* rollup state from
        aggregate values computed in SQL so the write is a single
        ``INSERT ... ON CONFLICT DO UPDATE`` per key with deterministic key
        ordering. Counts are taken from the observation history, **not**
        accumulated on top of existing rollup counts -- this is the explicit
        firebreak contract (replace-from-aggregate, not merge).

        Optional filters:

        - ``sport_slug`` -- restrict rebuild to one sport slug.
        - ``lookback_days`` -- only aggregate observations within the last
          ``N`` days. ``None`` aggregates the full observation history.

        Returns the number of rollup rows written.
        """

        fetch = getattr(executor, "fetch", None)
        if not callable(fetch):
            raise TypeError("executor must support .fetch() for batch rebuild")

        # Aggregate counts + latest phase timestamps + earliest probe seen,
        # filtered by optional sport_slug / lookback window. We rely on the
        # observation table's natural append-only semantics: counts are the
        # raw histogram of per-fetch outcomes.
        where_clauses: list[str] = []
        params: list[object] = []
        if sport_slug is not None:
            params.append(sport_slug)
            where_clauses.append(f"sport_slug = ${len(params)}")
        if lookback_days is not None:
            params.append(int(lookback_days))
            where_clauses.append(
                f"observed_at >= (NOW() - make_interval(days => ${len(params)}::int))"
            )
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

        aggregate_query = f"""
            SELECT
                sport_slug,
                endpoint_pattern,
                COUNT(*) FILTER (
                    WHERE http_status BETWEEN 200 AND 299
                      AND COALESCE(is_soft_error_payload, FALSE) = FALSE
                ) AS success_count,
                COUNT(*) FILTER (
                    WHERE http_status = 404
                      AND COALESCE(is_soft_error_payload, FALSE) = FALSE
                ) AS not_found_count,
                COUNT(*) FILTER (WHERE COALESCE(is_soft_error_payload, FALSE) = TRUE)
                    AS soft_error_count,
                COUNT(*) FILTER (WHERE COALESCE(is_empty_payload, FALSE) = TRUE)
                    AS empty_count,
                MAX(observed_at) FILTER (
                    WHERE http_status BETWEEN 200 AND 299
                      AND COALESCE(is_soft_error_payload, FALSE) = FALSE
                ) AS last_success_at,
                MAX(observed_at) FILTER (
                    WHERE http_status = 404
                      AND COALESCE(is_soft_error_payload, FALSE) = FALSE
                ) AS last_404_at,
                MAX(observed_at) FILTER (
                    WHERE COALESCE(is_soft_error_payload, FALSE) = TRUE
                ) AS last_soft_error_at
            FROM endpoint_capability_observation
            {where_sql}
            GROUP BY sport_slug, endpoint_pattern
            ORDER BY sport_slug, endpoint_pattern
        """

        aggregate_rows = await fetch(aggregate_query, *params)

        rebuilt = 0
        for row in aggregate_rows:
            row_sport_slug = str(row["sport_slug"])
            row_endpoint_pattern = str(row["endpoint_pattern"])
            success_count = int(row["success_count"] or 0)
            not_found_count = int(row["not_found_count"] or 0)
            soft_error_count = int(row["soft_error_count"] or 0)
            empty_count = int(row["empty_count"] or 0)
            total = success_count + not_found_count + soft_error_count

            if success_count > 0 and not_found_count == 0 and soft_error_count == 0:
                support_level = "supported"
            elif success_count == 0 and not_found_count > 0 and soft_error_count == 0:
                support_level = "unsupported"
            elif success_count > 0 or soft_error_count > 0:
                support_level = "conditionally_supported"
            else:
                support_level = "unknown"

            confidence = min(1.0, max(total, 1) / 3.0)

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
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULL)
                ON CONFLICT (sport_slug, endpoint_pattern) DO UPDATE SET
                    support_level = EXCLUDED.support_level,
                    confidence = EXCLUDED.confidence,
                    last_success_at = EXCLUDED.last_success_at,
                    last_404_at = EXCLUDED.last_404_at,
                    last_soft_error_at = EXCLUDED.last_soft_error_at,
                    success_count = EXCLUDED.success_count,
                    not_found_count = EXCLUDED.not_found_count,
                    soft_error_count = EXCLUDED.soft_error_count,
                    empty_count = EXCLUDED.empty_count
                """,
                row_sport_slug,
                row_endpoint_pattern,
                support_level,
                confidence,
                coerce_timestamptz(row["last_success_at"]),
                coerce_timestamptz(row["last_404_at"]),
                coerce_timestamptz(row["last_soft_error_at"]),
                success_count,
                not_found_count,
                soft_error_count,
                empty_count,
            )
            rebuilt += 1

        return rebuilt


def _jsonb(value: object) -> str | None:
    if value is None:
        return None
    return orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode("utf-8")


def _merge_rollup_record(existing: Mapping[str, Any], incoming: CapabilityRollupRecord) -> CapabilityRollupRecord:
    success_count = int(existing.get("success_count") or 0) + incoming.success_count
    not_found_count = int(existing.get("not_found_count") or 0) + incoming.not_found_count
    soft_error_count = int(existing.get("soft_error_count") or 0) + incoming.soft_error_count
    empty_count = int(existing.get("empty_count") or 0) + incoming.empty_count
    total = success_count + not_found_count + soft_error_count

    if success_count > 0 and not_found_count == 0 and soft_error_count == 0:
        support_level = "supported"
    elif success_count == 0 and not_found_count > 0 and soft_error_count == 0:
        support_level = "unsupported"
    elif success_count > 0 or soft_error_count > 0:
        support_level = "conditionally_supported"
    else:
        support_level = "unknown"

    return CapabilityRollupRecord(
        sport_slug=incoming.sport_slug,
        endpoint_pattern=incoming.endpoint_pattern,
        support_level=support_level,
        confidence=min(1.0, max(total, 1) / 3.0),
        last_success_at=_later_timestamp(existing.get("last_success_at"), incoming.last_success_at),
        last_404_at=_later_timestamp(existing.get("last_404_at"), incoming.last_404_at),
        last_soft_error_at=_later_timestamp(existing.get("last_soft_error_at"), incoming.last_soft_error_at),
        success_count=success_count,
        not_found_count=not_found_count,
        soft_error_count=soft_error_count,
        empty_count=empty_count,
        notes=incoming.notes or _normalize_text(existing.get("notes")),
    )


def _later_timestamp(left: object, right: str | None) -> str | None:
    left_source = left if isinstance(left, (str, datetime)) or left is None else str(left)
    left_dt = coerce_timestamptz(left_source)
    right_dt = coerce_timestamptz(right)
    if left_dt is None:
        return right_dt.isoformat() if right_dt is not None else None
    if right_dt is None:
        return left_dt.isoformat()
    return max(left_dt, right_dt).isoformat()


def _normalize_text(value: object) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None
