"""PostgreSQL repository for raw transport and snapshot control-plane tables."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Protocol

from ..endpoints import EndpointRegistryEntry
from ..parsers.base import RawSnapshot
from ._temporal import coerce_timestamptz


class SqlExecutor(Protocol):
    async def execute(self, query: str, *args: object) -> Any: ...


class SqlReturningExecutor(SqlExecutor, Protocol):
    async def fetchval(self, query: str, *args: object) -> Any: ...


class SqlFetchExecutor(SqlExecutor, Protocol):
    async def fetchrow(self, query: str, *args: object) -> Any: ...


class SqlBatchExecutor(SqlExecutor, Protocol):
    async def executemany(self, query: str, args: Iterable[tuple[object, ...]]) -> Any: ...


@dataclass(frozen=True)
class ApiRequestLogRecord:
    trace_id: str | None
    job_id: str | None
    job_type: str | None
    sport_slug: str | None
    method: str
    source_url: str
    endpoint_pattern: str
    request_headers_redacted: Mapping[str, object] | None
    query_params: Mapping[str, object] | None
    proxy_id: str | None
    transport_attempt: int | None
    http_status: int | None
    challenge_reason: str | None
    started_at: str
    finished_at: str | None
    latency_ms: int | None


@dataclass(frozen=True)
class PayloadSnapshotRecord:
    trace_id: str | None
    job_id: str | None
    sport_slug: str | None
    endpoint_pattern: str
    source_url: str
    resolved_url: str | None
    envelope_key: str
    context_entity_type: str | None
    context_entity_id: int | None
    context_unique_tournament_id: int | None
    context_season_id: int | None
    context_event_id: int | None
    http_status: int | None
    payload: object
    payload_hash: str | None
    payload_size_bytes: int | None
    content_type: str | None
    is_valid_json: bool
    is_soft_error_payload: bool
    fetched_at: str | None


@dataclass(frozen=True)
class ApiSnapshotHeadRecord:
    endpoint_pattern: str
    context_entity_type: str | None
    context_entity_id: int | None
    scope_key: str
    latest_snapshot_id: int
    latest_payload_hash: str | None
    latest_fetched_at: str | None


class RawRepository:
    """Writes raw request and snapshot metadata for hybrid ETL control-plane flows."""

    async def upsert_endpoint_registry_entries(
        self,
        executor: SqlExecutor,
        entries: Iterable[EndpointRegistryEntry],
    ) -> None:
        rows = [
            (
                item.pattern,
                item.path_template,
                item.query_template,
                item.envelope_key,
                item.target_table,
                item.notes,
            )
            for item in entries
        ]
        if not rows:
            return
        query = """
            INSERT INTO endpoint_registry (pattern, path_template, query_template, envelope_key, target_table, notes)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (pattern) DO UPDATE SET
                path_template = EXCLUDED.path_template,
                query_template = EXCLUDED.query_template,
                envelope_key = EXCLUDED.envelope_key,
                target_table = EXCLUDED.target_table,
                notes = EXCLUDED.notes
        """
        executemany = getattr(executor, "executemany", None)
        if callable(executemany):
            await executemany(query, rows)
            return
        for row in rows:
            await executor.execute(query, *row)

    async def insert_request_log(self, executor: SqlExecutor, record: ApiRequestLogRecord) -> None:
        # Intentionally append-only: retries must remain visible as separate transport
        # attempts for observability and proxy/upstream debugging. Only payload snapshots
        # and snapshot heads are retry-deduplicated.
        await executor.execute(
            """
            INSERT INTO api_request_log (
                trace_id,
                job_id,
                job_type,
                sport_slug,
                method,
                source_url,
                endpoint_pattern,
                request_headers_redacted,
                query_params,
                proxy_id,
                transport_attempt,
                http_status,
                challenge_reason,
                started_at,
                finished_at,
                latency_ms
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8::jsonb, $9::jsonb, $10, $11, $12, $13, $14, $15, $16
            )
            """,
            record.trace_id,
            record.job_id,
            record.job_type,
            record.sport_slug,
            record.method,
            record.source_url,
            record.endpoint_pattern,
            _jsonb(record.request_headers_redacted),
            _jsonb(record.query_params),
            record.proxy_id,
            record.transport_attempt,
            record.http_status,
            record.challenge_reason,
            coerce_timestamptz(record.started_at),
            coerce_timestamptz(record.finished_at),
            record.latency_ms,
        )

    async def insert_payload_snapshot(self, executor: SqlExecutor, record: PayloadSnapshotRecord) -> None:
        await executor.execute(
            """
            INSERT INTO api_payload_snapshot (
                endpoint_pattern,
                source_url,
                resolved_url,
                envelope_key,
                context_entity_type,
                context_entity_id,
                context_unique_tournament_id,
                context_season_id,
                context_event_id,
                payload,
                fetched_at,
                trace_id,
                job_id,
                sport_slug,
                http_status,
                payload_hash,
                payload_size_bytes,
                content_type,
                is_valid_json,
                is_soft_error_payload
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10::jsonb, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
            )
            """,
            record.endpoint_pattern,
            record.source_url,
            record.resolved_url,
            record.envelope_key,
            record.context_entity_type,
            record.context_entity_id,
            record.context_unique_tournament_id,
            record.context_season_id,
            record.context_event_id,
            _jsonb(record.payload),
            coerce_timestamptz(record.fetched_at),
            record.trace_id,
            record.job_id,
            record.sport_slug,
            record.http_status,
            record.payload_hash,
            record.payload_size_bytes,
            record.content_type,
            record.is_valid_json,
            record.is_soft_error_payload,
        )

    async def insert_payload_snapshot_returning_id(
        self,
        executor: SqlExecutor,
        record: PayloadSnapshotRecord,
    ) -> int | None:
        return await self.insert_payload_snapshot_if_missing_returning_id(executor, record)

    async def insert_payload_snapshot_if_missing_returning_id(
        self,
        executor: SqlExecutor,
        record: PayloadSnapshotRecord,
    ) -> int | None:
        # Single-roundtrip, race-free upsert keyed by (scope_key, payload_hash).
        #
        # Relies on the partial unique index
        #   uniq_api_payload_snapshot_scope_hash
        #     ON api_payload_snapshot (scope_key, payload_hash)
        #     WHERE scope_key IS NOT NULL AND payload_hash IS NOT NULL
        # created by migration 2026-04-20_api_payload_snapshot_scope_hash_uniq.sql.
        #
        # The ON CONFLICT DO UPDATE sets scope_key to its own value; this is a
        # deliberate no-op assignment used purely to force Postgres to return
        # the surviving row's id via RETURNING on conflict (DO NOTHING would
        # suppress RETURNING when the row already exists).
        scope_key = _snapshot_scope_key(record)
        query = """
            INSERT INTO api_payload_snapshot (
                endpoint_pattern,
                source_url,
                resolved_url,
                envelope_key,
                context_entity_type,
                context_entity_id,
                context_unique_tournament_id,
                context_season_id,
                context_event_id,
                payload,
                fetched_at,
                trace_id,
                job_id,
                sport_slug,
                http_status,
                payload_hash,
                payload_size_bytes,
                content_type,
                is_valid_json,
                is_soft_error_payload,
                scope_key
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10::jsonb, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20,
                $21
            )
            ON CONFLICT (scope_key, payload_hash)
                WHERE scope_key IS NOT NULL AND payload_hash IS NOT NULL
                DO UPDATE SET scope_key = EXCLUDED.scope_key
            RETURNING id
        """
        args = (
            record.endpoint_pattern,
            record.source_url,
            record.resolved_url,
            record.envelope_key,
            record.context_entity_type,
            record.context_entity_id,
            record.context_unique_tournament_id,
            record.context_season_id,
            record.context_event_id,
            _jsonb(record.payload),
            coerce_timestamptz(record.fetched_at),
            record.trace_id,
            record.job_id,
            record.sport_slug,
            record.http_status,
            record.payload_hash,
            record.payload_size_bytes,
            record.content_type,
            record.is_valid_json,
            record.is_soft_error_payload,
            scope_key,
        )
        fetchval = getattr(executor, "fetchval", None)
        if callable(fetchval):
            result = await fetchval(query, *args)
            return int(result) if result is not None else None
        await executor.execute(query, *args)
        return None

    async def upsert_snapshot_head(self, executor: SqlExecutor, record: ApiSnapshotHeadRecord) -> None:
        await executor.execute(
            """
            INSERT INTO api_snapshot_head (
                scope_key,
                endpoint_pattern,
                context_entity_type,
                context_entity_id,
                latest_snapshot_id,
                latest_payload_hash,
                latest_fetched_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (scope_key) DO UPDATE SET
                endpoint_pattern = EXCLUDED.endpoint_pattern,
                context_entity_type = EXCLUDED.context_entity_type,
                context_entity_id = EXCLUDED.context_entity_id,
                latest_snapshot_id = EXCLUDED.latest_snapshot_id,
                latest_payload_hash = EXCLUDED.latest_payload_hash,
                latest_fetched_at = EXCLUDED.latest_fetched_at
            """,
            record.scope_key,
            record.endpoint_pattern,
            record.context_entity_type,
            record.context_entity_id,
            record.latest_snapshot_id,
            record.latest_payload_hash,
            coerce_timestamptz(record.latest_fetched_at),
        )

    async def fetch_payload_snapshot(self, executor: SqlFetchExecutor, snapshot_id: int) -> RawSnapshot:
        row = await executor.fetchrow(
            """
            SELECT
                id,
                endpoint_pattern,
                sport_slug,
                source_url,
                resolved_url,
                envelope_key,
                http_status,
                payload,
                fetched_at,
                context_entity_type,
                context_entity_id,
                context_unique_tournament_id,
                context_season_id,
                context_event_id
            FROM api_payload_snapshot
            WHERE id = $1
            """,
            snapshot_id,
        )
        if row is None:
            raise KeyError(snapshot_id)
        return RawSnapshot(
            snapshot_id=int(row["id"]),
            endpoint_pattern=str(row["endpoint_pattern"]),
            sport_slug=str(row["sport_slug"] or ""),
            source_url=str(row["source_url"]),
            resolved_url=str(row["resolved_url"] or row["source_url"]),
            envelope_key=str(row["envelope_key"] or "payload"),
            http_status=int(row["http_status"]) if row["http_status"] is not None else None,
            payload=row["payload"],
            fetched_at=str(row["fetched_at"] or ""),
            context_entity_type=row["context_entity_type"],
            context_entity_id=_maybe_int(row["context_entity_id"]),
            context_unique_tournament_id=_maybe_int(row["context_unique_tournament_id"]),
            context_season_id=_maybe_int(row["context_season_id"]),
            context_event_id=_maybe_int(row["context_event_id"]),
        )


def _jsonb(value: object) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _maybe_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _snapshot_scope_key(record: PayloadSnapshotRecord) -> str:
    if record.context_entity_type and record.context_entity_id is not None:
        return f"{record.context_entity_type}:{record.context_entity_id}:{record.endpoint_pattern}"
    return record.endpoint_pattern
