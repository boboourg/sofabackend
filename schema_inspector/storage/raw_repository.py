"""PostgreSQL repository for raw transport and snapshot control-plane tables."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Protocol

import orjson

from ..db import register_post_commit_hook
from ..endpoints import EndpointRegistryEntry
from ..write_avoidance_cache import ExpiringValueCache
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
    attempts_json: object | None = None
    payload_bytes: int | None = None
    error_message: str | None = None
    proxy_address: str | None = None


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
    source_slug: str = "sofascore"
    schema_fingerprint: str | None = None
    scope_hash: str | None = None


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

    _SERVING_SOURCE_SLUG = "sofascore"

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
                item.source_slug,
                item.contract_version,
            )
            for item in entries
        ]
        rows = _normalize_registry_rows(rows)
        if not rows:
            return
        cache_enabled = getattr(executor, "_enable_registry_sync_cache", True)
        contract_rows = rows
        serving_rows = [row for row in rows if row[6] == self._SERVING_SOURCE_SLUG]
        if cache_enabled:
            with _REGISTRY_SYNC_CACHE_LOCK:
                contract_rows = [
                    row
                    for row in rows
                    if _REGISTRY_CONTRACT_ROW_CACHE.get(_contract_registry_cache_key(row))
                    != _contract_registry_cache_value(row)
                ]
                serving_rows = [
                    row
                    for row in serving_rows
                    if _REGISTRY_SERVING_ROW_CACHE.get(_serving_registry_cache_key(row))
                    != _serving_registry_cache_value(row)
                ]
        if not contract_rows and not serving_rows:
            return
        contract_query = """
            INSERT INTO endpoint_contract_registry (
                pattern,
                path_template,
                query_template,
                envelope_key,
                target_table,
                notes,
                source_slug,
                contract_version
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (pattern, source_slug) DO UPDATE SET
                path_template = EXCLUDED.path_template,
                query_template = EXCLUDED.query_template,
                envelope_key = EXCLUDED.envelope_key,
                target_table = EXCLUDED.target_table,
                notes = EXCLUDED.notes,
                contract_version = EXCLUDED.contract_version
            WHERE endpoint_contract_registry.path_template IS DISTINCT FROM EXCLUDED.path_template
               OR endpoint_contract_registry.query_template IS DISTINCT FROM EXCLUDED.query_template
               OR endpoint_contract_registry.envelope_key IS DISTINCT FROM EXCLUDED.envelope_key
               OR endpoint_contract_registry.target_table IS DISTINCT FROM EXCLUDED.target_table
               OR endpoint_contract_registry.notes IS DISTINCT FROM EXCLUDED.notes
               OR endpoint_contract_registry.contract_version IS DISTINCT FROM EXCLUDED.contract_version
        """
        serving_query = """
            INSERT INTO endpoint_registry (
                pattern,
                path_template,
                query_template,
                envelope_key,
                target_table,
                notes,
                source_slug,
                contract_version
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (pattern) DO UPDATE SET
                path_template = EXCLUDED.path_template,
                query_template = EXCLUDED.query_template,
                envelope_key = EXCLUDED.envelope_key,
                target_table = EXCLUDED.target_table,
                notes = EXCLUDED.notes,
                source_slug = EXCLUDED.source_slug,
                contract_version = EXCLUDED.contract_version
            WHERE endpoint_registry.path_template IS DISTINCT FROM EXCLUDED.path_template
               OR endpoint_registry.query_template IS DISTINCT FROM EXCLUDED.query_template
               OR endpoint_registry.envelope_key IS DISTINCT FROM EXCLUDED.envelope_key
               OR endpoint_registry.target_table IS DISTINCT FROM EXCLUDED.target_table
               OR endpoint_registry.notes IS DISTINCT FROM EXCLUDED.notes
               OR endpoint_registry.source_slug IS DISTINCT FROM EXCLUDED.source_slug
               OR endpoint_registry.contract_version IS DISTINCT FROM EXCLUDED.contract_version
        """
        executemany = getattr(executor, "executemany", None)
        if callable(executemany):
            if contract_rows:
                await executemany(contract_query, contract_rows)
            if serving_rows:
                await executemany(serving_query, serving_rows)
            if cache_enabled:
                self._register_registry_rows(contract_rows, serving_rows)
            return
        for row in contract_rows:
            await executor.execute(contract_query, *row)
        for row in serving_rows:
            await executor.execute(serving_query, *row)
        if cache_enabled:
            self._register_registry_rows(contract_rows, serving_rows)

    def _register_registry_rows(
        self,
        contract_rows: Iterable[tuple[str | None, ...]],
        serving_rows: Iterable[tuple[str | None, ...]],
    ) -> None:
        resolved_contract_rows = tuple(contract_rows)
        resolved_serving_rows = tuple(serving_rows)

        def _commit_cache_entry() -> None:
            with _REGISTRY_SYNC_CACHE_LOCK:
                for row in resolved_contract_rows:
                    _REGISTRY_CONTRACT_ROW_CACHE.set(
                        _contract_registry_cache_key(row),
                        _contract_registry_cache_value(row),
                    )
                for row in resolved_serving_rows:
                    _REGISTRY_SERVING_ROW_CACHE.set(
                        _serving_registry_cache_key(row),
                        _serving_registry_cache_value(row),
                    )

        if not register_post_commit_hook(_commit_cache_entry):
            _commit_cache_entry()

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
                proxy_address,
                transport_attempt,
                http_status,
                challenge_reason,
                started_at,
                finished_at,
                latency_ms,
                attempts_json,
                payload_bytes,
                error_message
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7,
                $8::jsonb, $9::jsonb, $10, $11, $12, $13, $14, $15, $16,
                $17, $18::jsonb, $19, $20
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
            record.proxy_address,
            record.transport_attempt,
            record.http_status,
            record.challenge_reason,
            coerce_timestamptz(record.started_at),
            coerce_timestamptz(record.finished_at),
            record.latency_ms,
            _jsonb(record.attempts_json),
            record.payload_bytes,
            record.error_message,
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
                is_soft_error_payload,
                source_slug,
                schema_fingerprint,
                scope_hash
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10::jsonb, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23
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
            record.source_slug,
            record.schema_fingerprint,
            record.scope_hash,
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
                scope_key,
                source_slug,
                schema_fingerprint,
                scope_hash
            )
            VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10::jsonb, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21,
                $22, $23, $24
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
            record.source_slug,
            record.schema_fingerprint,
            record.scope_hash,
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
    return orjson.dumps(value, option=orjson.OPT_SORT_KEYS).decode("utf-8")


_REGISTRY_CONTRACT_ROW_CACHE = ExpiringValueCache[
    tuple[str | None, str | None],
    tuple[str | None, str | None, str | None, str | None, str | None, str | None],
](ttl_seconds=1800.0)
_REGISTRY_SERVING_ROW_CACHE = ExpiringValueCache[
    str | None,
    tuple[str | None, str | None, str | None, str | None, str | None, str | None, str | None],
](ttl_seconds=1800.0)
_REGISTRY_SYNC_CACHE_LOCK = threading.Lock()


def reset_all_registry_sync_caches() -> None:
    with _REGISTRY_SYNC_CACHE_LOCK:
        _REGISTRY_CONTRACT_ROW_CACHE.clear()
        _REGISTRY_SERVING_ROW_CACHE.clear()


def _normalize_registry_rows(
    rows: Iterable[tuple[str | None, str | None, str | None, str | None, str | None, str | None, str | None, str | None]]
) -> list[tuple[str | None, str | None, str | None, str | None, str | None, str | None, str | None, str | None]]:
    unique_rows = dict.fromkeys(rows)
    return sorted(
        unique_rows,
        key=lambda row: tuple("" if value is None else value for value in row),
    )


def _contract_registry_cache_key(
    row: tuple[str | None, str | None, str | None, str | None, str | None, str | None, str | None, str | None]
) -> tuple[str | None, str | None]:
    return row[0], row[6]


def _contract_registry_cache_value(
    row: tuple[str | None, str | None, str | None, str | None, str | None, str | None, str | None, str | None]
) -> tuple[str | None, str | None, str | None, str | None, str | None, str | None]:
    return row[1], row[2], row[3], row[4], row[5], row[7]


def _serving_registry_cache_key(
    row: tuple[str | None, str | None, str | None, str | None, str | None, str | None, str | None, str | None]
) -> str | None:
    return row[0]


def _serving_registry_cache_value(
    row: tuple[str | None, str | None, str | None, str | None, str | None, str | None, str | None, str | None]
) -> tuple[str | None, str | None, str | None, str | None, str | None, str | None, str | None]:
    return row[1], row[2], row[3], row[4], row[5], row[6], row[7]


def _maybe_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def _snapshot_scope_key(record: PayloadSnapshotRecord) -> str:
    if record.context_entity_type and record.context_entity_id is not None:
        return f"{record.source_slug}:{record.context_entity_type}:{record.context_entity_id}:{record.endpoint_pattern}"
    return f"{record.source_slug}:{record.endpoint_pattern}"
