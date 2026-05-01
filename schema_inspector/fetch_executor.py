"""Shared fetch executor that bridges transport, classification, and raw storage."""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

from .fetch_classifier import (
    CLASSIFICATION_SOFT_ERROR_JSON,
    CLASSIFICATION_SUCCESS_EMPTY_JSON,
    CLASSIFICATION_SUCCESS_JSON,
    classify_fetch_result,
)
from .fetch_models import FetchOutcomeEnvelope, FetchTask
from .parsers.base import RawSnapshot
from .runtime import TransportAttempt
from .storage.raw_repository import ApiRequestLogRecord, ApiSnapshotHeadRecord, PayloadSnapshotRecord, RawRepository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PrefetchedFetchRecord:
    task: FetchTask
    outcome: FetchOutcomeEnvelope
    request_log: ApiRequestLogRecord
    payload_snapshot: PayloadSnapshotRecord | None
    snapshot_head: ApiSnapshotHeadRecord | None


class FetchExecutor:
    """Executes one fetch task through the shared transport and writes raw control-plane records."""

    def __init__(
        self,
        *,
        transport: Any,
        raw_repository: RawRepository,
        sql_executor: Any,
        snapshot_store=None,
        write_mode: str = "immediate",
        clock=None,
        freshness_store=None,
    ) -> None:
        self.transport = transport
        self.raw_repository = raw_repository
        self.sql_executor = sql_executor
        self.snapshot_store = snapshot_store
        self.write_mode = str(write_mode or "immediate").strip().lower()
        self.clock = clock or time.monotonic
        self.freshness_store = freshness_store
        self._prefetched_records: list[PrefetchedFetchRecord] = []

    @property
    def prefetched_records(self) -> tuple[PrefetchedFetchRecord, ...]:
        return tuple(self._prefetched_records)

    async def commit_prefetched_record(self, record: PrefetchedFetchRecord) -> FetchOutcomeEnvelope:
        if self.sql_executor is None:
            raise RuntimeError("sql_executor is required to commit prefetched raw records")
        await self.raw_repository.insert_request_log(self.sql_executor, record.request_log)
        snapshot_id: int | None = None
        if record.payload_snapshot is not None:
            snapshot_id = await self.raw_repository.insert_payload_snapshot_if_missing_returning_id(
                self.sql_executor,
                record.payload_snapshot,
            )
        if snapshot_id is not None and record.snapshot_head is not None:
            await self.raw_repository.upsert_snapshot_head(
                self.sql_executor,
                replace(record.snapshot_head, latest_snapshot_id=snapshot_id),
            )
        return replace(record.outcome, snapshot_id=snapshot_id)

    async def execute(self, task: FetchTask) -> FetchOutcomeEnvelope:
        started_monotonic = self.clock()
        started_at = _utc_now()
        try:
            transport_result = await self.transport.fetch(
                task.source_url,
                headers=dict(task.request_headers or {}),
                timeout=task.timeout_seconds,
            )
        except Exception as exc:
            finished_at = _utc_now()
            latency_ms = int((self.clock() - started_monotonic) * 1000)
            request_log = ApiRequestLogRecord(
                trace_id=task.trace_id,
                job_id=task.job_id,
                job_type=task.fetch_reason,
                sport_slug=task.sport_slug,
                method=task.method,
                source_url=task.source_url,
                endpoint_pattern=task.endpoint_pattern,
                request_headers_redacted=task.request_headers,
                query_params=task.query_params,
                proxy_id=None,
                proxy_address=None,
                transport_attempt=None,
                http_status=None,
                challenge_reason=None,
                started_at=started_at,
                finished_at=finished_at,
                latency_ms=latency_ms,
                attempts_json=None,
                payload_bytes=None,
                error_message=str(exc),
            )
            outcome = FetchOutcomeEnvelope(
                trace_id=task.trace_id,
                job_id=task.job_id,
                endpoint_pattern=task.endpoint_pattern,
                source_url=task.source_url,
                resolved_url=None,
                http_status=None,
                classification="network_error",
                proxy_id=None,
                challenge_reason=None,
                snapshot_id=None,
                payload_hash=None,
                retry_recommended=True,
                capability_signal="transient",
                attempts=(),
                fetched_at=finished_at,
                error_message=str(exc),
            )
            if self.write_mode == "deferred":
                self._prefetched_records.append(
                    PrefetchedFetchRecord(
                        task=task,
                        outcome=outcome,
                        request_log=request_log,
                        payload_snapshot=None,
                        snapshot_head=None,
                    )
                )
                return outcome
            await self.raw_repository.insert_request_log(self.sql_executor, request_log)
            return outcome

        classified = classify_fetch_result(transport_result)
        finished_at = _utc_now()
        latency_ms = int((self.clock() - started_monotonic) * 1000)
        request_log = ApiRequestLogRecord(
            trace_id=task.trace_id,
            job_id=task.job_id,
            job_type=task.fetch_reason,
            sport_slug=task.sport_slug,
            method=task.method,
            source_url=task.source_url,
            endpoint_pattern=task.endpoint_pattern,
            request_headers_redacted=task.request_headers,
            query_params=task.query_params,
            proxy_id=transport_result.final_proxy_name,
            proxy_address=transport_result.final_proxy_address,
            transport_attempt=len(transport_result.attempts),
            http_status=transport_result.status_code,
            challenge_reason=transport_result.challenge_reason,
            started_at=started_at,
            finished_at=finished_at,
            latency_ms=latency_ms,
            attempts_json=_attempts_json(transport_result.attempts),
            payload_bytes=len(transport_result.body_bytes),
            error_message=None,
        )

        payload_hash = hashlib.sha256(transport_result.body_bytes).hexdigest() if transport_result.body_bytes else None
        snapshot_id: int | None = None
        payload_snapshot: PayloadSnapshotRecord | None = None
        snapshot_head: ApiSnapshotHeadRecord | None = None
        if transport_result.body_bytes:
            payload_snapshot = PayloadSnapshotRecord(
                trace_id=task.trace_id,
                job_id=task.job_id,
                sport_slug=task.sport_slug,
                endpoint_pattern=task.endpoint_pattern,
                source_url=task.source_url,
                resolved_url=transport_result.resolved_url,
                envelope_key=task.endpoint_pattern.split("/")[-1] if task.endpoint_pattern else "payload",
                context_entity_type=task.context_entity_type,
                context_entity_id=task.context_entity_id,
                context_unique_tournament_id=task.context_unique_tournament_id,
                context_season_id=task.context_season_id,
                context_event_id=task.context_event_id,
                http_status=transport_result.status_code,
                payload=classified.payload if classified.is_valid_json else {"raw": transport_result.body_bytes.decode("utf-8", errors="ignore")},
                payload_hash=payload_hash,
                payload_size_bytes=len(transport_result.body_bytes),
                content_type=classified.content_type,
                is_valid_json=classified.is_valid_json,
                is_soft_error_payload=classified.is_soft_error_payload,
                fetched_at=finished_at,
            )
            if self.write_mode == "deferred":
                if self.snapshot_store is None:
                    raise RuntimeError("snapshot_store is required when write_mode='deferred'")
                snapshot_id = self.snapshot_store.stage_snapshot(payload_snapshot)
            else:
                await self.raw_repository.insert_request_log(self.sql_executor, request_log)
                snapshot_id = await self.raw_repository.insert_payload_snapshot_if_missing_returning_id(
                    self.sql_executor,
                    payload_snapshot,
                )
        elif self.write_mode != "deferred":
            await self.raw_repository.insert_request_log(self.sql_executor, request_log)

        if snapshot_id is not None and classified.classification in {
            CLASSIFICATION_SUCCESS_JSON,
            CLASSIFICATION_SUCCESS_EMPTY_JSON,
            CLASSIFICATION_SOFT_ERROR_JSON,
        }:
            snapshot_head = ApiSnapshotHeadRecord(
                endpoint_pattern=task.endpoint_pattern,
                context_entity_type=task.context_entity_type,
                context_entity_id=task.context_entity_id,
                scope_key=_scope_key(task),
                latest_snapshot_id=snapshot_id,
                latest_payload_hash=payload_hash,
                latest_fetched_at=finished_at,
            )
            if self.write_mode != "deferred":
                await self.raw_repository.upsert_snapshot_head(
                    self.sql_executor,
                    snapshot_head,
                )

        outcome = FetchOutcomeEnvelope(
            trace_id=task.trace_id,
            job_id=task.job_id,
            endpoint_pattern=task.endpoint_pattern,
            source_url=task.source_url,
            resolved_url=transport_result.resolved_url,
            http_status=transport_result.status_code,
            classification=classified.classification,
            proxy_id=transport_result.final_proxy_name,
            challenge_reason=transport_result.challenge_reason,
            snapshot_id=snapshot_id,
            payload_hash=payload_hash,
            payload_root_keys=classified.payload_root_keys,
            is_valid_json=classified.is_valid_json,
            is_empty_payload=classified.is_empty_payload,
            is_soft_error_payload=classified.is_soft_error_payload,
            retry_recommended=classified.retry_recommended,
            capability_signal=classified.capability_signal,
            attempts=tuple(transport_result.attempts),
            fetched_at=finished_at,
            error_message=None,
        )
        self._mark_freshness_if_success(task, outcome)
        if self.write_mode == "deferred":
            self._prefetched_records.append(
                PrefetchedFetchRecord(
                    task=task,
                    outcome=outcome,
                    request_log=request_log,
                    payload_snapshot=payload_snapshot,
                    snapshot_head=snapshot_head,
                )
            )
        return outcome

    def _mark_freshness_if_success(self, task: FetchTask, outcome: FetchOutcomeEnvelope) -> None:
        if (
            self.freshness_store is None
            or outcome.http_status != 200
            or not task.freshness_key
            or task.freshness_ttl_seconds is None
        ):
            return
        try:
            self.freshness_store.mark_fetched(task.freshness_key, task.freshness_ttl_seconds)
        except Exception as exc:
            logger.warning("FreshnessStore.mark_fetched failed from FetchExecutor: %s", exc)


def build_fetch_task_key(task: FetchTask) -> tuple[object, ...]:
    return (
        task.fetch_reason,
        task.endpoint_pattern,
        task.source_url,
        task.context_entity_type,
        task.context_entity_id,
        task.context_unique_tournament_id,
        task.context_season_id,
        task.context_event_id,
    )


def _scope_key(task: FetchTask) -> str:
    if task.context_entity_type and task.context_entity_id is not None:
        return f"{task.context_entity_type}:{task.context_entity_id}:{task.endpoint_pattern}"
    return task.endpoint_pattern


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _attempts_json(attempts: tuple[TransportAttempt, ...]) -> list[dict[str, object]] | None:
    if not attempts:
        return None
    return [
        {
            "attempt_number": int(item.attempt_number),
            "proxy_name": item.proxy_name,
            "proxy_address": item.proxy_address,
            "status_code": item.status_code,
            "error": item.error,
            "challenge_reason": item.challenge_reason,
        }
        for item in attempts
    ]
