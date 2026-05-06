"""Generic resource refresh worker.

Consumes ``stream:etl:resource_refresh`` produced by ``ResourcePlannerDaemon``.
For every envelope it builds a ``FetchTask`` from ``params`` and hands it to
the shared :class:`FetchExecutor`. The executor performs:

* freshness skip (via ``FreshnessStore``)
* HTTP fetch through the configured proxy pool
* response classification
* ``api_payload_snapshot`` insert (idempotent on payload_hash)
* request log insert
* freshness mark on success

This worker is intentionally thin: it carries NO endpoint-specific business
logic. Every endpoint family is handled the same way; new endpoints are
added by setting metadata on ``SofascoreEndpoint`` and adding/extending a
``ScopeResolver``.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Mapping

from ..endpoints import SofascoreEndpoint, local_api_endpoints
from ..fetch_models import FetchTask
from ..queue.streams import (
    GROUP_RESOURCE_REFRESH,
    STREAM_RESOURCE_REFRESH,
    StreamEntry,
)
from ..services.worker_runtime import WorkerRuntime, resolve_worker_max_concurrency
from ._stream_jobs import decode_stream_job

logger = logging.getLogger(__name__)


class ResourceRefreshWorker:
    """Stream consumer that hands every envelope to FetchExecutor."""

    def __init__(
        self,
        *,
        fetch_executor,
        delayed_scheduler,
        delayed_payload_store=None,
        completion_store=None,
        queue,
        consumer: str,
        group: str = GROUP_RESOURCE_REFRESH,
        stream: str = STREAM_RESOURCE_REFRESH,
        block_ms: int = 5_000,
        now_ms_factory=None,
        endpoints: tuple[SofascoreEndpoint, ...] | None = None,
        default_timeout_seconds: float = 20.0,
        job_audit_logger=None,
        max_concurrency: int | None = None,
    ) -> None:
        self.fetch_executor = fetch_executor
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
        self.queue = queue
        self.consumer = consumer
        self.group = group
        self.stream = stream
        self.default_timeout_seconds = float(default_timeout_seconds)
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self._endpoints_by_pattern: dict[str, SofascoreEndpoint] = {
            ep.pattern: ep
            for ep in (endpoints if endpoints is not None else local_api_endpoints())
        }
        self.runtime = WorkerRuntime(
            name="resource-refresh-worker",
            queue=queue,
            stream=stream,
            group=group,
            consumer=consumer,
            handler=self.handle,
            retry_handler=self.retry_later,
            completion_store=completion_store,
            block_ms=block_ms,
            now_ms_factory=self.now_ms_factory,
            job_audit_logger=job_audit_logger,
            max_concurrency=resolve_worker_max_concurrency(
                default=4,
                explicit=max_concurrency,
                env_names=("SOFASCORE_RESOURCE_REFRESH_WORKER_MAX_CONCURRENCY",),
            ),
        )

    async def handle(self, entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        params: Mapping[str, Any] = job.params or {}
        pattern = str(params.get("endpoint_pattern") or "")
        if not pattern:
            raise RuntimeError(
                f"resource refresh job missing endpoint_pattern: job_id={job.job_id}"
            )
        endpoint = self._endpoints_by_pattern.get(pattern)
        if endpoint is None:
            raise RuntimeError(
                f"resource refresh job for unknown endpoint_pattern={pattern!r}; "
                "register it in endpoints.py before publishing"
            )
        path_params = dict(params.get("path_params") or {})
        try:
            source_url = endpoint.build_url(**path_params)
        except Exception as exc:
            raise RuntimeError(
                f"resource refresh: cannot build URL for {pattern} "
                f"path_params={path_params}: {exc}"
            ) from exc

        task = FetchTask(
            trace_id=job.trace_id,
            job_id=job.job_id,
            sport_slug=job.sport_slug,
            endpoint_pattern=pattern,
            source_url=source_url,
            timeout_profile="resource_refresh",
            timeout_seconds=self.default_timeout_seconds,
            method="GET",
            request_headers=None,
            query_params=None,
            context_entity_type=job.entity_type,
            context_entity_id=int(job.entity_id) if job.entity_id is not None else None,
            context_unique_tournament_id=_optional_int(params.get("context_unique_tournament_id")),
            context_season_id=_optional_int(params.get("context_season_id")),
            context_event_id=_optional_int(params.get("context_event_id")),
            expected_content_type="application/json",
            fetch_reason="resource_refresh",
            freshness_key=_optional_str(params.get("freshness_key")),
            freshness_ttl_seconds=_optional_int(params.get("freshness_ttl_seconds")),
        )

        outcome = await self.fetch_executor.execute(task)
        if outcome.http_status is None:
            # Transport-level failure -> let the runtime classify and retry.
            raise RuntimeError(
                f"resource refresh transport failed: pattern={pattern} "
                f"target={job.entity_id} reason={outcome.classification}"
            )
        return "completed"

    async def retry_later(self, entry: StreamEntry, exc: Exception, *, delay_ms: int) -> str:
        del exc
        job = decode_stream_job(entry)
        if self.delayed_payload_store is not None:
            self.delayed_payload_store.save_entry(entry)
        self.delayed_scheduler.schedule(
            job.job_id,
            run_at_epoch_ms=int(self.now_ms_factory()) + int(delay_ms),
        )
        return "requeued"

    async def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        await self.runtime.run_forever(install_signal_handlers=install_signal_handlers)

    def request_shutdown(self) -> None:
        self.runtime.request_shutdown()


def _optional_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_str(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


__all__ = ["ResourceRefreshWorker"]
