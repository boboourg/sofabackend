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
from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_REFRESH_RESOURCE
from ..queue.empty_data import EmptyDataStore
from ..queue.pagination_done import PaginationDoneStore
from ..queue.resource_negative_cache import ResourceNegativeCache
from ..queue.totw_404_store import ToTW404Store
from ..queue.streams import (
    GROUP_RESOURCE_REFRESH,
    STREAM_RESOURCE_REFRESH,
    RedisStreamQueue,
    StreamEntry,
)
from ..services.worker_runtime import WorkerRuntime, resolve_worker_max_concurrency
from ._stream_jobs import decode_stream_job, encode_stream_job

logger = logging.getLogger(__name__)

# D13.3: hard-coded match for the ToTW endpoint pattern. Kept here as a
# module constant rather than imported from ``endpoints`` to avoid a
# circular import (endpoints.py is imported via local_api_endpoints
# inside the worker constructor). If the ToTW path template changes,
# update this string in lockstep.
_TOTW_ENDPOINT_PATTERN = (
    "/api/v1/unique-tournament/{unique_tournament_id}/season/{season_id}/team-of-the-week/{period_id}"
)


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
        negative_cache: ResourceNegativeCache | None = None,
        pagination_done: PaginationDoneStore | None = None,
        empty_data_store: EmptyDataStore | None = None,
        empty_predicates: dict[str, Any] | None = None,
        snapshot_reader=None,
        normalize_publisher=None,
        totw_404_store: ToTW404Store | None = None,
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
        self.negative_cache = negative_cache
        self.pagination_done = pagination_done
        self.empty_data_store = empty_data_store
        self.empty_predicates = dict(empty_predicates or DEFAULT_EMPTY_PREDICATES)
        self._normalize_publisher = normalize_publisher
        self.totw_404_store = totw_404_store
        # ``snapshot_reader`` is an awaitable callable
        # ``async (snapshot_id: int) -> dict | None`` that returns the
        # decoded payload of a freshly inserted snapshot. Tests inject a
        # mock; the production wire-up is in service_app.
        self._snapshot_reader = snapshot_reader
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
        # Mark stale targets in the negative cache so the planner stops
        # republishing them. Triggered by upstream 404 OR a soft-error
        # JSON envelope (some Sofascore endpoints return 200 with an error
        # body; classify_fetch_result tags those via classification).
        if self.negative_cache is not None and _is_negative_outcome(outcome):
            self.negative_cache.mark_404(
                endpoint_pattern=pattern,
                path_params=path_params,
            )
            # D13.3: Smart-404 by season for /team-of-the-week. A 404 on
            # any period_id for a (ut, season) pair is a strong signal
            # that the season does not support ToTW at all — every other
            # period_id under the same season would also 404. Mark the
            # season "no" so PeriodOfRegistryFootballResolver skips its
            # full period list on the next planner tick.
            if (
                self.totw_404_store is not None
                and pattern == _TOTW_ENDPOINT_PATTERN
            ):
                ut_id = path_params.get("unique_tournament_id")
                season_id = path_params.get("season_id")
                if ut_id is not None and season_id is not None:
                    try:
                        self.totw_404_store.mark_no(int(ut_id), int(season_id))
                    except Exception as exc:
                        logger.warning(
                            "totw blacklist mark_no failed ut=%s season=%s: %s",
                            ut_id, season_id, exc,
                        )
        elif (
            outcome.http_status == 200
            and outcome.snapshot_id is not None
        ):
            # D13.1: forward the freshly inserted snapshot to the normalize
            # stream so a worker-side parser populates the durable tables
            # (season_round, period, ...). Failure of the publish must not
            # fail the resource-refresh job — the snapshot is already on
            # disk and can be replayed via ``replay --snapshot-id`` if
            # needed.
            if self._normalize_publisher is not None:
                try:
                    self._normalize_publisher(
                        snapshot_id=int(outcome.snapshot_id),
                        endpoint_pattern=pattern,
                        sport_slug=str(job.sport_slug or ""),
                        trace_id=job.trace_id,
                        parent_job_id=job.job_id,
                    )
                except Exception as exc:
                    logger.warning(
                        "normalize publish failed pattern=%s snapshot_id=%s: %s",
                        pattern,
                        outcome.snapshot_id,
                        exc,
                    )
            if job.entity_id is not None:
                # D6: empty-data marker. Some endpoints return 200 with an
                # empty body for entities that have no data (e.g.
                # last-year-summary for retired players,
                # national-team-statistics for players without a national
                # appearance). The negative cache cannot help because
                # status is 200; we instead inspect the payload through
                # an endpoint-specific predicate and record the entity in
                # the EmptyDataStore so the planner can skip re-publishing
                # for the endpoint's empty_data_ttl_seconds.
                if (
                    endpoint.empty_predicate is not None
                    and self.empty_data_store is not None
                ):
                    await self._maybe_mark_empty(
                        job=job,
                        pattern=pattern,
                        snapshot_id=outcome.snapshot_id,
                        endpoint=endpoint,
                    )
                if endpoint.auto_paginate:
                    # Stage 8 / C.4 worker-side auto-pagination: chain
                    # page=K+1 on hasNextPage=true, mark completed on
                    # hasNextPage=false. Only runs on a successful fetch
                    # with a fresh snapshot row (skipping dedup'd inserts
                    # is fine -- the next planner tick will retry).
                    await self._maybe_chain_next_page(
                        job=job,
                        pattern=pattern,
                        path_params=path_params,
                        snapshot_id=outcome.snapshot_id,
                        endpoint=endpoint,
                    )
        return "completed"

    async def _maybe_mark_empty(
        self,
        *,
        job: JobEnvelope,
        pattern: str,
        snapshot_id: int,
        endpoint: SofascoreEndpoint,
    ) -> None:
        if self._snapshot_reader is None:
            return
        predicate_name = str(endpoint.empty_predicate or "")
        predicate = self.empty_predicates.get(predicate_name)
        if predicate is None:
            logger.warning(
                "empty-data: unknown predicate %r for pattern=%s; skipping mark",
                predicate_name,
                pattern,
            )
            return
        try:
            payload = await self._snapshot_reader(snapshot_id)
        except Exception as exc:
            logger.warning(
                "empty-data snapshot read failed pattern=%s target=%s id=%s: %s",
                pattern,
                job.entity_id,
                snapshot_id,
                exc,
            )
            return
        try:
            is_empty = bool(predicate(payload))
        except Exception as exc:
            logger.warning(
                "empty-data predicate %r raised on pattern=%s target=%s: %s",
                predicate_name,
                pattern,
                job.entity_id,
                exc,
            )
            return
        if not is_empty:
            return
        entity_id = int(job.entity_id) if job.entity_id is not None else None
        if entity_id is None:
            return
        self.empty_data_store.mark_empty(
            endpoint_pattern=pattern,
            entity_id=entity_id,
            when_ms=int(self.now_ms_factory()),
        )

    async def _maybe_chain_next_page(
        self,
        *,
        job: JobEnvelope,
        pattern: str,
        path_params: Mapping[str, Any],
        snapshot_id: int,
        endpoint: SofascoreEndpoint,
    ) -> None:
        """Inspect just-inserted snapshot, publish page=K+1 if appropriate."""

        if self._snapshot_reader is None:
            return  # no reader wired (e.g. unit tests opting out)
        try:
            payload = await self._snapshot_reader(snapshot_id)
        except Exception as exc:
            logger.warning(
                "auto-pagination snapshot read failed pattern=%s target=%s id=%s: %s",
                pattern,
                job.entity_id,
                snapshot_id,
                exc,
            )
            return
        if not isinstance(payload, dict):
            return
        has_next = payload.get("hasNextPage")
        entity_id = int(job.entity_id) if job.entity_id is not None else None
        if entity_id is None:
            return

        # Terminal: hasNextPage=false (or absent) -> mark completed and stop.
        if has_next is False:
            if self.pagination_done is not None:
                self.pagination_done.mark_completed(
                    endpoint_pattern=pattern,
                    entity_id=entity_id,
                    when_ms=int(self.now_ms_factory()),
                )
            return
        if has_next is None:
            # Defensive: malformed payload -> do not chain (ambiguous).
            return

        current_page = int(path_params.get("page", 0) or 0)
        next_page = current_page + 1

        # Safety fuse: never let a chain run past max_pages.
        if endpoint.max_pages and next_page >= int(endpoint.max_pages):
            logger.warning(
                "auto-pagination safety fuse: pattern=%s target=%s reached max_pages=%s",
                pattern,
                entity_id,
                endpoint.max_pages,
            )
            if self.pagination_done is not None:
                # Treat as completed for cool-down purposes; the next audit
                # cycle will retry.
                self.pagination_done.mark_completed(
                    endpoint_pattern=pattern,
                    entity_id=entity_id,
                    when_ms=int(self.now_ms_factory()),
                )
            return

        # Cool-down only on the planner-driven page=0 entry. Once a chain
        # has started (page>=1) we always continue until terminal/max_pages.
        if (
            current_page == 0
            and self.pagination_done is not None
            and endpoint.audit_interval_seconds is not None
            and self.pagination_done.is_completed_recently(
                endpoint_pattern=pattern,
                entity_id=entity_id,
                audit_interval_seconds=int(endpoint.audit_interval_seconds),
                now_ms=int(self.now_ms_factory()),
            )
        ):
            return

        # Defence in depth: skip if the next page is already known-bad in
        # the negative cache.
        next_path_params = dict(path_params)
        next_path_params["page"] = next_page
        if (
            self.negative_cache is not None
            and self.negative_cache.is_negatively_cached(
                endpoint_pattern=pattern,
                path_params=next_path_params,
            )
        ):
            return

        next_envelope = _build_next_page_envelope(job, pattern, next_path_params)
        try:
            self.queue.publish(self.stream, encode_stream_job(next_envelope))
        except Exception as exc:
            logger.warning(
                "auto-pagination publish failed pattern=%s target=%s page=%s: %s",
                pattern,
                entity_id,
                next_page,
                exc,
            )

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


def _build_next_page_envelope(
    job: JobEnvelope,
    pattern: str,
    next_path_params: Mapping[str, Any],
) -> JobEnvelope:
    """Build a JOB_REFRESH_RESOURCE envelope for page=K+1 of an auto-paginated chain.

    Reuses freshness_key / context fields from the originating job's params
    but rebuilds the freshness_key for the new page so per-page TTL stays
    independent. Note: idempotency_key is recomputed inside JobEnvelope.create
    based on the (job_type, entity, params) tuple, so two distinct pages of
    the same entity get distinct ids.
    """

    import json

    src_params = dict(job.params or {})
    new_params = dict(src_params)
    new_params["path_params"] = dict(next_path_params)
    # Per-page freshness_key so the FreshnessStore TTL on page=K does not
    # block page=K+1.
    pp_repr = json.dumps(
        dict(next_path_params), ensure_ascii=True, sort_keys=True, separators=(",", ":")
    )
    new_params["freshness_key"] = f"freshness:{pattern}|{pp_repr}"

    return JobEnvelope.create(
        job_type=JOB_REFRESH_RESOURCE,
        sport_slug=job.sport_slug,
        entity_type=job.entity_type,
        entity_id=int(job.entity_id) if job.entity_id is not None else None,
        scope=job.scope or "resource_refresh",
        params=new_params,
        priority=int(job.priority or 0),
        trace_id=job.trace_id,
        capability_hint=job.capability_hint or "resource_refresh",
        parent_job_id=job.job_id,
    )


def _is_negative_outcome(outcome: Any) -> bool:
    """True for outcomes that should mark the target in the negative cache.

    Covers HTTP 404 and JSON soft-error envelopes (200 with ``{"error":...}``
    body that ``classify_fetch_result`` flags via classification).
    """

    status = getattr(outcome, "http_status", None)
    if status == 404:
        return True
    if status is not None and 400 <= int(status) < 500 and status != 429:
        # 4xx other than 429 (rate limited) is also "no point retrying soon".
        return True
    classification = str(getattr(outcome, "classification", "") or "").lower()
    if "soft_error" in classification:
        return True
    return bool(getattr(outcome, "is_soft_error_payload", False))


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


def _is_empty_last_year_summary(payload: Any) -> bool:
    """True when the rolling 12-month summary carries no data.

    Pre-D2 probe shape: ``{"summary": [...], "uniqueTournamentsMap": {...}}``.
    Empty body for retired/inactive players is exactly the two-key dict with
    an empty list and an empty dict.
    """

    if not isinstance(payload, dict):
        return False
    summary = payload.get("summary")
    map_part = payload.get("uniqueTournamentsMap")
    summary_empty = isinstance(summary, list) and len(summary) == 0
    map_empty = isinstance(map_part, dict) and len(map_part) == 0
    return summary_empty and map_empty


def _is_empty_national_team_statistics(payload: Any) -> bool:
    """True when the player has no national-team aggregate statistics."""

    if not isinstance(payload, dict):
        return False
    stats = payload.get("statistics")
    return isinstance(stats, list) and len(stats) == 0


DEFAULT_EMPTY_PREDICATES: dict[str, Any] = {
    "last_year_summary": _is_empty_last_year_summary,
    "national_team_statistics": _is_empty_national_team_statistics,
}


__all__ = ["ResourceRefreshWorker", "DEFAULT_EMPTY_PREDICATES"]
