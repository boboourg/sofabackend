"""Continuous live details refresh worker service (P0(a) split-details).

Consumes ``stream:etl:live_details`` / ``cg:live_details``. Each entry
is a ``refresh_live_event_details`` job published by the live-tier
worker after its ROOT + edges critical phase completed for a given
``event_id``. This worker runs the heavy per-player detail fanout
without occupying live-tier worker capacity.

Key invariants:

* **Independent capacity pool.** Tier-1/2/3 worker counts are
  unaffected. Details concurrency is controlled separately via
  ``SOFASCORE_LIVE_DETAILS_WORKER_MAX_CONCURRENCY``.
* **Failure isolation.** Details fetch errors (timeout, SSL, 403,
  ...) are recorded in fetch_outcomes but DO NOT raise
  ``RetryableJobError``: a failed details fanout MUST NOT retry the
  parent ``refresh_live_event`` job, and MUST NOT mark this job
  ``retry_scheduled`` (which would feed back into worker queue
  pressure).
* **Status visibility.** ``handle()`` returns one of:
    * ``"completed"`` — all fetch_outcomes succeeded
    * ``"completed_with_errors"`` — at least one fetch_outcome was a
      network_error / timeout / soft_error / decode_error etc.
    * ``"coalesced_details"`` — another details worker is already
      processing this event (per
      ``LiveEventDetailsInFlightStore``).
"""

from __future__ import annotations

import json
import logging
import time

from ..jobs.types import JOB_REFRESH_LIVE_EVENT_DETAILS
from ..queue.streams import GROUP_LIVE_DETAILS, STREAM_LIVE_DETAILS, StreamEntry
from ..services.worker_runtime import WorkerRuntime, resolve_worker_max_concurrency
from ._stream_jobs import decode_stream_job

logger = logging.getLogger(__name__)


_NON_OK_CLASSIFICATIONS = frozenset(
    {
        "network_error",
        "decode_error",
        "soft_error_json",
        "access_denied",
        "rate_limited",
        "challenge_detected",
        "unexpected_content",
    }
)


class LiveDetailsWorkerService:
    """Worker for ``stream:etl:live_details`` (P0(a) split-details)."""

    def __init__(
        self,
        *,
        orchestrator,
        queue,
        consumer: str,
        block_ms: int = 5_000,
        now_ms_factory=None,
        default_sport_slug: str = "football",
        job_audit_logger=None,
        max_concurrency: int | None = None,
        details_in_flight_store=None,
    ) -> None:
        self.orchestrator = orchestrator
        self.queue = queue
        self.details_in_flight_store = details_in_flight_store
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.default_sport_slug = default_sport_slug
        resolved_max_concurrency = resolve_worker_max_concurrency(
            default=4,
            explicit=max_concurrency,
            env_names=("SOFASCORE_LIVE_DETAILS_WORKER_MAX_CONCURRENCY",),
        )
        self.runtime = WorkerRuntime(
            name="live-details-worker",
            queue=queue,
            stream=STREAM_LIVE_DETAILS,
            group=GROUP_LIVE_DETAILS,
            consumer=consumer,
            handler=self.handle,
            retry_handler=None,  # details failures do not retry
            completion_store=None,
            block_ms=block_ms,
            now_ms_factory=self.now_ms_factory,
            job_audit_logger=job_audit_logger,
            max_concurrency=resolved_max_concurrency,
            prefetch_count=resolved_max_concurrency,
        )

    async def handle(self, entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        if job.job_type != JOB_REFRESH_LIVE_EVENT_DETAILS:
            # Defensive: log and ack. Different job type on the details
            # stream is a misconfig, not retryable.
            logger.warning(
                "live-details-worker received unexpected job_type=%s message_id=%s — acking without work",
                job.job_type,
                entry.message_id,
            )
            return "completed"
        if job.entity_id is None:
            logger.warning(
                "live-details-worker received job without entity_id message_id=%s — acking without work",
                entry.message_id,
            )
            return "completed"
        event_id = int(job.entity_id)
        sport_slug = job.sport_slug or self.default_sport_slug
        context = self._extract_context(job.params)

        lock_owner: str | None = None
        if self.details_in_flight_store is not None:
            lock_owner = f"{self.runtime.consumer}:{entry.message_id}:{job.job_id}"
            if not self.details_in_flight_store.claim(event_id=event_id, owner=lock_owner):
                logger.info(
                    "Coalesced live-details fanout: event_id=%s message_id=%s consumer=%s",
                    event_id,
                    entry.message_id,
                    self.runtime.consumer,
                )
                return "coalesced_details"

        try:
            try:
                report = await self.orchestrator.run_event_details(
                    event_id=event_id,
                    sport_slug=sport_slug,
                    context=context,
                )
            except Exception as exc:
                # Details exception — log full trace, return non-retry
                # status. We intentionally do NOT raise; details
                # failures must not retry through the worker runtime.
                logger.warning(
                    "live-details-worker run_event_details raised for event_id=%s: type=%s msg=%s",
                    event_id,
                    type(exc).__name__,
                    str(exc)[:200],
                    exc_info=True,
                )
                return "completed_with_errors"
        finally:
            if self.details_in_flight_store is not None and lock_owner is not None:
                self.details_in_flight_store.release(event_id=event_id, owner=lock_owner)

        bad_outcomes = [
            outcome
            for outcome in report.fetch_outcomes
            if getattr(outcome, "classification", None) in _NON_OK_CLASSIFICATIONS
        ]
        if bad_outcomes:
            logger.info(
                "live-details-worker completed_with_errors event_id=%s bad=%s of %s",
                event_id,
                len(bad_outcomes),
                len(report.fetch_outcomes),
            )
            return "completed_with_errors"
        return "completed"

    def _extract_context(self, params: dict | None) -> dict:
        if not params:
            return {}
        # context can be passed inline as ``details_context`` dict or
        # serialised under ``details_context_json``. Both supported for
        # tolerance to future serialisation changes.
        ctx = params.get("details_context")
        if isinstance(ctx, dict):
            return dict(ctx)
        if isinstance(ctx, str):
            try:
                parsed = json.loads(ctx)
            except (TypeError, ValueError):
                return {}
            return parsed if isinstance(parsed, dict) else {}
        ctx_json = params.get("details_context_json")
        if isinstance(ctx_json, str):
            try:
                parsed = json.loads(ctx_json)
            except (TypeError, ValueError):
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    async def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        await self.runtime.run_forever(install_signal_handlers=install_signal_handlers)

    def request_shutdown(self) -> None:
        self.runtime.request_shutdown()
