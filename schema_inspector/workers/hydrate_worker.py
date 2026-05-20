"""Continuous hydrate worker backed by the shared worker runtime."""

from __future__ import annotations

import logging
import os
import time

from ..live_hydration_mode import resolve_live_hydration_mode
from ..queue.streams import GROUP_HYDRATE, STREAM_HISTORICAL_HYDRATE, STREAM_HYDRATE, StreamEntry
from ..services.worker_runtime import WorkerRuntime, resolve_worker_max_concurrency
from ._stream_jobs import decode_stream_job

logger = logging.getLogger(__name__)


def _env_flag_enabled(name: str) -> bool:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return False
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _is_retryable_for_quarantine(exc: Exception) -> bool:
    """P5b: same retryable classification as P0(c).2 worker code.

    Imports lazily to avoid pulling ``services.retry_policy`` (curl_cffi
    dependency) at module-import time. Fail-closed on import error so
    quarantine never engages for unclassifiable errors.
    """
    try:
        from ..services.retry_policy import is_retryable_worker_error
    except Exception:  # pragma: no cover - extremely defensive
        return False
    try:
        return bool(is_retryable_worker_error(exc))
    except Exception:  # pragma: no cover - defensive
        return False


class HydrateWorker:
    def __init__(
        self,
        *,
        orchestrator,
        delayed_scheduler,
        delayed_payload_store=None,
        completion_store=None,
        queue,
        consumer: str,
        group: str = GROUP_HYDRATE,
        stream: str = STREAM_HYDRATE,
        block_ms: int = 5_000,
        now_ms_factory=None,
        default_sport_slug: str = "football",
        job_audit_logger=None,
        max_concurrency: int | None = None,
        circuit_breaker=None,
        circuit_breaker_inprogress_count_provider=None,
        hydrate_inflight_store=None,
    ) -> None:
        self.orchestrator = orchestrator
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
        self.queue = queue
        self.consumer = consumer
        self.group = group
        self.stream = stream
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.default_sport_slug = default_sport_slug
        # Phase 2.1 (2026-05-20 perf audit): Redis single-flight lock keyed
        # on event_id. When None (legacy / unit tests), the worker behaves
        # as before — no dedup. service_app wires this in production via
        # ``HydrateInFlightStore`` so two parallel hydrate workers cannot
        # process the same event concurrently.
        self.hydrate_inflight_store = hydrate_inflight_store
        # P5b: per-event circuit breaker. ``circuit_breaker`` is an
        # ``EventCircuitBreaker(lane="hydrate")`` instance or None (when
        # the rollout flag ``HYDRATE_EVENT_CIRCUIT_BREAKER_ENABLED`` is
        # off; service_app passes store=None in that case to keep the
        # fast path branch-free). When wired, handle() checks the store
        # before orchestrator.run_event and skip+ACKs quarantined events;
        # success/RetryableJobError outcomes feed counter / clear marker
        # via record_success / record_failure.
        self.circuit_breaker = circuit_breaker
        self.circuit_breaker_inprogress_count_provider = circuit_breaker_inprogress_count_provider
        env_names = ("SOFASCORE_HYDRATE_WORKER_MAX_CONCURRENCY",)
        if stream == STREAM_HISTORICAL_HYDRATE:
            env_names = ("SOFASCORE_HISTORICAL_HYDRATE_WORKER_MAX_CONCURRENCY", *env_names)
        self.runtime = WorkerRuntime(
            name="hydrate-worker",
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
                default=2,
                explicit=max_concurrency,
                env_names=env_names,
            ),
        )

    async def handle(self, entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        if job.entity_id is None:
            raise RuntimeError("Hydrate worker requires event entity_id in stream payload.")
        event_id = int(job.entity_id)
        sport_slug = job.sport_slug or self.default_sport_slug

        # P5b: per-event circuit breaker — skip+ACK if event is quarantined
        # on the hydrate lane. Identical semantics to P0(c).2 quarantine
        # on tier_1, but in a separate Redis namespace (live:event_cb:
        # hydrate:*). Global-cap brake fails open when too many events
        # are quarantined cluster-wide.
        if self.circuit_breaker is not None:
            now_ms = int(self.now_ms_factory())
            until_ms = self.circuit_breaker.is_quarantined(event_id=event_id, now_ms=now_ms)
            if until_ms > now_ms:
                inprogress_count = 0
                provider = self.circuit_breaker_inprogress_count_provider
                if callable(provider):
                    try:
                        inprogress_count = int(provider() or 0)
                    except Exception as exc:
                        logger.debug(
                            "Hydrate circuit breaker inprogress-count provider failed: %s — treating as 0",
                            exc,
                        )
                        inprogress_count = 0
                cap_exceeded = False
                if inprogress_count > 0:
                    try:
                        cap_exceeded = self.circuit_breaker.global_cap_exceeded(
                            inprogress_event_count=inprogress_count
                        )
                    except Exception as exc:
                        logger.debug(
                            "Hydrate circuit breaker global_cap check failed: %s",
                            exc,
                        )
                        cap_exceeded = False
                if cap_exceeded:
                    logger.warning(
                        "Hydrate circuit breaker global cap exceeded — failing open: "
                        "event_id=%s job_type=%s inprogress=%s cap_pct=%s",
                        event_id, job.job_type, inprogress_count, self.circuit_breaker.global_cap_pct,
                    )
                    # fall through to normal flow
                else:
                    logger.info(
                        "Hydrate circuit breaker skip: event_id=%s job_type=%s cooldown_remaining_ms=%s",
                        event_id, job.job_type, max(0, until_ms - now_ms),
                    )
                    return "quarantined_skip"

        # Phase 2.1 (2026-05-20 perf audit): in-flight single-flight lock.
        # Two ``sofascore-hydrate@N`` workers may legitimately pull a job
        # for the same event_id (planner re-publish before ack, discovery
        # double-enqueue from multiple scopes). Without this guard, both
        # workers race on parallel /event GETs + overlapping DB upserts.
        lock_acquired = False
        lock_owner = f"{self.consumer}:{entry.message_id}:{job.job_id}"
        if self.hydrate_inflight_store is not None:
            try:
                lock_acquired = bool(
                    self.hydrate_inflight_store.claim(
                        event_id=event_id, owner=lock_owner
                    )
                )
            except Exception as exc:  # noqa: BLE001 — defensive
                logger.debug(
                    "hydrate_worker: in-flight claim failed event_id=%s: %r",
                    event_id,
                    exc,
                )
                lock_acquired = True  # fail open: don't block on Redis errors
            if not lock_acquired:
                logger.info(
                    "hydrate_worker: in-flight skip event_id=%s consumer=%s job=%s",
                    event_id, self.consumer, job.job_id,
                )
                return "inflight_skip"

        try:
            await self.orchestrator.run_event(
                event_id=event_id,
                sport_slug=sport_slug,
                hydration_mode=resolve_live_hydration_mode(
                    requested_mode=job.params.get("hydration_mode", "full"),
                    sport_slug=sport_slug,
                    scope=job.scope,
                ),
            )
        except Exception as exc:
            # P5b: only count retryable failures (libcurl timeouts,
            # network errors). Non-retryable bugs (AttributeError) must
            # NOT feed the circuit breaker counter — they must surface
            # for fix, not be silently rerouted via cooldown.
            if (
                self.circuit_breaker is not None
                and _is_retryable_for_quarantine(exc)
            ):
                try:
                    self.circuit_breaker.record_failure(event_id=event_id)
                except Exception as q_exc:  # pragma: no cover - defensive
                    logger.debug(
                        "Hydrate circuit breaker record_failure failed for event_id=%s: %s",
                        event_id, q_exc,
                    )
            raise
        else:
            if self.circuit_breaker is not None:
                try:
                    self.circuit_breaker.record_success(event_id=event_id)
                except Exception as q_exc:  # pragma: no cover - defensive
                    logger.debug(
                        "Hydrate circuit breaker record_success failed for event_id=%s: %s",
                        event_id, q_exc,
                    )
        finally:
            # Phase 2.1 (2026-05-20 perf audit): release the single-flight
            # lock in finally — covers both success and exception paths.
            # TTL gives a worst-case bound if the worker is SIGKILL-ed.
            if lock_acquired and self.hydrate_inflight_store is not None:
                try:
                    self.hydrate_inflight_store.release(
                        event_id=event_id, owner=lock_owner
                    )
                except Exception as exc:  # noqa: BLE001 — defensive
                    logger.debug(
                        "hydrate_worker: in-flight release failed event_id=%s: %r",
                        event_id,
                        exc,
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
