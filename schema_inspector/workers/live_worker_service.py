"""Continuous live refresh worker services for hot and warm lanes."""

from __future__ import annotations

import json
import logging
import os
import time

from ..jobs.envelope import JobEnvelope
from ..jobs.types import JOB_REFRESH_LIVE_EVENT, JOB_REFRESH_LIVE_EVENT_DETAILS
from ..live_dispatch_policy import fetch_timeout_for_dispatch_tier
from ..live_hydration_mode import resolve_live_hydration_mode
from ..queue.streams import (
    GROUP_LIVE_HOT,
    GROUP_LIVE_DETAILS,
    GROUP_LIVE_TIER_1,
    GROUP_LIVE_TIER_2,
    GROUP_LIVE_TIER_3,
    GROUP_LIVE_WARM,
    STREAM_LIVE_DETAILS,
    STREAM_LIVE_HOT,
    STREAM_LIVE_TIER_1,
    STREAM_LIVE_TIER_2,
    STREAM_LIVE_TIER_3,
    STREAM_LIVE_WARM,
    StreamEntry,
)
from ..services.worker_runtime import BatchDispatchPlan, WorkerRuntime, resolve_worker_max_concurrency
from ._stream_jobs import decode_stream_job
from .hydrate_worker import _scope_bypasses_terminal_lock

_HOT_PREFETCH_COUNT = 25
logger = logging.getLogger(__name__)


class LiveWorkerService:
    def __init__(
        self,
        *,
        orchestrator,
        delayed_scheduler,
        delayed_payload_store=None,
        completion_store=None,
        queue,
        lane: str,
        consumer: str,
        block_ms: int = 5_000,
        now_ms_factory=None,
        default_sport_slug: str = "football",
        job_audit_logger=None,
        max_concurrency: int | None = None,
        in_flight_store=None,
        details_throttle=None,
        details_backpressure_limit: int | None = None,
        root_in_flight_store=None,
        edges_throttle=None,
        edges_backpressure_limit: int | None = None,
        quarantine_store=None,
        quarantine_inprogress_count_provider=None,
        live_state_repository=None,
        database=None,
    ) -> None:
        normalized_lane = str(lane).strip().lower()
        if normalized_lane not in {"hot", "warm", "tier_1", "tier_2", "tier_3"}:
            raise ValueError(f"Unsupported live lane: {lane!r}")

        self.orchestrator = orchestrator
        self.delayed_scheduler = delayed_scheduler
        self.delayed_payload_store = delayed_payload_store
        self.in_flight_store = in_flight_store
        self.queue = queue
        # P0(a) split-details rollout: per-event rate-limiter + optional
        # backpressure on the details stream. ``details_throttle`` is the
        # ``LiveDetailsThrottle`` instance (or None for tests / legacy).
        # ``details_backpressure_limit`` (env
        # ``LIVE_DETAILS_STREAM_BACKPRESSURE_LIMIT``) caps the
        # ``stream:etl:live_details`` length; above the cap, live-tier
        # workers skip the details enqueue and log
        # "details_backpressure_skip" rather than letting the details
        # backlog grow unbounded.
        self.details_throttle = details_throttle
        env_backpressure = _env_optional_positive_int("LIVE_DETAILS_STREAM_BACKPRESSURE_LIMIT")
        self.details_backpressure_limit = (
            details_backpressure_limit
            if details_backpressure_limit is not None
            else env_backpressure
        )
        # P0(b) tier_1 root-only rollout. ``root_in_flight_store`` is a
        # ``LiveEventRootInFlightStore`` instance (or None for tests /
        # legacy). When ``LIVE_TIER_1_ROOT_ONLY=1`` and this worker's
        # lane is ``tier_1``, ``handle()`` claims this lock instead of
        # ``in_flight_store`` so root-only fetches don't block on (and
        # aren't blocked by) full ROOT+edges runs on other lanes.
        # ``edges_throttle`` rate-limits the follow-up
        # ``refresh_live_event`` enqueue onto ``stream:etl:live_warm``.
        # ``edges_backpressure_limit`` (env
        # ``LIVE_EDGES_STREAM_BACKPRESSURE_LIMIT``) caps the
        # ``stream:etl:live_warm`` length; above the cap, root-only
        # workers skip the edges enqueue rather than letting the warm
        # backlog grow unbounded.
        self.root_in_flight_store = root_in_flight_store
        self.edges_throttle = edges_throttle
        env_edges_backpressure = _env_optional_positive_int("LIVE_EDGES_STREAM_BACKPRESSURE_LIMIT")
        self.edges_backpressure_limit = (
            edges_backpressure_limit
            if edges_backpressure_limit is not None
            else env_edges_backpressure
        )
        # P0(c) tier_1 root-only retry quarantine. ``quarantine_store`` is a
        # ``LiveTier1RetryQuarantineStore`` instance (or None when the
        # rollout flag ``LIVE_TIER_1_QUARANTINE_ENABLED`` is OFF — the
        # service-app constructs the store unconditionally but only passes
        # it through when the flag is enabled, keeping the legacy fast path
        # branch-free at runtime). When set, the worker checks the store
        # before claiming the inflight lock and skips+ACKs quarantined
        # events; on real-work success/failure it updates the store so the
        # counter and exponential cooldown stay accurate.
        # ``quarantine_inprogress_count_provider`` is an optional callable
        # ``() -> int`` returning the current count of inprogress live
        # events; used by the global-cap brake to fail-open when too
        # many events are quarantined cluster-wide. None → cap-check is
        # skipped (treated as "cannot evaluate" → quarantine still works
        # but no runaway protection); intended for test setups.
        self.quarantine_store = quarantine_store
        self.quarantine_inprogress_count_provider = quarantine_inprogress_count_provider
        # Task 2 Phase C (2026-05-20): terminal-lock gate.
        self.live_state_repository = live_state_repository
        self.database = database
        self.lane = normalized_lane
        # Phase1-A2 (2026-05-29): per-tier HTTP fetch timeout for this lane's
        # run_event calls. tier_1/hot get the most headroom (top live matches
        # must survive proxy-latency bursts to hydrate their match-center —
        # the 2026-05-29 event-15728277 regression); tier_3 fails fast so a
        # dead niche fetch does not pin a proxy. None for unrecognised lanes
        # → caller keeps the global SOFASCORE_FETCH_TIMEOUT_SECONDS default.
        self._fetch_timeout_seconds = fetch_timeout_for_dispatch_tier(normalized_lane)
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.default_sport_slug = default_sport_slug
        stream, group = _lane_stream_group(normalized_lane)
        hot_like_lane = normalized_lane in {"hot", "tier_1", "tier_2", "tier_3"}
        resolved_max_concurrency = resolve_worker_max_concurrency(
            default=1 if hot_like_lane else 2,
            explicit=max_concurrency,
            env_names=_lane_concurrency_env_names(normalized_lane),
        )
        batch_preprocessor = _coalesce_live_refresh_batch if hot_like_lane else None
        prefetch_count = _HOT_PREFETCH_COUNT if hot_like_lane else resolved_max_concurrency
        self.runtime = WorkerRuntime(
            name=f"live-{normalized_lane}-worker",
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
            max_concurrency=resolved_max_concurrency,
            prefetch_count=prefetch_count,
            batch_preprocessor=batch_preprocessor,
        )

    async def handle(self, entry: StreamEntry) -> str:
        job = decode_stream_job(entry)
        if job.entity_id is None:
            raise RuntimeError("Live worker requires event entity_id in stream payload.")
        event_id = int(job.entity_id)
        sport_slug = job.sport_slug or self.default_sport_slug

        # Task 2 Phase C (2026-05-20): terminal-lock skip gate.
        # Frozen events (event_terminal_state.locked_at IS NOT NULL)
        # bypass the entire live pipeline. The planner ALREADY skips
        # via Redis live_state.is_finalized — this is defense-in-depth
        # against stale stream messages that bypassed the planner gate
        # (XAUTOCLAIM of pending messages, manual republishes, etc.).
        if (
            self.live_state_repository is not None
            and self.database is not None
            and not _scope_bypasses_terminal_lock(job.scope)
        ):
            try:
                async with self.database.connection() as connection:
                    locked = await self.live_state_repository.is_event_locked(
                        connection, event_id=event_id
                    )
            except Exception as exc:  # noqa: BLE001 — fail-open
                logger.debug(
                    "live_worker: locked-check failed event_id=%s: %r",
                    event_id, exc,
                )
                locked = False
            if locked:
                logger.info(
                    "live_worker: terminal-lock skip event_id=%s lane=%s "
                    "scope=%s job=%s",
                    event_id, self.lane, job.scope, job.job_id,
                )
                return "terminal_locked_skip"

        # P0(b) tier_1 root-only fast path. Only kicks in for ``tier_1``
        # lane workers (NOT tier_2/tier_3/warm/hot) AND only for normal
        # ``refresh_live_event`` jobs (NOT details follow-ups, etc.). The
        # path uses an INDEPENDENT lock (``live:root_inflight``) so a
        # full-refresh holding ``live:refresh_inflight`` (e.g. on
        # live_warm via the edges follow-up) does not block the next
        # tier_1 root tick. When the flag is OFF, this worker preserves
        # legacy behaviour exactly.
        is_root_only_tier_1 = (
            self.lane == "tier_1"
            and job.job_type == JOB_REFRESH_LIVE_EVENT
            and _env_flag_enabled("LIVE_TIER_1_ROOT_ONLY")
            and self.root_in_flight_store is not None
        )
        active_lock_store = self.root_in_flight_store if is_root_only_tier_1 else self.in_flight_store

        # P0(c.2) Quarantine eligibility is INDEPENDENT of the root-only
        # fast path. The original P0(c) rollout gated quarantine on
        # ``is_root_only_tier_1`` which required ``job.job_type ==
        # JOB_REFRESH_LIVE_EVENT``. In practice tier_1 stream also
        # carries ``JOB_HYDRATE_EVENT_ROOT`` bootstrap jobs published by
        # the discovery worker for newly-discovered live events
        # (discovery_worker.py:_stream_for_live_dispatch_tier). Those
        # bypassed quarantine and burned tier_1 capacity indefinitely on
        # bad-route events (e.g. event_id 14036755 observed retrying
        # every ~2 min for hours through curl timeout). Decoupling
        # quarantine from the root-only branch lets the same per-event
        # failure counter + cooldown apply to BOTH job types arriving
        # on the tier_1 lane. The root-only fast path itself (mode
        # selection, edges follow-up, root_inflight lock) is unchanged.
        is_quarantine_eligible = (
            self.lane == "tier_1"
            and self.quarantine_store is not None
        )

        # P0(c.3) high-value carve-out (2026-05-30 live audit, event
        # 15728277): a marquee tier_1 match must NEVER be quarantined.
        # The 2026-05-29 incident was a fully-covered top match that went
        # 19 endpoints NO_FETCH and 404'd to the frontend after 3 transient
        # proxy timeouts tripped the quarantine threshold — exactly like a
        # niche event. ``resolve_live_dispatch_tier`` already returns
        # ``tier_1`` for football detailId==1 (_TIER_1_DETAIL_IDS) OR
        # tournament user_count>=LIVE_TIER_1_MIN_USER_COUNT (6500), and the
        # planner / discovery worker persist that verdict as
        # ``live_dispatch_tier`` in the job params — the ONLY marquee signal
        # reliably present on the dominant planner refresh path (the
        # LiveEventState hash stores dispatch_tier, not raw detail_id /
        # user_count). We also honour raw detail_id / user_count when a
        # caller threads them, so the carve-out stays correct if a future
        # publisher includes them. ``is_high_value`` is threaded into BOTH
        # the skip check (so a marquee event is never skipped) AND
        # record_failure (so its counter never even trips) — see the
        # ``high_value`` kwarg on LiveTier1RetryQuarantineStore.
        is_high_value = is_quarantine_eligible and _is_high_value_tier_1(job)

        # P0(c) tier_1 retry quarantine check. Performed BEFORE inflight
        # claim so a quarantined event does not spend the in-flight lock
        # slot on this tick. Skip+ACK semantics: the worker returns
        # ``quarantined_skip`` so the runtime treats it as a normal
        # completion (no retry budget), the stream message is acked, and
        # the next planner- or discovery-published tick for the same
        # event will see the cooldown expired (or not) and decide again.
        # Global-cap brake fails open if too many events are currently
        # quarantined.
        if is_quarantine_eligible and not is_high_value:
            now_ms = int(self.now_ms_factory())
            until_ms = self.quarantine_store.is_quarantined(event_id=event_id, now_ms=now_ms)
            if until_ms > now_ms:
                inprogress_count = 0
                provider = self.quarantine_inprogress_count_provider
                if callable(provider):
                    try:
                        inprogress_count = int(provider() or 0)
                    except Exception as exc:
                        logger.debug(
                            "Quarantine inprogress-count provider failed: %s — treating as 0 (fail-open cap)",
                            exc,
                        )
                        inprogress_count = 0
                cap_exceeded = False
                if inprogress_count > 0:
                    try:
                        cap_exceeded = self.quarantine_store.global_cap_exceeded(
                            inprogress_event_count=inprogress_count
                        )
                    except Exception as exc:
                        logger.debug(
                            "Quarantine global_cap_exceeded check failed: %s — treating as not-exceeded",
                            exc,
                        )
                        cap_exceeded = False
                if cap_exceeded:
                    logger.warning(
                        "Tier_1 quarantine global cap exceeded — failing open: "
                        "event_id=%s job_type=%s inprogress=%s cap_pct=%s",
                        event_id,
                        job.job_type,
                        inprogress_count,
                        self.quarantine_store.global_cap_pct,
                    )
                    # fall through to normal flow
                else:
                    logger.info(
                        "Tier_1 quarantine skip: event_id=%s job_type=%s cooldown_remaining_ms=%s",
                        event_id,
                        job.job_type,
                        max(0, until_ms - now_ms),
                    )
                    # P0(c.3) reschedule-on-skip (2026-05-30 live audit): the
                    # quarantine-skip return fires BEFORE the inflight claim and
                    # BEFORE orchestrator.run_event — and run_event is the ONLY
                    # path that reaches track_event, the ONLY place next_poll_at
                    # (the hot/warm/cold zset score) advances. So a quarantined
                    # live event keeps its old, already-due score: the planner
                    # re-dispatches it every tick (a coalesced refresh storm) and
                    # oldest_hot_score_age_seconds (= now - min(zset score)),
                    # which gates the freshness SLO + the 24x7 exit criteria,
                    # grows unbounded. Push the score forward to a backed-off
                    # retry (reuses the shipped 2026-05-29 root-fetch helper) so
                    # the metric reflects the real next-retry time and the event
                    # self-heals when the cooldown expires. The worker does NOT
                    # hold live_state_store directly; it reaches through the
                    # orchestrator (ServiceApp.app), which owns both .live_worker
                    # and .live_state_store. Fully defensive via getattr — a test
                    # SimpleNamespace orchestrator without these attrs is a silent
                    # no-op, and the helper itself never raises / never re-livens
                    # a finalized event.
                    self._reschedule_on_quarantine_skip(
                        event_id=event_id, sport_slug=sport_slug
                    )
                    return "quarantined_skip"

        lock_owner: str | None = None
        if active_lock_store is not None:
            lock_owner = f"{self.runtime.consumer}:{entry.message_id}:{job.job_id}"
            if not active_lock_store.claim(event_id=event_id, owner=lock_owner):
                logger.info(
                    "Coalesced in-flight live refresh: lane=%s event_id=%s stream=%s message_id=%s consumer=%s%s",
                    self.lane,
                    event_id,
                    entry.stream,
                    entry.message_id,
                    self.runtime.consumer,
                    " mode=root_only" if is_root_only_tier_1 else "",
                )
                return "coalesced_inflight_root_only" if is_root_only_tier_1 else "coalesced_inflight"

        if is_root_only_tier_1:
            effective_hydration_mode = "root_only"
        else:
            effective_hydration_mode = resolve_live_hydration_mode(
                requested_mode=job.params.get("hydration_mode") or "live_delta",
                sport_slug=sport_slug,
                scope=job.scope,
            )

        report = None
        try:
            try:
                report = await self.orchestrator.run_event(
                    event_id=event_id,
                    sport_slug=sport_slug,
                    hydration_mode=effective_hydration_mode,
                    # Phase1-A2: per-tier fetch timeout for this lane.
                    fetch_timeout_seconds=self._fetch_timeout_seconds,
                )
            except Exception as exc:
                # P0(c) quarantine bookkeeping: only count
                # RetryableJobError-class failures (network_error,
                # libcurl timeouts, RetryableJobError from upstream
                # transport). Non-retryable raises (logic bugs,
                # AttributeError) MUST NOT feed the quarantine counter
                # — those should surface, not be silently rerouted.
                # Coalesced returns already exited earlier via
                # "coalesced_inflight*" and never reach this block, so
                # the success/failure bookkeeping below only ever sees
                # real-work outcomes. As of P0(c.2) this applies to
                # BOTH refresh_live_event AND hydrate_event_root jobs
                # on the tier_1 lane — see the
                # ``is_quarantine_eligible`` comment above.
                if (
                    is_quarantine_eligible
                    and _is_retryable_for_quarantine(exc)
                ):
                    try:
                        # high_value=True short-circuits the counter so a
                        # marquee tier_1 match never accumulates toward the
                        # quarantine threshold (P0(c.3) carve-out). The
                        # transient failure is still surfaced via the re-raise
                        # below, and the orchestrator's own
                        # reschedule_after_transient_failure (root-fetch path)
                        # keeps the schedule honest.
                        self.quarantine_store.record_failure(
                            event_id=event_id, high_value=is_high_value
                        )
                    except Exception as q_exc:  # pragma: no cover - defensive
                        logger.debug(
                            "Quarantine record_failure failed for event_id=%s: %s",
                            event_id,
                            q_exc,
                        )
                raise
            else:
                if is_quarantine_eligible:
                    try:
                        self.quarantine_store.record_success(event_id=event_id)
                    except Exception as q_exc:  # pragma: no cover - defensive
                        logger.debug(
                            "Quarantine record_success failed for event_id=%s: %s",
                            event_id,
                            q_exc,
                        )
        finally:
            # Release critical lock IMMEDIATELY after run_event returns.
            # Under split-details (P0(a)) the orchestrator returns after
            # ROOT + edges only — releasing now lets the next root poll
            # for this event proceed without waiting for details fanout.
            # Under root-only (P0(b)) this is even shorter — release
            # the short-TTL ``live:root_inflight`` lock now.
            if active_lock_store is not None and lock_owner is not None:
                active_lock_store.release(event_id=event_id, owner=lock_owner)
            # Task 6 (2026-05-15): release the live-bootstrap hydrate
            # lock so a half-completed bootstrap does not freeze every
            # subsequent poll cycle for this event behind a 60 s TTL.
            # No-op when no lock was acquired (worker was running in
            # delta mode, or the event was already bootstrapped).
            orchestrator = getattr(self, "orchestrator", None)
            if orchestrator is not None and hasattr(orchestrator, "release_hydrate_lock_if_held"):
                orchestrator.release_hydrate_lock_if_held()

        # P0(b) root-only: enqueue a follow-up ``refresh_live_event``
        # (full hydration) onto ``stream:etl:live_warm`` so edges/details
        # still happen on the slow lane. Skipped when:
        #   - report missing or edges_pending=False (e.g. terminal payload
        #     finalized inline this tick)
        #   - event finalized this tick (already terminal)
        #   - throttle says we just enqueued for this event (rate-limit
        #     window default 60 s)
        #   - live_warm stream length > backpressure cap
        # Enqueue failure is logged at WARNING but root-only still
        # returns "completed" — edges enqueue must NOT feed back into
        # root retry budget.
        if (
            is_root_only_tier_1
            and report is not None
            and getattr(report, "edges_pending", False)
            and not getattr(report, "finalized", False)
        ):
            self._maybe_enqueue_edges(event_id=event_id, sport_slug=sport_slug, job=job)
            return "completed"

        # P0(a): if split-details fanout is enabled (legacy non-root-only
        # path), enqueue a standalone ``refresh_live_event_details`` job
        # onto ``stream:etl:live_details``. Skipped when:
        #   - report missing (test/early-return paths) or details_pending=False
        #   - event finalized this tick (final sweep already covered details)
        #   - throttle says we just enqueued for this event (rate-limit window)
        #   - details stream consumer lag > backpressure cap
        # Enqueue failure is logged at WARNING but the parent
        # refresh_live_event job still returns "completed" — the details
        # path must not feed back into root retry budget.
        if (
            report is not None
            and getattr(report, "details_pending", False)
            and not getattr(report, "finalized", False)
        ):
            self._maybe_enqueue_details(event_id=event_id, sport_slug=sport_slug, job=job, report=report)
        return "completed"

    def _reschedule_on_quarantine_skip(self, *, event_id: int, sport_slug: str) -> None:
        """Advance ``next_poll_at`` for an event we are about to skip via
        quarantine, so its hot/warm/cold zset score does not stay frozen at
        an already-due value and inflate ``oldest_hot_score_age``.

        The worker does not hold ``live_state_store``; it reaches it through
        the orchestrator (``ServiceApp.app`` == the PilotOrchestrator), which
        owns both ``live_worker`` and ``live_state_store``. Entirely
        best-effort and defensive: any missing attribute or raise is a silent
        no-op so the quarantine-skip return is never disturbed. The helper
        itself (LiveWorker.reschedule_after_transient_failure) is a no-op for
        finalized events and for states not in the hot/warm/cold lanes.
        """
        orchestrator = getattr(self, "orchestrator", None)
        if orchestrator is None:
            return
        live_worker = getattr(orchestrator, "live_worker", None)
        reschedule = getattr(live_worker, "reschedule_after_transient_failure", None)
        if not callable(reschedule):
            return
        live_state_store = getattr(orchestrator, "live_state_store", None)
        if live_state_store is None:
            return
        try:
            reschedule(
                sport_slug=sport_slug,
                event_id=event_id,
                live_state_store=live_state_store,
            )
        except Exception as exc:  # noqa: BLE001 — never break the skip path
            logger.debug(
                "Quarantine-skip reschedule failed for event_id=%s: %s",
                event_id,
                exc,
            )

    def _maybe_enqueue_details(self, *, event_id: int, sport_slug: str, job, report) -> None:
        if self.details_throttle is not None:
            try:
                allowed = self.details_throttle.should_enqueue(event_id=event_id)
            except Exception as exc:
                logger.warning(
                    "Details throttle check failed for event_id=%s: %s — skipping enqueue (fail-closed to avoid duplicate flooding)",
                    event_id,
                    exc,
                )
                return
            if not allowed:
                logger.debug(
                    "Skipping details enqueue (throttled): event_id=%s",
                    event_id,
                )
                return
        if self.details_backpressure_limit is not None:
            try:
                stream_lag = self._stream_backpressure_value(
                    STREAM_LIVE_DETAILS, GROUP_LIVE_DETAILS
                )
            except Exception as exc:
                logger.warning(
                    "Details backpressure lag check failed: %s — skipping (fail-closed)",
                    exc,
                )
                return
            if stream_lag is not None and stream_lag >= self.details_backpressure_limit:
                logger.info(
                    "details_backpressure_skip: event_id=%s stream_lag=%s limit=%s",
                    event_id,
                    stream_lag,
                    self.details_backpressure_limit,
                )
                return
        details_context = getattr(report, "details_context", None) or {}
        details_job = JobEnvelope.create(
            job_type=JOB_REFRESH_LIVE_EVENT_DETAILS,
            sport_slug=sport_slug,
            entity_type="event",
            entity_id=event_id,
            scope="details",
            params={
                "details_context": details_context,
                "live_dispatch_tier": job.params.get("live_dispatch_tier"),
                "parent_job_id": job.job_id,
            },
            priority=2,
            trace_id=job.trace_id,
        )
        try:
            self.queue.publish(STREAM_LIVE_DETAILS, _job_to_stream_payload(details_job))
        except Exception as exc:
            logger.warning(
                "Failed to enqueue refresh_live_event_details for event_id=%s: %s — root job still completes",
                event_id,
                exc,
            )

    def _maybe_enqueue_edges(self, *, event_id: int, sport_slug: str, job) -> None:
        """Publish a follow-up ``refresh_live_event`` (full hydration) to
        ``stream:etl:live_warm`` after a tier_1 root-only run completed
        cleanly. Throttled per-event and bounded by warm-stream consumer lag.

        The follow-up has ``hydration_mode`` UNSET in params (defaults to
        ``live_delta``) so the live_warm consumer runs the legacy ROOT +
        edges + details pipeline. Yes — this re-fetches the same
        ``/event`` endpoint that the root-only run just persisted; the
        cost is one cheap (~1 s) duplicate fetch in exchange for keeping
        the worker layer trivially correct (no new edges-only orchestrator
        path, no new stream/group/worker class)."""
        if self.edges_throttle is not None:
            try:
                allowed = self.edges_throttle.should_enqueue(event_id=event_id)
            except Exception as exc:
                logger.warning(
                    "Edges throttle check failed for event_id=%s: %s — skipping enqueue (fail-closed to avoid duplicate flooding)",
                    event_id,
                    exc,
                )
                return
            if not allowed:
                logger.debug(
                    "Skipping edges enqueue (throttled): event_id=%s",
                    event_id,
                )
                return
        if self.edges_backpressure_limit is not None:
            try:
                stream_lag = self._stream_backpressure_value(
                    STREAM_LIVE_WARM, GROUP_LIVE_WARM
                )
            except Exception as exc:
                logger.warning(
                    "Edges backpressure lag check failed: %s — skipping (fail-closed)",
                    exc,
                )
                return
            if stream_lag is not None and stream_lag >= self.edges_backpressure_limit:
                logger.info(
                    "edges_backpressure_skip: event_id=%s stream_lag=%s limit=%s",
                    event_id,
                    stream_lag,
                    self.edges_backpressure_limit,
                )
                return
        edges_job = JobEnvelope.create(
            job_type=JOB_REFRESH_LIVE_EVENT,
            sport_slug=sport_slug,
            entity_type="event",
            entity_id=event_id,
            scope="warm",
            params={
                "live_dispatch_tier": job.params.get("live_dispatch_tier"),
                "parent_job_id": job.job_id,
                "edges_followup": True,
            },
            priority=1,
            trace_id=job.trace_id,
        )
        try:
            self.queue.publish(STREAM_LIVE_WARM, _job_to_stream_payload(edges_job))
        except Exception as exc:
            logger.warning(
                "Failed to enqueue edges refresh_live_event for event_id=%s: %s — root-only job still completes",
                event_id,
                exc,
            )

    def _stream_backpressure_value(self, stream_name: str, group_name: str) -> int | None:
        """Return consumer lag for backpressure, with XLEN fallback.

        Redis Streams keep acknowledged entries until XTRIM/MAXLEN, so XLEN
        can be huge while consumers are fully caught up. Using XLEN here
        blocks live_warm/live_details fanout indefinitely after a high-volume
        day. Prefer XINFO GROUPS lag; fall back to XLEN only for old fakes or
        backends that cannot report group lag.
        """

        group_info = getattr(self.queue, "group_info", None)
        if callable(group_info):
            try:
                info = group_info(stream_name, group_name)
            except Exception:
                info = None
            lag = getattr(info, "lag", None) if info is not None else None
            if lag is not None:
                try:
                    return int(lag)
                except (TypeError, ValueError):
                    return None

        backend = getattr(self.queue, "backend", None) or getattr(self.queue, "_backend", None)
        if backend is not None:
            xinfo_groups = getattr(backend, "xinfo_groups", None)
            if callable(xinfo_groups):
                try:
                    for row in xinfo_groups(stream_name) or ():
                        name = row.get("name") if isinstance(row, dict) else None
                        if str(name or "") != str(group_name):
                            continue
                        lag = row.get("lag") if isinstance(row, dict) else None
                        if lag is None:
                            break
                        return int(lag)
                except Exception:
                    return None

        return self._stream_length(stream_name)

    def _stream_length(self, stream_name: str) -> int | None:
        backend = getattr(self.queue, "backend", None) or getattr(self.queue, "_backend", None)
        if backend is None:
            return None
        xlen = getattr(backend, "xlen", None)
        if not callable(xlen):
            return None
        try:
            value = xlen(stream_name)
        except Exception:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

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


def _coalesce_live_refresh_batch(entries: tuple[StreamEntry, ...]) -> BatchDispatchPlan:
    decoded_jobs = tuple(decode_stream_job(entry) for entry in entries)
    latest_index_by_event_id: dict[int, int] = {}
    for index, job in enumerate(decoded_jobs):
        if job.job_type != JOB_REFRESH_LIVE_EVENT or job.entity_id is None:
            continue
        latest_index_by_event_id[int(job.entity_id)] = index

    kept_entries: list[StreamEntry] = []
    stale_entries: list[StreamEntry] = []
    coalesced_counts: dict[str, int] = {}
    for index, (entry, job) in enumerate(zip(entries, decoded_jobs, strict=False)):
        event_id = job.entity_id
        if (
            job.job_type == JOB_REFRESH_LIVE_EVENT
            and event_id is not None
            and latest_index_by_event_id.get(int(event_id)) != index
        ):
            stale_entries.append(entry)
            event_key = str(int(event_id))
            coalesced_counts[event_key] = coalesced_counts.get(event_key, 0) + 1
            continue
        kept_entries.append(entry)

    return BatchDispatchPlan(
        entries_to_process=tuple(kept_entries),
        stale_entries_to_ack=tuple(stale_entries),
        coalesced_counts=tuple(sorted(coalesced_counts.items())),
    )


def _lane_stream_group(lane: str) -> tuple[str, str]:
    normalized = str(lane).strip().lower()
    if normalized == "hot":
        return STREAM_LIVE_HOT, GROUP_LIVE_HOT
    if normalized == "warm":
        return STREAM_LIVE_WARM, GROUP_LIVE_WARM
    if normalized == "tier_1":
        return STREAM_LIVE_TIER_1, GROUP_LIVE_TIER_1
    if normalized == "tier_2":
        return STREAM_LIVE_TIER_2, GROUP_LIVE_TIER_2
    if normalized == "tier_3":
        return STREAM_LIVE_TIER_3, GROUP_LIVE_TIER_3
    raise ValueError(f"Unsupported live lane: {lane!r}")


def _lane_concurrency_env_names(lane: str) -> tuple[str, ...]:
    normalized = str(lane).strip().lower()
    if normalized == "tier_1":
        return ("SOFASCORE_LIVE_TIER_1_WORKER_MAX_CONCURRENCY", "SOFASCORE_LIVE_HOT_WORKER_MAX_CONCURRENCY")
    if normalized == "tier_2":
        return ("SOFASCORE_LIVE_TIER_2_WORKER_MAX_CONCURRENCY",)
    if normalized == "tier_3":
        return ("SOFASCORE_LIVE_TIER_3_WORKER_MAX_CONCURRENCY",)
    if normalized == "hot":
        return ("SOFASCORE_LIVE_HOT_WORKER_MAX_CONCURRENCY",)
    return ("SOFASCORE_LIVE_WARM_WORKER_MAX_CONCURRENCY",)


def _job_to_stream_payload(job: JobEnvelope) -> dict[str, object]:
    """Serialise a JobEnvelope into the same flat dict shape the planner uses
    when XADDing to a stream. Kept locally (rather than imported from
    services.planner_daemon) so the live worker module has no service-layer
    dependency."""
    return {
        "job_id": job.job_id,
        "job_type": job.job_type,
        "sport_slug": job.sport_slug or "",
        "entity_type": job.entity_type or "",
        "entity_id": job.entity_id if job.entity_id is not None else "",
        "scope": job.scope or "",
        "params_json": json.dumps(job.params, ensure_ascii=True, sort_keys=True, separators=(",", ":")),
        "priority": job.priority,
        "scheduled_at": job.scheduled_at,
        "attempt": job.attempt,
        "parent_job_id": job.parent_job_id or "",
        "trace_id": job.trace_id or "",
        "capability_hint": job.capability_hint or "",
        "idempotency_key": job.idempotency_key,
    }


def _is_high_value_tier_1(job) -> bool:
    """Return True iff ``job`` is a marquee tier_1 event that must never be
    quarantined (P0(c.3) carve-out).

    Signal priority:
      1. ``params['live_dispatch_tier'] == 'tier_1'`` — the resolved verdict
         the planner / discovery worker persist on EVERY live job. This is
         the only marquee signal reliably present on the dominant planner
         refresh path, and resolve_live_dispatch_tier already folds football
         detailId==1 and tournament user_count>=LIVE_TIER_1_MIN_USER_COUNT
         (6500) into it.
      2. raw ``params['detail_id'] == 1`` — honoured if a future publisher
         threads it (football top-tier coverage).
      3. raw ``params['user_count'] >= LIVE_TIER_1_MIN_USER_COUNT`` — same.

    Defensive: unknown / malformed params → False (event stays eligible for
    quarantine, i.e. the safe default that preserves the bad-route brake).
    """
    params = getattr(job, "params", None)
    if not isinstance(params, dict):
        return False
    tier = params.get("live_dispatch_tier")
    if isinstance(tier, str) and tier.strip().lower() == "tier_1":
        return True
    detail_id = _coerce_int(params.get("detail_id"))
    if detail_id == 1:
        return True
    user_count = _coerce_int(params.get("user_count"))
    if user_count is not None:
        from ..live_dispatch_policy import LIVE_TIER_1_MIN_USER_COUNT

        if user_count >= LIVE_TIER_1_MIN_USER_COUNT:
            return True
    return False


def _coerce_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _is_retryable_for_quarantine(exc: Exception) -> bool:
    """Return True iff ``exc`` is the retryable class that should feed the
    quarantine counter.

    Imports ``is_retryable_worker_error`` lazily so this module does not
    grow an import-time dependency on ``services.retry_policy`` (which
    in turn imports ``curl_cffi``). On import failure we fail closed —
    return False — so quarantine never triggers on unclassifiable errors
    and a misconfigured environment cannot silently start quarantining
    arbitrary events.
    """
    try:
        from ..services.retry_policy import is_retryable_worker_error
    except Exception:  # pragma: no cover - extremely defensive
        return False
    try:
        return bool(is_retryable_worker_error(exc))
    except Exception:  # pragma: no cover - defensive
        return False


def _env_flag_enabled(name: str) -> bool:
    """Return True iff env var ``name`` is set to a truthy value.

    Truthy: ``1``, ``true``, ``yes``, ``on`` (case-insensitive). Anything
    else (unset, empty, ``0``, etc.) returns False. Used for boolean
    rollout flags like ``LIVE_TIER_1_ROOT_ONLY`` that gate per-worker
    behaviour at handle() time without restarts of unrelated lanes.
    """
    raw = os.environ.get(name)
    if raw in (None, ""):
        return False
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_optional_positive_int(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return None
    try:
        value = int(str(raw))
    except ValueError:
        logger.warning("Invalid %s=%r; ignoring (no backpressure cap)", name, raw)
        return None
    if value < 1:
        return None
    return value
