"""Tests for the P0(a) split-details rollout.

Behaviour covered:

* live-tier worker returns "completed" after ROOT + edges without waiting
  on details fanout (orchestrator returns ``details_pending=True``);
* details are enqueued onto ``stream:etl:live_details`` exactly once per
  throttle window per event (no duplicate flooding under 5s root poll
  cadence);
* details-stream backpressure cap blocks new enqueues while still letting
  the root path complete cleanly;
* the dedicated details worker uses ``LiveEventDetailsInFlightStore`` to
  coalesce concurrent fanouts for the same event;
* details fanout failure surfaces as ``completed_with_errors`` (or
  ``"completed_with_errors"`` on uncaught exception) and DOES NOT raise
  ``RetryableJobError`` back into the worker runtime — root retry budget
  must stay independent of details health;
* legacy in-line behaviour is preserved when
  ``LIVE_SPLIT_DETAILS_FANOUT`` is unset / 0;
* terminal/finalized events skip the details enqueue;
* the new consumer group is wired to the new stream
  (``stream:etl:live_details`` / ``cg:live_details``).
"""
from __future__ import annotations

import json
import os
import unittest
from contextlib import contextmanager
from dataclasses import dataclass

from schema_inspector.queue.streams import (
    GROUP_LIVE_DETAILS,
    STREAM_LIVE_DETAILS,
    STREAM_LIVE_TIER_1,
    StreamEntry,
)


@dataclass
class _FakeReport:
    """Stand-in for ``PilotRunReport`` — only the fields the worker reads."""

    fetch_outcomes: tuple = ()
    parse_results: tuple = ()
    finalized: bool = False
    details_pending: bool = False
    details_context: dict | None = None


@dataclass
class _FakeOutcome:
    classification: str = "success_json"


class _FakeRedisBackend:
    """Minimal Redis-shaped in-memory backend for SET NX EX, GET, DELETE."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.streams: dict[str, list[tuple[str, dict]]] = {}

    def set(self, key, value, *, nx=False, px=None, ex=None):
        if nx and key in self.values:
            return False
        self.values[key] = str(value)
        return True

    def get(self, key):
        return self.values.get(key)

    def delete(self, key):
        return self.values.pop(key, None) is not None and 1 or 0

    def expire(self, key, seconds):
        return key in self.values

    def xadd(self, stream, payload):
        msg_id = f"{stream}-{len(self.streams.get(stream, []))+1}"
        self.streams.setdefault(stream, []).append((msg_id, dict(payload)))
        return msg_id

    def xlen(self, stream):
        return len(self.streams.get(stream, []))


class _RecordingQueue:
    """Queue facade that records ``publish(stream, payload)`` calls."""

    def __init__(self, *, backend: _FakeRedisBackend | None = None) -> None:
        self.backend = backend or _FakeRedisBackend()
        self.published: list[tuple[str, dict]] = []

    def publish(self, stream, payload):
        self.published.append((stream, dict(payload)))
        self.backend.xadd(stream, payload)
        return f"{stream}-msg-{len(self.published)}"

    def read_group(self, *args, **kwargs):
        del args, kwargs
        return ()

    def ack(self, *args, **kwargs):
        del args, kwargs
        return 0


@dataclass
class _FakeJob:
    """Replay of decode_stream_job result for tests."""
    job_id: str = "job-1"
    job_type: str = "refresh_live_event"
    sport_slug: str = "football"
    entity_id: int = 7001
    scope: str = "tier_1"
    params: dict = None
    trace_id: str = "trace-1"

    def __post_init__(self):
        if self.params is None:
            self.params = {}


class _OrchestratorReturningDetailsPending:
    """Orchestrator double whose ``run_event`` returns a report with
    ``details_pending=True`` and an arbitrary context dict."""

    def __init__(self, *, finalized: bool = False) -> None:
        self.calls: list[tuple[int, str, str]] = []
        self.finalized = finalized

    async def run_event(self, *, event_id, sport_slug, hydration_mode):
        self.calls.append((event_id, sport_slug, hydration_mode))
        return _FakeReport(
            fetch_outcomes=(),
            parse_results=(),
            finalized=self.finalized,
            details_pending=not self.finalized,
            details_context={
                "status_type": "inprogress",
                "home_team_id": 1,
                "away_team_id": 2,
                "has_xg": True,
                "effective_hydration_mode": "live_delta",
                "core_only": False,
            },
        )


@contextmanager
def _patched_env(**kv):
    previous = {k: os.environ.get(k) for k in kv}
    try:
        for k, v in kv.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = str(v)
        yield
    finally:
        for k, v in previous.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _live_tier_1_entry(event_id: int = 7001, message_id: str = "1-1") -> StreamEntry:
    return StreamEntry(
        stream=STREAM_LIVE_TIER_1,
        message_id=message_id,
        values={
            "job_id": f"job-live-{event_id}",
            "job_type": "refresh_live_event",
            "sport_slug": "football",
            "event_id": str(event_id),
            "lane": "tier_1",
            "attempt": "1",
            "params_json": json.dumps({"hydration_mode": "live_delta", "live_dispatch_tier": "tier_1"}),
        },
    )


# ---------------------------------------------------------------------------
# 1. Live-tier worker returns "completed" after ROOT/edges; details get
#    enqueued onto a separate stream rather than running inline.
# ---------------------------------------------------------------------------
class LiveTierWorkerSplitTests(unittest.IsolatedAsyncioTestCase):
    async def test_root_returns_completed_and_publishes_details_job(self) -> None:
        from schema_inspector.queue.live_inflight import LiveEventInFlightStore
        from schema_inspector.queue.live_details_throttle import LiveDetailsThrottle
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)

        with _patched_env(LIVE_SPLIT_DETAILS_FANOUT="1"):
            worker = LiveWorkerService(
                orchestrator=_OrchestratorReturningDetailsPending(),
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_1",
                consumer="worker-live-tier-1-1",
                in_flight_store=LiveEventInFlightStore(backend, ttl_ms=60_000),
                details_throttle=LiveDetailsThrottle(backend, interval_seconds=30),
            )

            result = await worker.handle(_live_tier_1_entry())

        self.assertEqual(result, "completed")
        # Exactly one details job published.
        details_publishes = [s for s, _ in queue.published if s == STREAM_LIVE_DETAILS]
        self.assertEqual(len(details_publishes), 1)
        # The published payload carries a serialised details_context.
        _, payload = next(p for p in queue.published if p[0] == STREAM_LIVE_DETAILS)
        self.assertEqual(payload["job_type"], "refresh_live_event_details")
        self.assertEqual(payload["entity_id"], 7001)
        params = json.loads(payload["params_json"])
        self.assertIn("details_context", params)
        self.assertEqual(params["details_context"]["status_type"], "inprogress")
        self.assertEqual(params["live_dispatch_tier"], "tier_1")

    async def test_throttle_collapses_concurrent_enqueues_for_same_event(self) -> None:
        from schema_inspector.queue.live_inflight import LiveEventInFlightStore
        from schema_inspector.queue.live_details_throttle import LiveDetailsThrottle
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)

        with _patched_env(LIVE_SPLIT_DETAILS_FANOUT="1"):
            worker = LiveWorkerService(
                orchestrator=_OrchestratorReturningDetailsPending(),
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_1",
                consumer="worker-live-tier-1-1",
                in_flight_store=None,  # no critical lock to keep concurrency simple
                details_throttle=LiveDetailsThrottle(backend, interval_seconds=30),
            )

            for i in range(3):
                await worker.handle(_live_tier_1_entry(message_id=f"1-{i}"))

        details_publishes = [s for s, _ in queue.published if s == STREAM_LIVE_DETAILS]
        # Only the first call publishes; the next two are throttled.
        self.assertEqual(len(details_publishes), 1)

    async def test_details_backpressure_skips_enqueue_but_root_completes(self) -> None:
        from schema_inspector.queue.live_inflight import LiveEventInFlightStore
        from schema_inspector.queue.live_details_throttle import LiveDetailsThrottle
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        # Pre-fill the details stream past the cap.
        for i in range(10):
            backend.xadd(STREAM_LIVE_DETAILS, {"k": str(i)})
        queue = _RecordingQueue(backend=backend)

        with _patched_env(LIVE_SPLIT_DETAILS_FANOUT="1"):
            worker = LiveWorkerService(
                orchestrator=_OrchestratorReturningDetailsPending(),
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_1",
                consumer="worker-live-tier-1-1",
                in_flight_store=None,
                details_throttle=LiveDetailsThrottle(backend, interval_seconds=30),
                details_backpressure_limit=5,
            )

            result = await worker.handle(_live_tier_1_entry())

        self.assertEqual(result, "completed")
        # No new details publish landed on the details stream — backpressure
        # rejected it. The pre-existing 10 stay; ``queue.published`` only
        # records publishes that went through the queue facade.
        details_publishes = [s for s, _ in queue.published if s == STREAM_LIVE_DETAILS]
        self.assertEqual(len(details_publishes), 0)

    async def test_split_disabled_keeps_legacy_inline_path_no_details_publish(self) -> None:
        from schema_inspector.queue.live_inflight import LiveEventInFlightStore
        from schema_inspector.queue.live_details_throttle import LiveDetailsThrottle
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)

        # Flag NOT set — orchestrator must NOT have returned details_pending=True
        # in real run_event (gated by env). Even if a test fake returns
        # details_pending=True, with the flag off the live worker still
        # publishes (the flag check is in the orchestrator, not the worker
        # — this is by design, the worker trusts the orchestrator's
        # signal). Verify that an orchestrator returning a plain dict
        # (legacy fake style) results in NO details enqueue:
        class _LegacyOrchestrator:
            async def run_event(self, **kwargs):
                return {"event_id": kwargs["event_id"]}

        with _patched_env(LIVE_SPLIT_DETAILS_FANOUT=None):
            worker = LiveWorkerService(
                orchestrator=_LegacyOrchestrator(),
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_1",
                consumer="worker-live-tier-1-1",
                in_flight_store=None,
                details_throttle=LiveDetailsThrottle(backend, interval_seconds=30),
            )

            result = await worker.handle(_live_tier_1_entry())

        self.assertEqual(result, "completed")
        details_publishes = [s for s, _ in queue.published if s == STREAM_LIVE_DETAILS]
        self.assertEqual(len(details_publishes), 0)

    async def test_terminal_event_does_not_enqueue_details(self) -> None:
        from schema_inspector.queue.live_details_throttle import LiveDetailsThrottle
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)

        with _patched_env(LIVE_SPLIT_DETAILS_FANOUT="1"):
            worker = LiveWorkerService(
                orchestrator=_OrchestratorReturningDetailsPending(finalized=True),
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_1",
                consumer="worker-live-tier-1-1",
                in_flight_store=None,
                details_throttle=LiveDetailsThrottle(backend, interval_seconds=30),
            )

            result = await worker.handle(_live_tier_1_entry())

        self.assertEqual(result, "completed")
        # Finalized → details enqueue suppressed (final sweep already
        # ran inside run_event).
        details_publishes = [s for s, _ in queue.published if s == STREAM_LIVE_DETAILS]
        self.assertEqual(len(details_publishes), 0)


# ---------------------------------------------------------------------------
# 2. Details worker behaviour under the new stream/group/lock.
# ---------------------------------------------------------------------------
class LiveDetailsWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_details_worker_uses_correct_stream_and_group(self) -> None:
        from schema_inspector.queue.live_inflight import LiveEventDetailsInFlightStore
        from schema_inspector.workers.live_details_worker_service import LiveDetailsWorkerService

        backend = _FakeRedisBackend()
        worker = LiveDetailsWorkerService(
            orchestrator=_NoopDetailsOrchestrator(),
            queue=_RecordingQueue(backend=backend),
            consumer="worker-live-details-1",
            details_in_flight_store=LiveEventDetailsInFlightStore(backend, ttl_ms=60_000),
        )

        self.assertEqual(worker.runtime.stream, STREAM_LIVE_DETAILS)
        self.assertEqual(worker.runtime.group, GROUP_LIVE_DETAILS)
        self.assertEqual(worker.runtime.consumer, "worker-live-details-1")

    async def test_details_worker_lock_prevents_duplicate_fanout(self) -> None:
        from schema_inspector.queue.live_inflight import LiveEventDetailsInFlightStore
        from schema_inspector.workers.live_details_worker_service import LiveDetailsWorkerService

        backend = _FakeRedisBackend()
        in_flight = LiveEventDetailsInFlightStore(backend, ttl_ms=60_000)
        # Pre-claim by another worker so this handle() must coalesce.
        self.assertTrue(in_flight.claim(event_id=7001, owner="other-details-worker"))

        orchestrator = _NoopDetailsOrchestrator()
        worker = LiveDetailsWorkerService(
            orchestrator=orchestrator,
            queue=_RecordingQueue(backend=backend),
            consumer="worker-live-details-1",
            details_in_flight_store=in_flight,
        )

        result = await worker.handle(_details_entry(event_id=7001, message_id="d-1"))

        self.assertEqual(result, "coalesced_details")
        # Orchestrator MUST NOT have been called — fanout coalesced.
        self.assertEqual(orchestrator.calls, [])

    async def test_details_worker_completed_with_errors_on_bad_outcome(self) -> None:
        from schema_inspector.workers.live_details_worker_service import LiveDetailsWorkerService

        # Orchestrator returns a report with a network_error outcome — the
        # details worker must surface that as ``completed_with_errors`` so
        # ops can see it without it feeding back into the root retry queue.
        class _ErrorReportOrchestrator:
            async def run_event_details(self, *, event_id, sport_slug, context):
                return _FakeReport(
                    fetch_outcomes=(_FakeOutcome(classification="network_error"),),
                )

        worker = LiveDetailsWorkerService(
            orchestrator=_ErrorReportOrchestrator(),
            queue=_RecordingQueue(),
            consumer="worker-live-details-1",
            details_in_flight_store=None,
        )

        result = await worker.handle(_details_entry(event_id=7002, message_id="d-2"))

        self.assertEqual(result, "completed_with_errors")

    async def test_details_worker_completed_with_errors_on_uncaught_exception(self) -> None:
        from schema_inspector.workers.live_details_worker_service import LiveDetailsWorkerService

        class _RaisingOrchestrator:
            calls: list = []

            async def run_event_details(self, *, event_id, sport_slug, context):
                self.calls.append(event_id)
                raise RuntimeError("boom — simulated transport failure")

        orchestrator = _RaisingOrchestrator()
        worker = LiveDetailsWorkerService(
            orchestrator=orchestrator,
            queue=_RecordingQueue(),
            consumer="worker-live-details-1",
            details_in_flight_store=None,
        )

        # MUST NOT raise. Worker traps and surfaces as completed_with_errors.
        result = await worker.handle(_details_entry(event_id=7003, message_id="d-3"))

        self.assertEqual(result, "completed_with_errors")
        self.assertEqual(orchestrator.calls, [7003])

    async def test_details_worker_completed_when_all_outcomes_clean(self) -> None:
        from schema_inspector.workers.live_details_worker_service import LiveDetailsWorkerService

        class _CleanOrchestrator:
            async def run_event_details(self, *, event_id, sport_slug, context):
                return _FakeReport(
                    fetch_outcomes=(
                        _FakeOutcome(classification="success_json"),
                        _FakeOutcome(classification="success_empty_json"),
                        _FakeOutcome(classification="not_found"),
                    ),
                )

        worker = LiveDetailsWorkerService(
            orchestrator=_CleanOrchestrator(),
            queue=_RecordingQueue(),
            consumer="worker-live-details-1",
            details_in_flight_store=None,
        )

        result = await worker.handle(_details_entry(event_id=7004, message_id="d-4"))
        self.assertEqual(result, "completed")


# ---------------------------------------------------------------------------
# 3. Helpers
# ---------------------------------------------------------------------------
class _NoopDelayedScheduler:
    def schedule(self, *args, **kwargs):
        del args, kwargs


class _NoopDetailsOrchestrator:
    def __init__(self) -> None:
        self.calls: list = []

    async def run_event_details(self, *, event_id, sport_slug, context):
        self.calls.append(event_id)
        return _FakeReport(fetch_outcomes=())


def _details_entry(*, event_id: int, message_id: str) -> StreamEntry:
    return StreamEntry(
        stream=STREAM_LIVE_DETAILS,
        message_id=message_id,
        values={
            "job_id": f"job-details-{event_id}",
            "job_type": "refresh_live_event_details",
            "sport_slug": "football",
            "entity_id": str(event_id),
            "scope": "details",
            "attempt": "1",
            "params_json": json.dumps(
                {
                    "details_context": {
                        "status_type": "inprogress",
                        "home_team_id": 1,
                        "away_team_id": 2,
                    },
                    "live_dispatch_tier": "tier_1",
                }
            ),
        },
    )


class HybridAppRunEventDetailsExposureTests(unittest.TestCase):
    """Regression: ``LiveDetailsWorkerService`` is wired with
    ``orchestrator=ServiceApp.app`` (= ``HybridApp``). The new public
    method ``run_event_details`` must therefore exist on ``HybridApp``,
    not only on ``PilotOrchestrator``. The first canary roll-out hit
    ``AttributeError: 'HybridApp' object has no attribute
    'run_event_details'`` — this test pins down the exposure."""

    def test_hybrid_app_class_has_run_event_details(self) -> None:
        from schema_inspector.cli import HybridApp

        self.assertTrue(
            hasattr(HybridApp, "run_event_details"),
            "HybridApp must expose run_event_details so the live-details "
            "worker can call orchestrator.run_event_details(...) without "
            "AttributeError.",
        )


if __name__ == "__main__":
    unittest.main()
