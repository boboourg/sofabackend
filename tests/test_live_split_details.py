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

    def test_hybrid_app_run_event_details_does_not_use_replay_pipeline(self) -> None:
        """Regression for canary v2 — ``HybridApp.run_event_details`` was
        initially implemented via the run_event prefetch+commit+persist
        pattern. Persist phase calls ``orchestrator.run_event`` to replay,
        but the prefetch ran ``run_event_details`` only (no ROOT
        outcome). Replay then raised ``RuntimeError: No prefetched fetch
        outcome available for task=('hydrate_event_root', ...)``.

        Source-level invariant: the details path must NOT use the replay
        pipeline. It must drive ``orchestrator.run_event_details``
        directly under an immediate (non-deferred) ``FetchExecutor``."""

        import ast
        import inspect
        import textwrap
        from schema_inspector.cli import HybridApp

        # ``inspect.getsource`` preserves the original 4-space class-method
        # indent which trips ``ast.parse`` with IndentationError. Dedent
        # before parsing.
        source = textwrap.dedent(inspect.getsource(HybridApp.run_event_details))
        tree = ast.parse(source)

        # Walk AST to find every Call node and inspect the callee
        # name/attribute path. Source-text scans are unreliable because
        # the docstring legitimately mentions ``_persist_prefetched_run``
        # / ``run_event`` as a description of what this method must
        # *not* do.
        called_attrs: list[str] = []
        called_keywords: list[str] = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Attribute):
                    called_attrs.append(func.attr)
                elif isinstance(func, ast.Name):
                    called_attrs.append(func.id)
                for kw in node.keywords:
                    if kw.arg:
                        called_keywords.append(kw.arg)

        self.assertNotIn(
            "_prefetch_event_run_details",
            called_attrs,
            "run_event_details must not call _prefetch_event_run_details "
            "(removed in canary v3 — the replay pattern was the v2 bug).",
        )
        self.assertNotIn(
            "_persist_prefetched_run",
            called_attrs,
            "run_event_details must not call _persist_prefetched_run — "
            "that helper invokes orchestrator.run_event(...) for replay, "
            "which fails when prefetch only ran details (no ROOT outcome).",
        )
        # FetchExecutor must not be constructed with write_mode="deferred"
        # — details writes are immediate. Search Call nodes for the
        # ``write_mode`` kwarg with the specific value "deferred".
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if kw.arg == "write_mode" and isinstance(kw.value, ast.Constant):
                        self.assertNotEqual(
                            kw.value.value,
                            "deferred",
                            "FetchExecutor must NOT be deferred mode in details path "
                            "(replay pipeline is unsupported here).",
                        )
        self.assertIn(
            "run_event_details",
            called_attrs,
            "run_event_details must drive orchestrator.run_event_details.",
        )
        self.assertNotIn(
            "run_event",
            called_attrs,
            "run_event_details must NOT call orchestrator.run_event "
            "(canary v2 bug — replay pipeline tries ROOT fetch which "
            "was never prefetched).",
        )


class HybridAppRunEventDetailsIntegrationTests(unittest.IsolatedAsyncioTestCase):
    """Integration test that drives the full ``HybridApp.run_event_details``
    code path with a spy on ``PilotOrchestrator`` to assert which
    orchestrator method gets called. Catches the v1+v2 canary failure
    modes that the unit tests with mock orchestrators missed:

    * v1 — AttributeError because HybridApp lacked run_event_details
    * v2 — RuntimeError because run_event_details internally invoked
      run_event via the replay pipeline
    """

    async def test_hybrid_app_run_event_details_calls_orchestrator_run_event_details_only(self) -> None:
        import schema_inspector.cli as hybrid_cli
        from schema_inspector.runtime import RuntimeConfig
        from unittest import mock as _mock

        run_event_calls: list[dict] = []
        run_event_details_calls: list[dict] = []
        constructed_orchestrators: list[dict] = []

        original_pilot_orchestrator = hybrid_cli.PilotOrchestrator

        class _SpyOrchestrator:
            """Drop-in stand-in for ``PilotOrchestrator``. Records
            constructor kwargs so we can verify the executor is *not* a
            replay executor (the v2 bug path), and records every
            ``run_event(...)`` and ``run_event_details(...)`` call."""

            def __init__(self, **kwargs):
                constructed_orchestrators.append(kwargs)
                self._kwargs = kwargs

            async def run_event(self, **kwargs):
                run_event_calls.append(kwargs)
                return None

            async def run_event_details(self, **kwargs):
                run_event_details_calls.append(kwargs)
                return None

            @property
            def freshness_skip_keys(self):
                return frozenset()

        app = hybrid_cli.HybridApp(
            database=_HybridStubDatabase(),
            runtime_config=RuntimeConfig(require_proxy=False),
            redis_backend=None,
        )

        async def _noop_ensure_endpoint_registry(*args, **kwargs):
            del args, kwargs
            return None

        async def _resolve_sport(*args, **kwargs):
            del args, kwargs
            return "football"

        with _mock.patch.object(hybrid_cli, "PilotOrchestrator", _SpyOrchestrator), \
                _mock.patch.object(app, "ensure_endpoint_registry", side_effect=_noop_ensure_endpoint_registry), \
                _mock.patch.object(app, "resolve_event_sport_slug", side_effect=_resolve_sport):
            result = await app.run_event_details(
                event_id=15345941,
                sport_slug="football",
                context={
                    "status_type": "inprogress",
                    "home_team_id": 1,
                    "away_team_id": 2,
                    "has_xg": True,
                    "effective_hydration_mode": "live_delta",
                },
            )

        # Critical assertions for the v1/v2 canary regressions.
        self.assertEqual(
            len(run_event_calls), 0,
            "HybridApp.run_event_details MUST NOT invoke "
            "orchestrator.run_event (canary v2 bug — _persist_prefetched_run "
            "replays via run_event and tries to fetch ROOT which was never "
            "prefetched).",
        )
        self.assertEqual(
            len(run_event_details_calls), 1,
            "HybridApp.run_event_details MUST drive "
            "orchestrator.run_event_details exactly once.",
        )
        self.assertEqual(run_event_details_calls[0]["event_id"], 15345941)
        self.assertEqual(run_event_details_calls[0]["sport_slug"], "football")
        self.assertEqual(run_event_details_calls[0]["context"]["status_type"], "inprogress")

        # Verify the constructed orchestrator was NOT given a
        # ReplayFetchExecutor — the v2 bug. The fetch_executor must be a
        # real (immediate-write) FetchExecutor instance.
        self.assertEqual(len(constructed_orchestrators), 1)
        executor = constructed_orchestrators[0]["fetch_executor"]
        self.assertEqual(
            type(executor).__name__,
            "FetchExecutor",
            "run_event_details must use a real FetchExecutor, not "
            "ReplayFetchExecutor (which only replays prefetched task keys "
            "and would raise on ROOT replay).",
        )


class _HybridStubDatabase:
    """Minimal Database-shaped stub for HybridApp construction +
    transaction context manager. Doesn't actually connect to anything."""

    def __init__(self) -> None:
        self.connection = _StubConnection()
        self.transaction_calls = 0

    def transaction(self):
        self.transaction_calls += 1
        return _AsyncCM(self.connection)

    def connection_factory(self):
        return _AsyncCM(self.connection)


class _StubConnection:
    async def fetch(self, *args, **kwargs):
        del args, kwargs
        return []

    async def fetchrow(self, *args, **kwargs):
        del args, kwargs
        return None

    async def execute(self, *args, **kwargs):
        del args, kwargs
        return None


class _AsyncCM:
    def __init__(self, value):
        self._value = value

    async def __aenter__(self):
        return self._value

    async def __aexit__(self, exc_type, exc, tb):
        del exc_type, exc, tb
        return None


if __name__ == "__main__":
    unittest.main()
