"""Tests for the P0(b) tier_1 root-only fast path rollout.

Behaviour covered:

* Orchestrator-level: ``run_event(hydration_mode="root_only")`` calls
  ``_fetch_and_parse`` exactly once for ``EVENT_DETAIL_ENDPOINT``, then
  returns a ``PilotRunReport`` with ``edges_pending=True`` (or
  ``finalized=True`` for terminal status payloads). It does NOT invoke
  ``planner.expand`` and does NOT fetch any non-root endpoint.
* Terminal-status root-only run: ``finalized=True``,
  ``edges_pending=False``, ``finalize_event`` was called on
  ``live_worker``, ``_record_terminal_state`` was awaited.
* Worker-level: with ``LIVE_TIER_1_ROOT_ONLY=1`` AND lane=tier_1, the
  worker:
   - claims ``live:root_inflight:{event_id}`` (NOT
     ``live:refresh_inflight``) — so a long-running full refresh on
     live_warm cannot block tier_1 root.
   - calls ``orchestrator.run_event(hydration_mode="root_only")``.
   - on ``edges_pending=True``, publishes a follow-up
     ``refresh_live_event`` to ``stream:etl:live_warm`` (throttled +
     backpressure-aware).
* Flag OFF preserves legacy behaviour exactly (claims
  ``live:refresh_inflight``, hydration_mode resolved from job params,
  no edges-followup publish).
* Flag ON applies ONLY to lane=tier_1; tier_2/tier_3 still take the
  legacy lock and run normal hydration mode.
* Edges enqueue is throttled (default 60 s) and backpressure-aware
  (skipped when warm-stream length exceeds limit).
* Edges enqueue failure does NOT raise out of the worker — root-only
  job still completes.
* AST regression: ``LiveWorkerService.handle`` actually reads
  ``LIVE_TIER_1_ROOT_ONLY`` and dispatches to ``run_event`` with the
  ``root_only`` hydration mode — guards against silent removal in
  future refactors.
"""
from __future__ import annotations

import json
import os
import unittest
from contextlib import contextmanager
from dataclasses import dataclass

from schema_inspector.queue.streams import (
    STREAM_LIVE_TIER_1,
    STREAM_LIVE_TIER_2,
    STREAM_LIVE_TIER_3,
    STREAM_LIVE_WARM,
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
    edges_pending: bool = False


@dataclass
class _FakeJob:
    job_id: str = "job-1"
    job_type: str = "refresh_live_event"
    sport_slug: str = "football"
    entity_id: int = 8001
    scope: str = "tier_1"
    params: dict = None
    trace_id: str = "trace-1"

    def __post_init__(self):
        if self.params is None:
            self.params = {}


class _FakeRedisBackend:
    """Minimal Redis-shaped in-memory backend for SET NX EX, GET, DELETE, XADD/XLEN."""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.streams: dict[str, list[tuple[str, dict]]] = {}
        self.group_lags: dict[tuple[str, str], int] = {}

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
        del seconds
        return key in self.values

    def xadd(self, stream, payload):
        msg_id = f"{stream}-{len(self.streams.get(stream, []))+1}"
        self.streams.setdefault(stream, []).append((msg_id, dict(payload)))
        return msg_id

    def xlen(self, stream):
        return len(self.streams.get(stream, []))

    def xinfo_groups(self, stream):
        from schema_inspector.queue.streams import GROUP_LIVE_WARM

        lag = self.group_lags.get((stream, GROUP_LIVE_WARM), len(self.streams.get(stream, [])))
        return [{"name": GROUP_LIVE_WARM, "lag": lag, "consumers": 1, "pending": 0}]


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


class _NoopDelayedScheduler:
    def schedule(self, *args, **kwargs):
        del args, kwargs


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


class _SpyOrchestrator:
    """Records the (event_id, sport_slug, hydration_mode) of every
    ``run_event`` call, returns the report passed in at construction."""

    def __init__(self, *, report: _FakeReport) -> None:
        self.calls: list[tuple[int, str, str]] = []
        self._report = report

    async def run_event(self, *, event_id, sport_slug, hydration_mode, fetch_timeout_seconds=None, **kwargs):
        self.calls.append((event_id, sport_slug, hydration_mode))
        return self._report


def _live_entry(*, lane_stream: str, event_id: int = 8001, message_id: str = "1-1", live_dispatch_tier: str = "tier_1") -> StreamEntry:
    return StreamEntry(
        stream=lane_stream,
        message_id=message_id,
        values={
            "job_id": f"job-live-{event_id}",
            "job_type": "refresh_live_event",
            "sport_slug": "football",
            "event_id": str(event_id),
            "lane": live_dispatch_tier,
            "attempt": "1",
            "params_json": json.dumps(
                {"hydration_mode": "live_delta", "live_dispatch_tier": live_dispatch_tier}
            ),
        },
    )


# ---------------------------------------------------------------------------
# 1. Worker-level: flag ON + tier_1 -> root-only path, edges followup published
# ---------------------------------------------------------------------------
class LiveTier1RootOnlyWorkerTests(unittest.IsolatedAsyncioTestCase):
    async def test_flag_on_tier_1_dispatches_root_only_and_publishes_edges_followup(self) -> None:
        from schema_inspector.queue.live_edges_throttle import LiveEdgesThrottle
        from schema_inspector.queue.live_inflight import (
            LiveEventInFlightStore,
            LiveEventRootInFlightStore,
        )
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        spy = _SpyOrchestrator(
            report=_FakeReport(fetch_outcomes=(), edges_pending=True, finalized=False)
        )

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = LiveWorkerService(
                orchestrator=spy,
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_1",
                consumer="worker-live-tier-1-1",
                in_flight_store=LiveEventInFlightStore(backend, ttl_ms=600_000),
                root_in_flight_store=LiveEventRootInFlightStore(backend, ttl_ms=60_000),
                edges_throttle=LiveEdgesThrottle(backend, interval_seconds=60),
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        self.assertEqual(result, "completed")
        # The orchestrator was called with hydration_mode="root_only".
        self.assertEqual(len(spy.calls), 1)
        self.assertEqual(spy.calls[0][2], "root_only")
        # Edges follow-up published to live_warm.
        warm_publishes = [p for p in queue.published if p[0] == STREAM_LIVE_WARM]
        self.assertEqual(len(warm_publishes), 1)
        _, payload = warm_publishes[0]
        self.assertEqual(payload["job_type"], "refresh_live_event")
        self.assertEqual(payload["entity_id"], 8001)
        params = json.loads(payload["params_json"])
        self.assertTrue(params.get("edges_followup"))
        # Root lock was claimed at the root key (released after run, so
        # current value is None / absent — but the in_flight key was
        # never touched).
        self.assertNotIn("live:refresh_inflight:8001", backend.values)

    async def test_flag_on_tier_1_uses_root_lock_not_refresh_lock(self) -> None:
        """Pre-claim ``live:refresh_inflight:{event_id}`` to simulate a
        full refresh on live_warm. The tier_1 root-only worker MUST still
        proceed (claim its own ``live:root_inflight`` instead)."""
        from schema_inspector.queue.live_edges_throttle import LiveEdgesThrottle
        from schema_inspector.queue.live_inflight import (
            LIVE_EVENT_INFLIGHT_KEY,
            LiveEventInFlightStore,
            LiveEventRootInFlightStore,
        )
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        # Simulate a long-running full refresh holding refresh_inflight.
        backend.set(LIVE_EVENT_INFLIGHT_KEY.format(event_id=8001), "warm-worker:abc")
        queue = _RecordingQueue(backend=backend)
        spy = _SpyOrchestrator(
            report=_FakeReport(fetch_outcomes=(), edges_pending=True, finalized=False)
        )

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = LiveWorkerService(
                orchestrator=spy,
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_1",
                consumer="worker-live-tier-1-1",
                in_flight_store=LiveEventInFlightStore(backend, ttl_ms=600_000),
                root_in_flight_store=LiveEventRootInFlightStore(backend, ttl_ms=60_000),
                edges_throttle=LiveEdgesThrottle(backend, interval_seconds=60),
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        self.assertEqual(result, "completed")
        # Root-only ran despite the refresh_inflight being held by another worker.
        self.assertEqual(len(spy.calls), 1)
        self.assertEqual(spy.calls[0][2], "root_only")

    async def test_flag_off_preserves_legacy_behavior(self) -> None:
        from schema_inspector.queue.live_edges_throttle import LiveEdgesThrottle
        from schema_inspector.queue.live_inflight import (
            LiveEventInFlightStore,
            LiveEventRootInFlightStore,
        )
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        spy = _SpyOrchestrator(
            report=_FakeReport(fetch_outcomes=(), edges_pending=False, finalized=False)
        )

        # Flag explicitly absent — preserves legacy.
        with _patched_env(LIVE_TIER_1_ROOT_ONLY=None):
            worker = LiveWorkerService(
                orchestrator=spy,
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_1",
                consumer="worker-live-tier-1-1",
                in_flight_store=LiveEventInFlightStore(backend, ttl_ms=600_000),
                root_in_flight_store=LiveEventRootInFlightStore(backend, ttl_ms=60_000),
                edges_throttle=LiveEdgesThrottle(backend, interval_seconds=60),
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        self.assertEqual(result, "completed")
        # Hydration mode is the legacy value resolved from job params (live_delta),
        # NOT root_only.
        self.assertEqual(len(spy.calls), 1)
        self.assertEqual(spy.calls[0][2], "live_delta")
        # No edges-followup publish.
        warm_publishes = [p for p in queue.published if p[0] == STREAM_LIVE_WARM]
        self.assertEqual(len(warm_publishes), 0)

    async def test_flag_on_tier_2_does_not_use_root_only(self) -> None:
        from schema_inspector.queue.live_edges_throttle import LiveEdgesThrottle
        from schema_inspector.queue.live_inflight import (
            LiveEventInFlightStore,
            LiveEventRootInFlightStore,
        )
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        spy = _SpyOrchestrator(
            report=_FakeReport(fetch_outcomes=(), edges_pending=False, finalized=False)
        )

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = LiveWorkerService(
                orchestrator=spy,
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_2",
                consumer="worker-live-tier-2-1",
                in_flight_store=LiveEventInFlightStore(backend, ttl_ms=600_000),
                root_in_flight_store=LiveEventRootInFlightStore(backend, ttl_ms=60_000),
                edges_throttle=LiveEdgesThrottle(backend, interval_seconds=60),
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_2, live_dispatch_tier="tier_2"))

        self.assertEqual(result, "completed")
        self.assertEqual(spy.calls[0][2], "live_delta")  # NOT root_only
        # No edges-followup on tier_2.
        warm_publishes = [p for p in queue.published if p[0] == STREAM_LIVE_WARM]
        self.assertEqual(len(warm_publishes), 0)

    async def test_flag_on_tier_3_does_not_use_root_only(self) -> None:
        from schema_inspector.queue.live_edges_throttle import LiveEdgesThrottle
        from schema_inspector.queue.live_inflight import (
            LiveEventInFlightStore,
            LiveEventRootInFlightStore,
        )
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        spy = _SpyOrchestrator(
            report=_FakeReport(fetch_outcomes=(), edges_pending=False, finalized=False)
        )

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = LiveWorkerService(
                orchestrator=spy,
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_3",
                consumer="worker-live-tier-3-1",
                in_flight_store=LiveEventInFlightStore(backend, ttl_ms=600_000),
                root_in_flight_store=LiveEventRootInFlightStore(backend, ttl_ms=60_000),
                edges_throttle=LiveEdgesThrottle(backend, interval_seconds=60),
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_3, live_dispatch_tier="tier_3"))

        self.assertEqual(result, "completed")
        self.assertEqual(spy.calls[0][2], "live_delta")
        warm_publishes = [p for p in queue.published if p[0] == STREAM_LIVE_WARM]
        self.assertEqual(len(warm_publishes), 0)

    async def test_edges_enqueue_throttled_within_window(self) -> None:
        from schema_inspector.queue.live_edges_throttle import LiveEdgesThrottle
        from schema_inspector.queue.live_inflight import (
            LiveEventInFlightStore,
            LiveEventRootInFlightStore,
        )
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        spy = _SpyOrchestrator(
            report=_FakeReport(fetch_outcomes=(), edges_pending=True, finalized=False)
        )

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = LiveWorkerService(
                orchestrator=spy,
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_1",
                consumer="worker-live-tier-1-1",
                in_flight_store=LiveEventInFlightStore(backend, ttl_ms=600_000),
                # Root-only path requires root_in_flight_store wired; the
                # in-memory backend's SET NX EX is event-keyed, so the same
                # event_id across iterations releases-and-reclaims cleanly.
                root_in_flight_store=LiveEventRootInFlightStore(backend, ttl_ms=60_000),
                edges_throttle=LiveEdgesThrottle(backend, interval_seconds=60),
            )
            for i in range(3):
                await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1, message_id=f"1-{i}"))

        # Only the first call publishes; the next two are throttled inside
        # the 60-second window.
        warm_publishes = [p for p in queue.published if p[0] == STREAM_LIVE_WARM]
        self.assertEqual(len(warm_publishes), 1)

    async def test_edges_backpressure_skip_does_not_block_root(self) -> None:
        from schema_inspector.queue.live_edges_throttle import LiveEdgesThrottle
        from schema_inspector.queue.live_inflight import (
            LiveEventInFlightStore,
            LiveEventRootInFlightStore,
        )
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        # Pre-fill warm stream past the cap.
        for i in range(20):
            backend.xadd(STREAM_LIVE_WARM, {"k": str(i)})
        queue = _RecordingQueue(backend=backend)
        spy = _SpyOrchestrator(
            report=_FakeReport(fetch_outcomes=(), edges_pending=True, finalized=False)
        )

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = LiveWorkerService(
                orchestrator=spy,
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_1",
                consumer="worker-live-tier-1-1",
                in_flight_store=None,
                root_in_flight_store=LiveEventRootInFlightStore(backend, ttl_ms=60_000),
                edges_throttle=LiveEdgesThrottle(backend, interval_seconds=60),
                edges_backpressure_limit=10,
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        self.assertEqual(result, "completed")
        # No edges-followup despite edges_pending=True — backpressure rejected.
        warm_publishes = [p for p in queue.published if p[0] == STREAM_LIVE_WARM]
        self.assertEqual(len(warm_publishes), 0)

    async def test_edges_backpressure_uses_lag_not_xlen(self) -> None:
        from schema_inspector.queue.live_edges_throttle import LiveEdgesThrottle
        from schema_inspector.queue.live_inflight import LiveEventRootInFlightStore
        from schema_inspector.queue.streams import GROUP_LIVE_WARM
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        for i in range(1_000):
            backend.xadd(STREAM_LIVE_WARM, {"k": str(i)})
        backend.group_lags[(STREAM_LIVE_WARM, GROUP_LIVE_WARM)] = 0
        queue = _RecordingQueue(backend=backend)
        spy = _SpyOrchestrator(
            report=_FakeReport(fetch_outcomes=(), edges_pending=True, finalized=False)
        )

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = LiveWorkerService(
                orchestrator=spy,
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_1",
                consumer="worker-live-tier-1-1",
                in_flight_store=None,
                root_in_flight_store=LiveEventRootInFlightStore(backend, ttl_ms=60_000),
                edges_throttle=LiveEdgesThrottle(backend, interval_seconds=60),
                edges_backpressure_limit=10,
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        self.assertEqual(result, "completed")
        warm_publishes = [p for p in queue.published if p[0] == STREAM_LIVE_WARM]
        self.assertEqual(len(warm_publishes), 1)

    async def test_terminal_finalized_skips_edges_enqueue(self) -> None:
        from schema_inspector.queue.live_edges_throttle import LiveEdgesThrottle
        from schema_inspector.queue.live_inflight import (
            LiveEventInFlightStore,
            LiveEventRootInFlightStore,
        )
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        spy = _SpyOrchestrator(
            # Terminal status — orchestrator returns finalized=True,
            # edges_pending=False (event has nothing left to refresh).
            report=_FakeReport(fetch_outcomes=(), edges_pending=False, finalized=True)
        )

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = LiveWorkerService(
                orchestrator=spy,
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_1",
                consumer="worker-live-tier-1-1",
                in_flight_store=LiveEventInFlightStore(backend, ttl_ms=600_000),
                root_in_flight_store=LiveEventRootInFlightStore(backend, ttl_ms=60_000),
                edges_throttle=LiveEdgesThrottle(backend, interval_seconds=60),
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        self.assertEqual(result, "completed")
        # No edges-followup for a terminal event.
        warm_publishes = [p for p in queue.published if p[0] == STREAM_LIVE_WARM]
        self.assertEqual(len(warm_publishes), 0)


# ---------------------------------------------------------------------------
# 2. Orchestrator-level: hydration_mode="root_only" early-return
# ---------------------------------------------------------------------------
class PilotOrchestratorRootOnlyTests(unittest.IsolatedAsyncioTestCase):
    """Drives the real ``PilotOrchestrator.run_event(hydration_mode=
    'root_only')`` path with stubs for fetch/parse and live state.
    Verifies:

    1. Exactly one fetch (the EVENT_DETAIL_ENDPOINT root).
    2. ``planner.expand`` is NOT called (no edges/details job expansion).
    3. ``edges_pending=True`` for non-terminal status.
    4. Terminal status -> ``finalized=True``, ``edges_pending=False``,
       ``finalize_event`` called, ``_record_terminal_state`` awaited.
    """

    def _build_orchestrator(self, *, status_type: str | None):
        """Construct a minimal PilotOrchestrator with stubs.

        Returns ``(orchestrator, planner_spy, fetch_calls, finalize_calls,
        terminal_state_calls)`` for assertions.
        """
        from schema_inspector.parsers.base import (
            PARSE_STATUS_PARSED,
            ParseResult,
            RawSnapshot,
        )
        from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator

        fetch_calls: list[tuple[str, dict]] = []
        finalize_calls: list[dict] = []
        terminal_state_calls: list[dict] = []
        live_state_history_calls: list[dict] = []

        class _PlannerSpy:
            def __init__(self) -> None:
                self.capability_rollup: dict = {}
                self.expand_calls: list = []

            def expand(self, root_job):
                self.expand_calls.append(root_job)
                return ()

        class _FakeOutcome:
            def __init__(self, *, snapshot_id: int = 100, fetched_at: str = "2026-05-09T20:00:00+0300"):
                self.classification = "success_json"
                self.retry_recommended = False
                self.http_status = 200
                self.snapshot_id = snapshot_id
                self.fetched_at = fetched_at
                self.error_message = None

        # Build the parse result — entity_upserts include "event" with status_type
        fake_event_row = {
            "id": 9001,
            "status_type": status_type,
            "home_team_id": 11,
            "away_team_id": 22,
            "detail_id": 99,
            "start_timestamp": 1_700_000_000,
            "has_event_player_statistics": False,
            "has_event_player_heat_map": False,
            "has_global_highlights": False,
            "has_xg": False,
        }
        snapshot = RawSnapshot(
            snapshot_id=100,
            endpoint_pattern="/api/v1/event/{event_id}",
            sport_slug="football",
            source_url="https://www.sofascore.com/api/v1/event/9001",
            resolved_url="https://api.sofascore.com/api/v1/event/9001",
            envelope_key="event",
            http_status=200,
            payload={"event": {"id": 9001}},
            fetched_at="2026-05-09T20:00:00+0300",
            context_entity_type="event",
            context_entity_id=9001,
            context_unique_tournament_id=None,
            context_season_id=None,
            context_event_id=9001,
        )
        parse_result = ParseResult(
            snapshot_id=100,
            parser_family="event_root",
            parser_version="v1",
            status=PARSE_STATUS_PARSED,
            entity_upserts={"event": (fake_event_row,)},
            relation_upserts={},
            metric_rows={},
            observed_root_keys=("event",),
        )

        async def _fetch_and_parse(*, endpoint, sport_slug, path_params, **_kw):
            fetch_calls.append((endpoint.pattern if hasattr(endpoint, "pattern") else str(endpoint), dict(path_params or {})))
            return _FakeOutcome(), parse_result

        class _LiveWorkerStub:
            def finalize_event(self, *, sport_slug, event_id, status_type, live_state_store):
                finalize_calls.append(
                    {"sport_slug": sport_slug, "event_id": event_id, "status_type": status_type}
                )

        async def _flush_capabilities():
            return None

        async def _record_live_state_history(*, event_id, status_type, poll_profile, observed_at):
            live_state_history_calls.append(
                {"event_id": event_id, "status_type": status_type, "poll_profile": poll_profile}
            )

        async def _record_terminal_state(*, event_id, status_type, finalized_at, final_snapshot_id):
            terminal_state_calls.append(
                {"event_id": event_id, "status_type": status_type, "final_snapshot_id": final_snapshot_id}
            )

        # Construct the orchestrator with a NoOp Sentinel — most fields
        # are unused under root_only because we early-return before they
        # matter. Patch the orchestrator instance methods directly.
        orchestrator = PilotOrchestrator.__new__(PilotOrchestrator)
        orchestrator.fetch_executor = object()  # truthy
        orchestrator.snapshot_store = None
        orchestrator.normalize_worker = None
        orchestrator.planner = _PlannerSpy()
        orchestrator.capability_repository = None
        orchestrator.sql_executor = None
        orchestrator.live_state_store = object()
        orchestrator.live_state_repository = None
        orchestrator.stream_queue = None
        orchestrator.season_widget_gate = None
        orchestrator.event_endpoint_gate = None
        orchestrator.final_sweep_gate = None
        orchestrator.freshness_store = None
        orchestrator.live_bootstrap_coordinator = None
        orchestrator.live_worker = _LiveWorkerStub()
        orchestrator.now_ms_factory = lambda: 1_700_000_000_000
        orchestrator._pending_capability_records = []
        orchestrator._fanout_max_inflight = 1
        orchestrator._freshness_skip_keys = set()

        # Wire stubs in place of methods that would otherwise hit DB / I/O.
        orchestrator._fetch_and_parse = _fetch_and_parse
        orchestrator._flush_capabilities = _flush_capabilities
        orchestrator._record_live_state_history = _record_live_state_history
        orchestrator._record_terminal_state = _record_terminal_state
        # _should_retire_missing_root is only called for not_found classification;
        # we use success_json so this is unreached.

        return (
            orchestrator,
            orchestrator.planner,
            fetch_calls,
            finalize_calls,
            terminal_state_calls,
        )

    async def test_root_only_inprogress_returns_edges_pending_no_planner_expand(self) -> None:
        orch, planner, fetch_calls, finalize_calls, terminal_state_calls = self._build_orchestrator(
            status_type="inprogress"
        )

        report = await orch.run_event(
            event_id=9001,
            sport_slug="football",
            hydration_mode="root_only",
        )

        # Exactly one ROOT fetch.
        self.assertEqual(len(fetch_calls), 1)
        self.assertIn("event", fetch_calls[0][0])  # EVENT_DETAIL_ENDPOINT pattern
        # planner.expand was NEVER called — no edges/details fan-out.
        self.assertEqual(planner.expand_calls, [])
        # Inprogress status -> NOT finalized, edges_pending=True.
        self.assertFalse(report.finalized)
        self.assertTrue(report.edges_pending)
        self.assertEqual(report.event_id, 9001)
        # Inline finalize / terminal_state must NOT have been called.
        self.assertEqual(finalize_calls, [])
        self.assertEqual(terminal_state_calls, [])

    async def test_root_only_terminal_finished_inline_finalizes(self) -> None:
        orch, planner, fetch_calls, finalize_calls, terminal_state_calls = self._build_orchestrator(
            status_type="finished"
        )

        report = await orch.run_event(
            event_id=9001,
            sport_slug="football",
            hydration_mode="root_only",
        )

        # Still exactly one ROOT fetch, still no planner.expand.
        self.assertEqual(len(fetch_calls), 1)
        self.assertEqual(planner.expand_calls, [])
        # Terminal status -> finalized=True, edges_pending=False.
        self.assertTrue(report.finalized)
        self.assertFalse(report.edges_pending)
        # Inline finalize was invoked with status_type="finished".
        self.assertEqual(len(finalize_calls), 1)
        self.assertEqual(finalize_calls[0]["status_type"], "finished")
        self.assertEqual(finalize_calls[0]["event_id"], 9001)
        # Terminal state recorded.
        self.assertEqual(len(terminal_state_calls), 1)
        self.assertEqual(terminal_state_calls[0]["status_type"], "finished")
        self.assertEqual(terminal_state_calls[0]["event_id"], 9001)

    async def test_root_only_terminal_postponed_inline_finalizes(self) -> None:
        orch, planner, fetch_calls, finalize_calls, terminal_state_calls = self._build_orchestrator(
            status_type="postponed"
        )

        report = await orch.run_event(
            event_id=9001,
            sport_slug="football",
            hydration_mode="root_only",
        )

        self.assertEqual(planner.expand_calls, [])
        self.assertTrue(report.finalized)
        self.assertFalse(report.edges_pending)
        self.assertEqual(len(finalize_calls), 1)
        self.assertEqual(finalize_calls[0]["status_type"], "postponed")


# ---------------------------------------------------------------------------
# 3. AST regression — guard against silent removal of root-only dispatch
# ---------------------------------------------------------------------------
class LiveWorkerServiceRootOnlyExposureTests(unittest.TestCase):
    def test_handle_reads_live_tier_1_root_only_env_and_dispatches_root_only(self) -> None:
        """Source-level invariant: ``LiveWorkerService.handle`` must
        actually read the env var and pass ``hydration_mode="root_only"``
        to the orchestrator. Without this guard a future refactor could
        silently drop the dispatch and the rollout flag would become a
        no-op."""
        import ast
        import inspect
        import textwrap

        from schema_inspector.workers.live_worker_service import LiveWorkerService

        source = textwrap.dedent(inspect.getsource(LiveWorkerService.handle))
        tree = ast.parse(source)

        # Walk for string constants used in the function body.
        string_constants: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                string_constants.append(node.value)

        self.assertIn(
            "LIVE_TIER_1_ROOT_ONLY",
            string_constants,
            "handle() must read LIVE_TIER_1_ROOT_ONLY env to drive the rollout flag.",
        )
        self.assertIn(
            "root_only",
            string_constants,
            "handle() must dispatch hydration_mode='root_only' to orchestrator.run_event.",
        )
        self.assertIn(
            "tier_1",
            string_constants,
            "handle() must scope the rollout flag to lane=='tier_1'.",
        )


# ---------------------------------------------------------------------------
# 4. Real PilotOrchestrator end-to-end: hydration_mode="root_only" must
#    NOT trigger any edge fetches via the actual fetch_executor +
#    transport, regardless of the inprogress-status payload.
# ---------------------------------------------------------------------------
class PilotOrchestratorRealRunRootOnlyTests(unittest.IsolatedAsyncioTestCase):
    """Reproduces the prod regression by driving the *real*
    ``PilotOrchestrator.run_event`` against a fake transport. If the
    orchestrator's root_only branch fires, only the EVENT_DETAIL_ENDPOINT
    URL is fetched and ``edges_pending=True`` comes back. If for any
    reason (helper rebuild, fall-through, code path change) the branch
    does not fire, the test fails because edge URLs end up in
    ``transport.seen_urls``."""

    async def test_root_only_real_orchestrator_fetches_only_event_root(self) -> None:
        import json as _json

        from schema_inspector.fetch_executor import FetchExecutor
        from schema_inspector.normalizers.worker import NormalizeWorker
        from schema_inspector.parsers.registry import ParserRegistry
        from schema_inspector.pipeline.pilot_orchestrator import PilotOrchestrator
        from schema_inspector.planner.planner import Planner
        from schema_inspector.runtime import TransportAttempt, TransportResult

        event_url = "https://www.sofascore.com/api/v1/event/8001"
        statistics_url = "https://www.sofascore.com/api/v1/event/8001/statistics"
        lineups_url = "https://www.sofascore.com/api/v1/event/8001/lineups"
        incidents_url = "https://www.sofascore.com/api/v1/event/8001/incidents"

        def make_response(url: str, payload: object) -> TransportResult:
            return TransportResult(
                resolved_url=url,
                status_code=200,
                headers={"Content-Type": "application/json"},
                body_bytes=_json.dumps(payload).encode("utf-8"),
                attempts=(TransportAttempt(1, "proxy_1", 200, None, None),),
                final_proxy_name="proxy_1",
                challenge_reason=None,
            )

        responses = {
            event_url: make_response(
                event_url,
                {
                    "event": {
                        "id": 8001,
                        "slug": "team-a-team-b",
                        "detailId": 1,
                        "tournament": {
                            "id": 100,
                            "slug": "test-league",
                            "name": "Test League",
                            "uniqueTournament": {
                                "id": 17,
                                "slug": "test-league",
                                "name": "Test League",
                            },
                        },
                        "season": {"id": 76986, "name": "Test 25/26", "year": "25/26"},
                        "status": {"code": 7, "type": "inprogress", "description": "1st half"},
                        "homeTeam": {"id": 11, "slug": "home", "name": "Home"},
                        "awayTeam": {"id": 22, "slug": "away", "name": "Away"},
                    }
                },
            ),
            # Edge URLs registered so transport doesn't 404 — but if
            # root_only branch fires, they should NEVER be hit.
            statistics_url: make_response(statistics_url, {"statistics": []}),
            lineups_url: make_response(lineups_url, {"home": {"players": []}, "away": {"players": []}}),
            incidents_url: make_response(incidents_url, {"incidents": []}),
        }

        class _FakeTransport:
            def __init__(self, mapping: dict[str, TransportResult]) -> None:
                self.mapping = mapping
                self.seen_urls: list[str] = []

            async def fetch(self, url: str, *, headers=None, timeout: float = 20.0) -> TransportResult:
                del headers, timeout
                self.seen_urls.append(url)
                return self.mapping[url]

        class _FakeRawSnapshotStore:
            def __init__(self) -> None:
                self.snapshots_by_id: dict[int, object] = {}
                self._next_id = 1

            async def insert_request_log(self, executor, record) -> None:
                del executor, record

            async def insert_payload_snapshot_returning_id(self, executor, record) -> int:
                del executor
                snapshot_id = self._next_id
                self._next_id += 1
                self.snapshots_by_id[snapshot_id] = record
                return snapshot_id

            async def insert_payload_snapshot_if_missing_returning_id(self, executor, record) -> int:
                return await self.insert_payload_snapshot_returning_id(executor, record)

            async def upsert_snapshot_head(self, executor, record) -> None:
                del executor, record

            def load_snapshot(self, snapshot_id: int):
                from schema_inspector.parsers.base import RawSnapshot

                record = self.snapshots_by_id[snapshot_id]
                return RawSnapshot(
                    snapshot_id=snapshot_id,
                    endpoint_pattern=record.endpoint_pattern,
                    sport_slug=record.sport_slug,
                    source_url=record.source_url,
                    resolved_url=record.resolved_url,
                    envelope_key=record.envelope_key,
                    http_status=record.http_status,
                    payload=record.payload,
                    fetched_at=record.fetched_at,
                    context_entity_type=record.context_entity_type,
                    context_entity_id=record.context_entity_id,
                    context_unique_tournament_id=record.context_unique_tournament_id,
                    context_season_id=record.context_season_id,
                    context_event_id=record.context_event_id,
                )

        transport = _FakeTransport(responses)
        raw_store = _FakeRawSnapshotStore()
        fetch_executor = FetchExecutor(
            transport=transport, raw_repository=raw_store, sql_executor=object()
        )
        orchestrator = PilotOrchestrator(
            fetch_executor=fetch_executor,
            snapshot_store=raw_store,
            normalize_worker=NormalizeWorker(ParserRegistry.default()),
            planner=Planner(capability_rollup={}),
            capability_repository=None,
            sql_executor=object(),
        )

        report = await orchestrator.run_event(
            event_id=8001,
            sport_slug="football",
            hydration_mode="root_only",
        )

        # CRITICAL ASSERTION — the orchestrator must NOT fetch any edge
        # endpoints when running root_only. Only the EVENT_DETAIL_ENDPOINT
        # URL should be in seen_urls.
        self.assertEqual(
            transport.seen_urls,
            [event_url],
            (
                f"root_only must fetch ONLY the event root URL, "
                f"but got: {transport.seen_urls}"
            ),
        )
        self.assertTrue(report.edges_pending)
        self.assertFalse(report.finalized)
        self.assertEqual(report.event_id, 8001)


# ---------------------------------------------------------------------------
# 5. HybridApp.run_event(hydration_mode="root_only") — propagation test.
#    Mocks the prefetch+commit+persist helpers and verifies each receives
#    hydration_mode="root_only" verbatim. If any helper sees a different
#    string (live_delta/full/etc.) the propagation chain is broken.
# ---------------------------------------------------------------------------
class HybridAppRootOnlyPropagationTests(unittest.IsolatedAsyncioTestCase):
    async def test_hybrid_app_run_event_propagates_root_only_to_all_helpers(self) -> None:
        """Propagation chain: HybridApp.run_event → _prefetch_event_run →
        _persist_prefetched_run. Each link must carry hydration_mode
        unchanged. If any link substitutes another value the inner
        PilotOrchestrator will not enter the root_only branch and edges
        fan-out fires (the prod regression we observed)."""
        from schema_inspector.cli import HybridApp

        app = HybridApp.__new__(HybridApp)
        # Capture what each helper receives.
        prefetch_called_with: list[str] = []
        persist_called_with: list[str] = []

        async def _resolve_event_sport_slug(event_id):
            del event_id
            return "football"

        async def _ensure_endpoint_registry(sport_slug):
            del sport_slug

        class _StubPrefetchedRun:
            event_id = 8001
            sport_slug = "football"
            total_payload_size_bytes = 1
            endpoint_count = 1

        async def _prefetch_event_run(*, event_id, sport_slug, hydration_mode, fetch_timeout_seconds=None, **kwargs):
            del event_id, sport_slug, fetch_timeout_seconds, kwargs
            prefetch_called_with.append(hydration_mode)
            return _StubPrefetchedRun()

        def _warn_if_prefetched_run_large(prefetched_run):
            del prefetched_run

        async def _commit_prefetched_run(prefetched_run):
            return prefetched_run

        async def _persist_prefetched_run(prefetched_run, *, hydration_mode, scope=None):
            del prefetched_run, scope
            persist_called_with.append(hydration_mode)
            from schema_inspector.pipeline.pilot_orchestrator import PilotRunReport

            return PilotRunReport(
                sport_slug="football",
                event_id=8001,
                fetch_outcomes=(),
                parse_results=(),
                edges_pending=(hydration_mode == "root_only"),
            )

        # Patch instance methods.
        app.resolve_event_sport_slug = _resolve_event_sport_slug
        app.ensure_endpoint_registry = _ensure_endpoint_registry
        app._prefetch_event_run = _prefetch_event_run
        app._warn_if_prefetched_run_large = _warn_if_prefetched_run_large
        app._commit_prefetched_run = _commit_prefetched_run
        app._persist_prefetched_run = _persist_prefetched_run
        app.live_bootstrap_coordinator = None  # bypass the bootstrap branch

        result = await app.run_event(
            event_id=8001,
            sport_slug="football",
            hydration_mode="root_only",
        )

        self.assertEqual(prefetch_called_with, ["root_only"])
        self.assertEqual(persist_called_with, ["root_only"])
        self.assertTrue(result.edges_pending)


# ---------------------------------------------------------------------------
# 6. Job-type discrimination on tier_1 with flag ON.
#    `refresh_live_event` MUST go through root_only path.
#    `hydrate_event_root` (published by discovery worker on newly-live
#    events) MUST stay on the legacy full-hydration path because new
#    live events need bootstrap (edges + details fan-out) on the first
#    sighting. This is the source of the observed `phase=edges` entries
#    on tier_1 during the canary — they came from hydrate_event_root
#    jobs, NOT from refresh_live_event runs (which correctly went
#    through root_only and never emitted phase=edges).
# ---------------------------------------------------------------------------
class LiveTier1JobTypeDiscriminationTests(unittest.IsolatedAsyncioTestCase):
    async def test_refresh_live_event_uses_root_only_but_hydrate_event_root_does_not(self) -> None:
        from schema_inspector.queue.live_edges_throttle import LiveEdgesThrottle
        from schema_inspector.queue.live_inflight import (
            LiveEventInFlightStore,
            LiveEventRootInFlightStore,
        )
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        spy = _SpyOrchestrator(
            report=_FakeReport(fetch_outcomes=(), edges_pending=True, finalized=False)
        )

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = LiveWorkerService(
                orchestrator=spy,
                delayed_scheduler=_NoopDelayedScheduler(),
                queue=queue,
                lane="tier_1",
                consumer="worker-live-tier-1-1",
                in_flight_store=LiveEventInFlightStore(backend, ttl_ms=600_000),
                root_in_flight_store=LiveEventRootInFlightStore(backend, ttl_ms=60_000),
                edges_throttle=LiveEdgesThrottle(backend, interval_seconds=60),
            )

            # 1. refresh_live_event → root_only path.
            refresh_entry = StreamEntry(
                stream=STREAM_LIVE_TIER_1,
                message_id="1-refresh",
                values={
                    "job_id": "job-refresh-9001",
                    "job_type": "refresh_live_event",
                    "sport_slug": "football",
                    "event_id": "9001",
                    "lane": "tier_1",
                    "attempt": "1",
                    "params_json": json.dumps(
                        {"hydration_mode": "live_delta", "live_dispatch_tier": "tier_1"}
                    ),
                },
            )
            await worker.handle(refresh_entry)

            # 2. hydrate_event_root → legacy path (discovery published).
            hydrate_entry = StreamEntry(
                stream=STREAM_LIVE_TIER_1,
                message_id="1-hydrate",
                values={
                    "job_id": "job-hydrate-9002",
                    "job_type": "hydrate_event_root",
                    "sport_slug": "football",
                    "event_id": "9002",
                    "lane": "tier_1",
                    "attempt": "1",
                    "params_json": json.dumps(
                        {"hydration_mode": "live_delta", "live_dispatch_tier": "tier_1"}
                    ),
                },
            )
            await worker.handle(hydrate_entry)

        # refresh_live_event went through root_only.
        # hydrate_event_root went through legacy (resolved to live_delta).
        self.assertEqual(len(spy.calls), 2)
        # Order: refresh first, then hydrate.
        refresh_call = spy.calls[0]
        hydrate_call = spy.calls[1]
        self.assertEqual(refresh_call[0], 9001)  # event_id
        self.assertEqual(refresh_call[2], "root_only")  # hydration_mode
        self.assertEqual(hydrate_call[0], 9002)
        self.assertEqual(hydrate_call[2], "live_delta")  # NOT root_only

        # refresh_live_event published edges-followup. hydrate_event_root did NOT.
        warm_publishes = [p for p in queue.published if p[0] == STREAM_LIVE_WARM]
        self.assertEqual(len(warm_publishes), 1)
        _, payload = warm_publishes[0]
        self.assertEqual(payload["entity_id"], 9001)  # only refresh's event triggered followup

        # Lock keys discriminate by path:
        # - refresh used live:root_inflight (released after run, key absent now)
        # - hydrate used live:refresh_inflight (also released)
        # Neither should leak; both released cleanly.
        self.assertNotIn("live:root_inflight:9001", backend.values)
        self.assertNotIn("live:refresh_inflight:9001", backend.values)
        self.assertNotIn("live:root_inflight:9002", backend.values)
        self.assertNotIn("live:refresh_inflight:9002", backend.values)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
