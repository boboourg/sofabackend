"""Integration tests for tier_1 root-only retry quarantine in ``LiveWorkerService``.

Verifies the worker layer correctly:

* Skips + ACKs quarantined events (returns ``quarantined_skip``) without
  claiming the inflight lock or invoking ``orchestrator.run_event``.
* Does NOT consult quarantine when ``quarantine_store=None`` (default
  prod config with flag OFF) — preserves legacy fast path exactly.
* Does NOT consult quarantine on non-root-only lanes (tier_2/tier_3/
  warm/hot) — keeps scope narrow to the lane we observed the issue on.
* Records ``RetryableJobError`` failures into the quarantine counter
  via ``record_failure``.
* Clears quarantine on a successful real-work run via
  ``record_success``.
* Does NOT call ``record_failure`` for non-retryable errors (e.g.
  ``AttributeError``) — those must surface, not get silently rerouted.
* Does NOT call ``record_failure``/``record_success`` on coalesced
  returns — coalesce is not a fetch attempt and must not feed the
  counter.
* Fails open when ``global_cap_exceeded`` is True — the worker logs a
  warning and falls through to the normal flow, preventing a runaway-
  quarantine scenario from parking every inprogress event.
"""

from __future__ import annotations

import json
import os
import unittest
from contextlib import contextmanager
from dataclasses import dataclass

from schema_inspector.queue.streams import (
    STREAM_LIVE_TIER_1,
    STREAM_LIVE_WARM,
    StreamEntry,
)


@dataclass
class _FakeReport:
    fetch_outcomes: tuple = ()
    parse_results: tuple = ()
    finalized: bool = False
    details_pending: bool = False
    details_context: dict | None = None
    edges_pending: bool = False


# ---------------------------------------------------------------------------
# Test helpers (intentionally local — not shared with test_live_root_only.py
# so this file stays independently runnable and we are free to evolve one
# backend without breaking the other test surface)
# ---------------------------------------------------------------------------


class _FakeRedisBackend:
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

    def incr(self, key):
        current = int(self.values.get(key, "0"))
        current += 1
        self.values[key] = str(current)
        return current

    def expire(self, key, seconds):
        del seconds
        return key in self.values

    def delete(self, key):
        return self.values.pop(key, None) is not None and 1 or 0

    def scan_iter(self, match=None):
        for key in list(self.values.keys()):
            if match is None:
                yield key
                continue
            if "*" in match:
                prefix, _, suffix = match.partition("*")
                if key.startswith(prefix) and key.endswith(suffix):
                    yield key
            elif key == match:
                yield key

    def xadd(self, stream, payload):
        msg_id = f"{stream}-{len(self.streams.get(stream, []))+1}"
        self.streams.setdefault(stream, []).append((msg_id, dict(payload)))
        return msg_id

    def xlen(self, stream):
        return len(self.streams.get(stream, []))

    # Intentionally NO ``eval`` method — that way
    # ``_release_with_lua`` returns False on inspection and the
    # ``LiveEventInFlightStore.release`` falls through to the Python
    # ``GET == owner -> DEL`` path, which actually deletes the lock key
    # against this fake. Mirrors test_live_root_only.py's pattern; an
    # ``eval`` that returns success without mutating state caused the
    # inflight lock to live forever and coalesced every second call.


class _RecordingQueue:
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
    def __init__(self, *, report: _FakeReport | None = None, raise_on_run=None) -> None:
        self.calls: list[tuple[int, str, str]] = []
        self._report = report or _FakeReport()
        self._raise = raise_on_run

    async def run_event(self, *, event_id, sport_slug, hydration_mode):
        self.calls.append((event_id, sport_slug, hydration_mode))
        if self._raise is not None:
            raise self._raise
        return self._report


def _live_entry(
    *,
    lane_stream: str,
    event_id: int = 8001,
    message_id: str = "1-1",
) -> StreamEntry:
    return StreamEntry(
        stream=lane_stream,
        message_id=message_id,
        values={
            "job_id": f"job-live-{event_id}",
            "job_type": "refresh_live_event",
            "sport_slug": "football",
            "event_id": str(event_id),
            "lane": "tier_1",
            "attempt": "1",
            "params_json": json.dumps(
                {"hydration_mode": "live_delta", "live_dispatch_tier": "tier_1"}
            ),
        },
    )


def _build_worker(
    *,
    backend: _FakeRedisBackend,
    queue: _RecordingQueue,
    orchestrator,
    quarantine_store=None,
    inprogress_count_provider=None,
    lane: str = "tier_1",
):
    """Construct LiveWorkerService with full dependency wiring."""
    from schema_inspector.queue.live_edges_throttle import LiveEdgesThrottle
    from schema_inspector.queue.live_inflight import (
        LiveEventInFlightStore,
        LiveEventRootInFlightStore,
    )
    from schema_inspector.workers.live_worker_service import LiveWorkerService

    return LiveWorkerService(
        orchestrator=orchestrator,
        delayed_scheduler=_NoopDelayedScheduler(),
        queue=queue,
        lane=lane,
        consumer=f"worker-live-{lane}-1",
        in_flight_store=LiveEventInFlightStore(backend, ttl_ms=600_000),
        root_in_flight_store=LiveEventRootInFlightStore(backend, ttl_ms=60_000),
        edges_throttle=LiveEdgesThrottle(backend, interval_seconds=60),
        quarantine_store=quarantine_store,
        quarantine_inprogress_count_provider=inprogress_count_provider,
    )


def _build_store(backend, **overrides):
    from schema_inspector.queue.live_tier_1_quarantine import (
        LiveTier1RetryQuarantineStore,
    )

    defaults = dict(
        threshold=3,
        window_seconds=600,
        base_cooldown_seconds=60,
        max_cooldown_seconds=600,
        global_cap_pct=25,
    )
    defaults.update(overrides)
    return LiveTier1RetryQuarantineStore(backend, **defaults)


# ---------------------------------------------------------------------------
# 1. Quarantine SKIP path
# ---------------------------------------------------------------------------
class QuarantineSkipPathTests(unittest.IsolatedAsyncioTestCase):
    async def test_quarantined_event_returns_quarantined_skip_without_calling_orchestrator(
        self,
    ) -> None:
        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        store = _build_store(backend)
        # Pre-trigger quarantine.
        for _ in range(3):
            store.record_failure(event_id=8001)
        self.assertGreater(store.is_quarantined(event_id=8001), 0)

        spy = _SpyOrchestrator(report=_FakeReport(edges_pending=True))
        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = _build_worker(
                backend=backend, queue=queue, orchestrator=spy, quarantine_store=store
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        self.assertEqual(result, "quarantined_skip")
        # Orchestrator was NOT called.
        self.assertEqual(spy.calls, [])
        # No edges follow-up was published — quarantined event must not
        # leak load onto warm.
        self.assertEqual(
            [p for p in queue.published if p[0] == STREAM_LIVE_WARM], []
        )

    async def test_not_quarantined_event_runs_orchestrator_normally(self) -> None:
        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        store = _build_store(backend)
        spy = _SpyOrchestrator(report=_FakeReport(edges_pending=True))

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = _build_worker(
                backend=backend, queue=queue, orchestrator=spy, quarantine_store=store
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        self.assertEqual(result, "completed")
        self.assertEqual(len(spy.calls), 1)
        self.assertEqual(spy.calls[0][2], "root_only")


# ---------------------------------------------------------------------------
# 2. Flag OFF regression
# ---------------------------------------------------------------------------
class QuarantineFlagOffRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_no_quarantine_store_means_legacy_behaviour(self) -> None:
        """When ``quarantine_store=None`` the worker MUST NOT consult any
        quarantine state — proves service_app's "flag OFF → pass None" gate
        is sufficient."""
        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        spy = _SpyOrchestrator(report=_FakeReport(edges_pending=True))

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = _build_worker(
                backend=backend, queue=queue, orchestrator=spy, quarantine_store=None
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        self.assertEqual(result, "completed")
        # Orchestrator ran the root-only run.
        self.assertEqual(spy.calls[0][2], "root_only")

    async def test_quarantine_only_applies_when_root_only_flag_on(self) -> None:
        """``LIVE_TIER_1_ROOT_ONLY`` OFF → quarantine check skipped even
        with quarantine_store wired in (defensive: a stale store instance
        on a worker built with the flag OFF must NOT silently start
        skipping events)."""
        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        store = _build_store(backend)
        for _ in range(3):
            store.record_failure(event_id=8001)
        spy = _SpyOrchestrator(report=_FakeReport(edges_pending=True))

        # Flag OFF — passing quarantine_store should have no effect.
        with _patched_env(LIVE_TIER_1_ROOT_ONLY=None):
            worker = _build_worker(
                backend=backend, queue=queue, orchestrator=spy, quarantine_store=store
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        self.assertEqual(result, "completed")
        # Orchestrator was called — quarantine check did not engage.
        self.assertEqual(len(spy.calls), 1)
        self.assertNotEqual(spy.calls[0][2], "root_only")  # legacy mode resolved

    async def test_quarantine_does_not_apply_on_non_tier_1_lanes(self) -> None:
        """Even with the flag on, lanes other than tier_1 must not consult
        quarantine — keeps blast radius scoped."""
        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        store = _build_store(backend)
        for _ in range(3):
            store.record_failure(event_id=8001)
        spy = _SpyOrchestrator(report=_FakeReport(edges_pending=False))

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = _build_worker(
                backend=backend,
                queue=queue,
                orchestrator=spy,
                quarantine_store=store,
                lane="tier_2",
            )
            result = await worker.handle(_live_entry(lane_stream="stream:etl:live_tier_2"))

        self.assertEqual(result, "completed")
        # tier_2 lane → no quarantine check → orchestrator ran.
        self.assertEqual(len(spy.calls), 1)


# ---------------------------------------------------------------------------
# 3. Success/Failure bookkeeping
# ---------------------------------------------------------------------------
class QuarantineBookkeepingTests(unittest.IsolatedAsyncioTestCase):
    async def test_retryable_failure_records_failure(self) -> None:
        from schema_inspector.services.retry_policy import RetryableJobError

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        store = _build_store(backend)
        spy = _SpyOrchestrator(
            raise_on_run=RetryableJobError("simulated upstream timeout")
        )

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = _build_worker(
                backend=backend, queue=queue, orchestrator=spy, quarantine_store=store
            )
            with self.assertRaises(RetryableJobError):
                await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        # Counter increments by one — verify via the counter key.
        counter_key = "live:tier1_retry_failed:8001"
        self.assertEqual(int(backend.values.get(counter_key, "0")), 1)

    async def test_three_retryable_failures_trigger_quarantine(self) -> None:
        from schema_inspector.services.retry_policy import RetryableJobError

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        store = _build_store(backend)
        spy = _SpyOrchestrator(
            raise_on_run=RetryableJobError("simulated upstream timeout")
        )

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = _build_worker(
                backend=backend, queue=queue, orchestrator=spy, quarantine_store=store
            )
            for _ in range(3):
                with self.assertRaises(RetryableJobError):
                    await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        # After three failures the event is quarantined.
        self.assertGreater(store.is_quarantined(event_id=8001), 0)
        # Next handle() returns quarantined_skip without raising.
        spy2 = _SpyOrchestrator(report=_FakeReport(edges_pending=True))
        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker2 = _build_worker(
                backend=backend, queue=queue, orchestrator=spy2, quarantine_store=store
            )
            result = await worker2.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))
        self.assertEqual(result, "quarantined_skip")
        self.assertEqual(spy2.calls, [])

    async def test_non_retryable_failure_does_not_record_failure(self) -> None:
        """AttributeError / RuntimeError must surface unchanged AND must
        NOT feed the quarantine counter — those are bugs, not bad routes."""
        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        store = _build_store(backend)
        spy = _SpyOrchestrator(
            raise_on_run=AttributeError("simulated logic bug")
        )

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = _build_worker(
                backend=backend, queue=queue, orchestrator=spy, quarantine_store=store
            )
            with self.assertRaises(AttributeError):
                await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        counter_key = "live:tier1_retry_failed:8001"
        # Counter must remain 0 — non-retryable error → no bookkeeping.
        self.assertEqual(int(backend.values.get(counter_key, "0")), 0)

    async def test_success_clears_existing_quarantine(self) -> None:
        from schema_inspector.services.retry_policy import RetryableJobError

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        store = _build_store(backend)

        # First: 3 failures put the event into quarantine.
        spy_fail = _SpyOrchestrator(raise_on_run=RetryableJobError("timeout"))
        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker_fail = _build_worker(
                backend=backend,
                queue=queue,
                orchestrator=spy_fail,
                quarantine_store=store,
            )
            for _ in range(3):
                with self.assertRaises(RetryableJobError):
                    await worker_fail.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))
        self.assertGreater(store.is_quarantined(event_id=8001), 0)

        # Manually clear the quarantine marker (as if cooldown expired in
        # real prod), then run a successful handle().
        backend.delete("live:tier1_quarantine:8001")
        spy_ok = _SpyOrchestrator(report=_FakeReport(edges_pending=True))
        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker_ok = _build_worker(
                backend=backend,
                queue=queue,
                orchestrator=spy_ok,
                quarantine_store=store,
            )
            result = await worker_ok.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))
        self.assertEqual(result, "completed")

        # Counter must be cleared after success.
        self.assertEqual(
            int(backend.values.get("live:tier1_retry_failed:8001", "0")), 0
        )

    async def test_coalesced_return_does_not_record_failure_or_success(self) -> None:
        """Pre-claim ``live:root_inflight:{event_id}`` so the next worker
        handle() short-circuits at the inflight check and returns
        ``coalesced_inflight_root_only``. Neither record_failure nor
        record_success may have been called — a coalesce is not a fetch
        attempt and must not feed the counter."""
        from schema_inspector.queue.live_inflight import (
            LIVE_EVENT_ROOT_INFLIGHT_KEY,
        )

        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        # Pre-claim the root-only inflight key with a different owner so
        # the worker's claim() returns False and the handler short-circuits.
        backend.values[LIVE_EVENT_ROOT_INFLIGHT_KEY.format(event_id=8001)] = (
            "some-other-worker:msg-id:job-id"
        )
        store = _build_store(backend)
        spy = _SpyOrchestrator(report=_FakeReport(edges_pending=True))

        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = _build_worker(
                backend=backend, queue=queue, orchestrator=spy, quarantine_store=store
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        self.assertEqual(result, "coalesced_inflight_root_only")
        # Orchestrator never ran.
        self.assertEqual(spy.calls, [])
        # Counter must be zero — coalesce is NOT a failure.
        self.assertEqual(
            int(backend.values.get("live:tier1_retry_failed:8001", "0")), 0
        )
        # And no quarantine marker was created.
        self.assertEqual(store.is_quarantined(event_id=8001), 0)


# ---------------------------------------------------------------------------
# 4. Global cap fail-open
# ---------------------------------------------------------------------------
class QuarantineGlobalCapTests(unittest.IsolatedAsyncioTestCase):
    async def test_global_cap_exceeded_fails_open_and_runs_orchestrator(self) -> None:
        """When the global-cap brake says too many events are quarantined,
        the worker must FALL THROUGH to normal processing — even for an
        otherwise-quarantined event. Prevents a runaway-quarantine from
        parking the entire inprogress set."""
        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        store = _build_store(backend, global_cap_pct=25)
        # Push enough events into quarantine to exceed the 25% cap given
        # inprogress=8 → cap=2, we need >2 quarantined.
        for event_id in (1, 2, 3, 4):
            for _ in range(3):
                store.record_failure(event_id=event_id)

        # Sanity: the target event is also quarantined.
        for _ in range(3):
            store.record_failure(event_id=8001)
        self.assertGreater(store.is_quarantined(event_id=8001), 0)

        spy = _SpyOrchestrator(report=_FakeReport(edges_pending=True))
        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = _build_worker(
                backend=backend,
                queue=queue,
                orchestrator=spy,
                quarantine_store=store,
                inprogress_count_provider=lambda: 8,
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        # Cap exceeded → fall through → root-only run executed normally.
        self.assertEqual(result, "completed")
        self.assertEqual(len(spy.calls), 1)
        self.assertEqual(spy.calls[0][2], "root_only")

    async def test_inprogress_count_provider_none_does_not_break_quarantine(self) -> None:
        """When no provider is wired (default test config), quarantine
        still works — the cap-check just cannot evaluate and is skipped."""
        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        store = _build_store(backend)
        for _ in range(3):
            store.record_failure(event_id=8001)

        spy = _SpyOrchestrator(report=_FakeReport(edges_pending=True))
        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = _build_worker(
                backend=backend,
                queue=queue,
                orchestrator=spy,
                quarantine_store=store,
                inprogress_count_provider=None,
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        # Provider is None → cap-check skipped → quarantine engages.
        self.assertEqual(result, "quarantined_skip")
        self.assertEqual(spy.calls, [])

    async def test_inprogress_count_provider_exception_treated_as_zero(self) -> None:
        """Provider raises → treat as 0 inprogress → cap-check skipped →
        quarantine still works (so a broken provider does not silently
        leak load onto bad-route events)."""
        backend = _FakeRedisBackend()
        queue = _RecordingQueue(backend=backend)
        store = _build_store(backend)
        for _ in range(3):
            store.record_failure(event_id=8001)

        def _broken_provider():
            raise RuntimeError("simulated provider failure")

        spy = _SpyOrchestrator(report=_FakeReport(edges_pending=True))
        with _patched_env(LIVE_TIER_1_ROOT_ONLY="1"):
            worker = _build_worker(
                backend=backend,
                queue=queue,
                orchestrator=spy,
                quarantine_store=store,
                inprogress_count_provider=_broken_provider,
            )
            result = await worker.handle(_live_entry(lane_stream=STREAM_LIVE_TIER_1))

        self.assertEqual(result, "quarantined_skip")


if __name__ == "__main__":
    unittest.main()
