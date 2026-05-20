"""P5b integration tests — HydrateWorker × EventCircuitBreaker.

Verifies the worker layer correctly:

* Skips + ACKs quarantined events (returns ``quarantined_skip``)
  without calling orchestrator.run_event.
* Does NOT consult circuit breaker when ``circuit_breaker=None``
  (default — preserves legacy fast path under
  HYDRATE_EVENT_CIRCUIT_BREAKER_ENABLED=unset).
* Records ``RetryableJobError`` into the counter via record_failure.
* Clears quarantine on successful run via record_success.
* Does NOT call record_failure for non-retryable errors (e.g.
  AttributeError) — those must surface, not get silently rerouted.
* Fails open when ``global_cap_exceeded`` returns True.

Mirrors the structure of ``test_live_worker_quarantine.py`` (P0(c).2
tests on tier_1) but for the hydrate lane.
"""

from __future__ import annotations

import unittest
from typing import Any

from schema_inspector.queue.streams import STREAM_HYDRATE, StreamEntry


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def get(self, key: str):
        return self.values.get(key)

    def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None, px: int | None = None) -> bool:
        del ex, px
        if nx and key in self.values:
            return False
        self.values[key] = str(value)
        return True

    def incr(self, key: str) -> int:
        current = int(self.values.get(key, "0"))
        current += 1
        self.values[key] = str(current)
        return current

    def expire(self, key: str, seconds: int) -> int:
        del seconds
        return 1 if key in self.values else 0

    def delete(self, key: str) -> int:
        return 1 if self.values.pop(key, None) is not None else 0

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


class _FakeQueue:
    def read_group(self, *args, **kwargs):
        del args, kwargs
        return ()

    def ack(self, *args, **kwargs):
        del args, kwargs
        return 0


class _RaisingOrchestrator:
    def __init__(self, *, exc: Exception) -> None:
        self.exc = exc
        self.calls: list[tuple[int, str | None, str]] = []

    async def run_event(
        self,
        *,
        event_id: int,
        sport_slug: str | None,
        hydration_mode: str = "full",
        scope: str | None = None,
    ):
        del scope
        self.calls.append((event_id, sport_slug, hydration_mode))
        raise self.exc


class _SucceedingOrchestrator:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str | None, str]] = []

    async def run_event(
        self,
        *,
        event_id: int,
        sport_slug: str | None,
        hydration_mode: str = "full",
        scope: str | None = None,
    ):
        del scope
        self.calls.append((event_id, sport_slug, hydration_mode))
        return {"event_id": event_id}


class _NoopDelayedScheduler:
    def schedule(self, *args, **kwargs):
        del args, kwargs


def _build_store(backend: _FakeRedisBackend, **overrides):
    from schema_inspector.queue.event_circuit_breaker import EventCircuitBreaker

    defaults = dict(
        lane="hydrate",
        threshold=3,
        window_seconds=600,
        base_cooldown_seconds=60,
        max_cooldown_seconds=600,
        global_cap_pct=25,
    )
    defaults.update(overrides)
    return EventCircuitBreaker(backend, **defaults)


def _build_worker(
    *,
    orchestrator,
    circuit_breaker=None,
    inprogress_count_provider=None,
):
    from schema_inspector.workers.hydrate_worker import HydrateWorker

    return HydrateWorker(
        orchestrator=orchestrator,
        delayed_scheduler=_NoopDelayedScheduler(),
        queue=_FakeQueue(),
        consumer="worker-hydrate-1",
        circuit_breaker=circuit_breaker,
        circuit_breaker_inprogress_count_provider=inprogress_count_provider,
    )


def _hydrate_entry(*, event_id: int = 9001) -> StreamEntry:
    import json

    return StreamEntry(
        stream=STREAM_HYDRATE,
        message_id="1-1",
        values={
            "job_id": f"job-hydrate-{event_id}",
            "job_type": "hydrate_event_root",
            "sport_slug": "football",
            "entity_type": "event",
            "entity_id": str(event_id),
            "scope": "live",
            "attempt": "1",
            "params_json": json.dumps({"hydration_mode": "full"}),
        },
    )


# ---------------------------------------------------------------------------
# 1. Quarantine SKIP path
# ---------------------------------------------------------------------------
class HydrateCircuitBreakerSkipTests(unittest.IsolatedAsyncioTestCase):
    async def test_quarantined_event_returns_quarantined_skip_without_orchestrator_call(self) -> None:
        backend = _FakeRedisBackend()
        store = _build_store(backend)
        # Pre-trigger quarantine
        for _ in range(3):
            store.record_failure(event_id=9001)
        self.assertGreater(store.is_quarantined(event_id=9001), 0)

        orch = _SucceedingOrchestrator()
        worker = _build_worker(orchestrator=orch, circuit_breaker=store)
        result = await worker.handle(_hydrate_entry(event_id=9001))

        self.assertEqual(result, "quarantined_skip")
        self.assertEqual(orch.calls, [])

    async def test_not_quarantined_event_runs_normally(self) -> None:
        backend = _FakeRedisBackend()
        store = _build_store(backend)
        orch = _SucceedingOrchestrator()
        worker = _build_worker(orchestrator=orch, circuit_breaker=store)
        result = await worker.handle(_hydrate_entry(event_id=9001))

        self.assertEqual(result, "completed")
        self.assertEqual(len(orch.calls), 1)


# ---------------------------------------------------------------------------
# 2. Flag-OFF / store=None regression
# ---------------------------------------------------------------------------
class HydrateCircuitBreakerOffTests(unittest.IsolatedAsyncioTestCase):
    async def test_no_circuit_breaker_store_means_legacy_path(self) -> None:
        """``circuit_breaker=None`` (default — flag unset in service_app)
        MUST preserve the pre-P5b behaviour exactly: orchestrator is
        always called, no quarantine_skip path engaged."""
        backend = _FakeRedisBackend()
        store = _build_store(backend)
        # Even with backend pre-loaded with a quarantine marker, the
        # worker should NOT consult it when circuit_breaker=None.
        for _ in range(3):
            store.record_failure(event_id=9001)

        orch = _SucceedingOrchestrator()
        worker = _build_worker(orchestrator=orch, circuit_breaker=None)
        result = await worker.handle(_hydrate_entry(event_id=9001))

        self.assertEqual(result, "completed")
        self.assertEqual(len(orch.calls), 1)


# ---------------------------------------------------------------------------
# 3. Success / failure bookkeeping
# ---------------------------------------------------------------------------
class HydrateCircuitBreakerBookkeepingTests(unittest.IsolatedAsyncioTestCase):
    async def test_retryable_failure_increments_counter(self) -> None:
        from schema_inspector.services.retry_policy import RetryableJobError

        backend = _FakeRedisBackend()
        store = _build_store(backend)
        orch = _RaisingOrchestrator(exc=RetryableJobError("simulated timeout"))
        worker = _build_worker(orchestrator=orch, circuit_breaker=store)

        with self.assertRaises(RetryableJobError):
            await worker.handle(_hydrate_entry(event_id=9001))

        counter_key = "live:event_cb:hydrate:retry_failed:9001"
        self.assertEqual(int(backend.values.get(counter_key, "0")), 1)

    async def test_three_retryable_failures_trigger_quarantine(self) -> None:
        from schema_inspector.services.retry_policy import RetryableJobError

        backend = _FakeRedisBackend()
        store = _build_store(backend)
        orch = _RaisingOrchestrator(exc=RetryableJobError("timeout"))
        worker = _build_worker(orchestrator=orch, circuit_breaker=store)

        for _ in range(3):
            with self.assertRaises(RetryableJobError):
                await worker.handle(_hydrate_entry(event_id=9001))

        self.assertGreater(store.is_quarantined(event_id=9001), 0)

    async def test_non_retryable_error_does_not_feed_counter(self) -> None:
        """AttributeError / RuntimeError / TypeError must surface
        AND must NOT increment the quarantine counter — those are
        application bugs, not transient transport failures."""
        backend = _FakeRedisBackend()
        store = _build_store(backend)
        orch = _RaisingOrchestrator(exc=AttributeError("simulated logic bug"))
        worker = _build_worker(orchestrator=orch, circuit_breaker=store)

        with self.assertRaises(AttributeError):
            await worker.handle(_hydrate_entry(event_id=9001))

        counter_key = "live:event_cb:hydrate:retry_failed:9001"
        self.assertEqual(int(backend.values.get(counter_key, "0")), 0)

    async def test_successful_run_clears_existing_quarantine(self) -> None:
        backend = _FakeRedisBackend()
        store = _build_store(backend)
        # Force quarantine first.
        for _ in range(3):
            store.record_failure(event_id=9001)
        self.assertGreater(store.is_quarantined(event_id=9001), 0)
        # Drop the marker (simulates cooldown expiry mid-cycle).
        backend.delete("live:event_cb:hydrate:quarantine:9001")
        # Now run handle() — orchestrator succeeds, record_success
        # should clear the counter.
        orch = _SucceedingOrchestrator()
        worker = _build_worker(orchestrator=orch, circuit_breaker=store)
        result = await worker.handle(_hydrate_entry(event_id=9001))

        self.assertEqual(result, "completed")
        self.assertEqual(
            int(backend.values.get("live:event_cb:hydrate:retry_failed:9001", "0")), 0
        )


# ---------------------------------------------------------------------------
# 4. Global-cap fail-open
# ---------------------------------------------------------------------------
class HydrateCircuitBreakerGlobalCapTests(unittest.IsolatedAsyncioTestCase):
    async def test_global_cap_exceeded_falls_through_to_orchestrator(self) -> None:
        backend = _FakeRedisBackend()
        store = _build_store(backend, global_cap_pct=25)
        # 4 events quarantined; inprogress count = 8 → cap = 2; 4 > 2 → exceeded
        for event_id in (1, 2, 3, 4):
            for _ in range(3):
                store.record_failure(event_id=event_id)
        # The target event is also quarantined, but cap-exceeded means fall through
        for _ in range(3):
            store.record_failure(event_id=9001)
        self.assertGreater(store.is_quarantined(event_id=9001), 0)

        orch = _SucceedingOrchestrator()
        worker = _build_worker(
            orchestrator=orch,
            circuit_breaker=store,
            inprogress_count_provider=lambda: 8,
        )
        result = await worker.handle(_hydrate_entry(event_id=9001))

        # Cap exceeded → fall through → orchestrator runs normally.
        self.assertEqual(result, "completed")
        self.assertEqual(len(orch.calls), 1)

    async def test_no_inprogress_provider_means_no_cap_check_quarantine_engages(self) -> None:
        backend = _FakeRedisBackend()
        store = _build_store(backend)
        for _ in range(3):
            store.record_failure(event_id=9001)

        orch = _SucceedingOrchestrator()
        worker = _build_worker(
            orchestrator=orch,
            circuit_breaker=store,
            inprogress_count_provider=None,
        )
        result = await worker.handle(_hydrate_entry(event_id=9001))

        # Provider unset → cap-check skipped → quarantine engages.
        self.assertEqual(result, "quarantined_skip")
        self.assertEqual(orch.calls, [])


if __name__ == "__main__":
    unittest.main()
