"""Unit tests for ``EventCircuitBreaker`` (P5b generic per-event quarantine).

Mirrors the test structure of ``test_live_tier_1_quarantine.py``
(P0(c) quarantine on tier_1). Verifies:

* threshold trigger after K real-work failures
* lane-namespaced Redis keys (hydrate ≠ discovery ≠ tier_1)
* sliding-window TTL refresh
* exponential cooldown backoff across re-trigger cycles
* cooldown cap
* success clears counter + marker
* global-cap brake activation
* fail-open on Redis exceptions
* env override per lane (EVENT_CB_{LANE}_THRESHOLD)
"""

from __future__ import annotations

import unittest


class EventCircuitBreakerTests(unittest.TestCase):
    def _build(self, lane="hydrate", **overrides):
        from schema_inspector.queue.event_circuit_breaker import EventCircuitBreaker

        clock = _Clock(start_ms=1_700_000_000_000)
        backend = _FakeRedisBackend(clock=clock)
        defaults = dict(
            lane=lane,
            threshold=3,
            window_seconds=600,
            base_cooldown_seconds=60,
            max_cooldown_seconds=600,
            global_cap_pct=25,
            now_ms_factory=clock.now_ms,
        )
        defaults.update(overrides)
        store = EventCircuitBreaker(backend, **defaults)
        return store, backend, clock

    def test_lane_required(self) -> None:
        from schema_inspector.queue.event_circuit_breaker import EventCircuitBreaker

        with self.assertRaises(ValueError):
            EventCircuitBreaker(_FakeRedisBackend(), lane="")

    def test_lane_namespaces_redis_keys(self) -> None:
        """hydrate-lane keys must not collide with discovery-lane keys —
        guarantees that enabling one circuit breaker does not affect
        another lane's quarantine state."""
        from schema_inspector.queue.event_circuit_breaker import (
            EventCircuitBreaker,
            EVENT_CB_RETRY_FAILED_KEY,
        )

        backend = _FakeRedisBackend()
        hydrate = EventCircuitBreaker(backend, lane="hydrate", threshold=3)
        discovery = EventCircuitBreaker(backend, lane="discovery", threshold=3)

        hydrate.record_failure(event_id=42)
        # Verify the Redis key written under hydrate lane is namespaced.
        self.assertIn(
            EVENT_CB_RETRY_FAILED_KEY.format(lane="hydrate", event_id=42),
            backend.values,
        )
        # Discovery lane should see no counter for event 42.
        self.assertNotIn(
            EVENT_CB_RETRY_FAILED_KEY.format(lane="discovery", event_id=42),
            backend.values,
        )

    def test_not_quarantined_initially(self) -> None:
        store, _, clock = self._build()
        self.assertEqual(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)

    def test_threshold_triggers_quarantine_with_base_cooldown(self) -> None:
        store, _, clock = self._build()
        self.assertFalse(store.record_failure(event_id=42))
        self.assertFalse(store.record_failure(event_id=42))
        self.assertTrue(store.record_failure(event_id=42))
        until_ms = store.is_quarantined(event_id=42, now_ms=clock.now_ms())
        self.assertGreater(until_ms, clock.now_ms())
        # base cooldown 60s
        self.assertEqual(until_ms - clock.now_ms(), 60_000)

    def test_success_clears_counter_and_quarantine(self) -> None:
        store, _, clock = self._build()
        for _ in range(3):
            store.record_failure(event_id=42)
        self.assertGreater(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)
        store.record_success(event_id=42)
        self.assertEqual(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)
        # And counter is reset.
        store.record_failure(event_id=42)
        store.record_failure(event_id=42)
        self.assertEqual(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)

    def test_exponential_backoff_on_re_trigger(self) -> None:
        store, _, clock = self._build()
        # cycle 1
        for _ in range(3):
            store.record_failure(event_id=42)
        cd1 = store.is_quarantined(event_id=42, now_ms=clock.now_ms()) - clock.now_ms()
        self.assertEqual(cd1, 60_000)
        # cycle 2 (active marker still present)
        for _ in range(3):
            store.record_failure(event_id=42)
        cd2 = store.is_quarantined(event_id=42, now_ms=clock.now_ms()) - clock.now_ms()
        self.assertEqual(cd2, 120_000)
        # cycle 3
        for _ in range(3):
            store.record_failure(event_id=42)
        cd3 = store.is_quarantined(event_id=42, now_ms=clock.now_ms()) - clock.now_ms()
        self.assertEqual(cd3, 240_000)

    def test_cooldown_caps_at_max(self) -> None:
        store, _, clock = self._build(base_cooldown_seconds=60, max_cooldown_seconds=200)
        for _ in range(10):
            for _ in range(3):
                store.record_failure(event_id=42)
        cd = store.is_quarantined(event_id=42, now_ms=clock.now_ms()) - clock.now_ms()
        self.assertEqual(cd, 200_000)

    def test_sliding_window_ttl_decay(self) -> None:
        store, _, clock = self._build(window_seconds=600)
        store.record_failure(event_id=42)
        store.record_failure(event_id=42)
        clock.advance_ms(601_000)
        # Counter expired.
        store.record_failure(event_id=42)
        store.record_failure(event_id=42)
        self.assertEqual(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)
        store.record_failure(event_id=42)
        self.assertGreater(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), clock.now_ms())

    def test_quarantine_ttl_expires(self) -> None:
        store, _, clock = self._build(base_cooldown_seconds=60)
        for _ in range(3):
            store.record_failure(event_id=42)
        clock.advance_ms(61_000)
        self.assertEqual(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)

    def test_quarantined_count_via_scan_iter(self) -> None:
        store, _, _ = self._build()
        for event_id in (1, 2, 3):
            for _ in range(3):
                store.record_failure(event_id=event_id)
        store.record_failure(event_id=99)
        store.record_failure(event_id=99)
        self.assertEqual(store.quarantined_count(), 3)

    def test_global_cap_exceeded_true(self) -> None:
        # cap_pct=25, inprogress=8 → cap=2; 3 quarantined → True
        store, _, _ = self._build(global_cap_pct=25)
        for event_id in (1, 2, 3):
            for _ in range(3):
                store.record_failure(event_id=event_id)
        self.assertTrue(store.global_cap_exceeded(inprogress_event_count=8))

    def test_global_cap_exceeded_false_when_under_threshold(self) -> None:
        store, _, _ = self._build(global_cap_pct=25)
        for event_id in (1, 2, 3):
            for _ in range(3):
                store.record_failure(event_id=event_id)
        self.assertFalse(store.global_cap_exceeded(inprogress_event_count=20))

    def test_global_cap_returns_false_for_zero_inprogress(self) -> None:
        """0 inprogress → cannot evaluate → fail-open (False) so a
        startup race doesn't block engagement."""
        store, _, _ = self._build()
        for _ in range(3):
            store.record_failure(event_id=42)
        self.assertFalse(store.global_cap_exceeded(inprogress_event_count=0))

    def test_is_quarantined_fail_open_on_backend_exception(self) -> None:
        from schema_inspector.queue.event_circuit_breaker import EventCircuitBreaker

        clock = _Clock(start_ms=1_700_000_000_000)
        backend = _FlakyBackend(get_raises=True)
        store = EventCircuitBreaker(
            backend, lane="hydrate", threshold=3, window_seconds=600,
            now_ms_factory=clock.now_ms,
        )
        # Backend.get raises → no crash, returns 0 (no quarantine)
        self.assertEqual(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)

    def test_record_failure_fail_open_on_incr_exception(self) -> None:
        from schema_inspector.queue.event_circuit_breaker import EventCircuitBreaker

        clock = _Clock(start_ms=1_700_000_000_000)
        backend = _FlakyBackend(incr_raises=True)
        store = EventCircuitBreaker(
            backend, lane="hydrate", threshold=3, window_seconds=600,
            now_ms_factory=clock.now_ms,
        )
        self.assertFalse(store.record_failure(event_id=42))

    def test_env_overrides_per_lane(self) -> None:
        """EVENT_CB_HYDRATE_THRESHOLD must be read only for hydrate
        lane; EVENT_CB_DISCOVERY_THRESHOLD only for discovery. Same
        knob name pattern, different lane values."""
        import os
        from schema_inspector.queue.event_circuit_breaker import EventCircuitBreaker

        prev = {
            "EVENT_CB_HYDRATE_THRESHOLD": os.environ.pop("EVENT_CB_HYDRATE_THRESHOLD", None),
            "EVENT_CB_DISCOVERY_THRESHOLD": os.environ.pop("EVENT_CB_DISCOVERY_THRESHOLD", None),
        }
        try:
            os.environ["EVENT_CB_HYDRATE_THRESHOLD"] = "7"
            os.environ["EVENT_CB_DISCOVERY_THRESHOLD"] = "11"
            hydrate = EventCircuitBreaker(_FakeRedisBackend(), lane="hydrate")
            discovery = EventCircuitBreaker(_FakeRedisBackend(), lane="discovery")
            self.assertEqual(hydrate.threshold, 7)
            self.assertEqual(discovery.threshold, 11)
        finally:
            for name, value in prev.items():
                if value is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = value

    def test_cycles_reset_after_cooldown_expiry(self) -> None:
        """Re-trigger AFTER cooldown expires must restart at cycle 1,
        not exponentially escalate forever."""
        store, _, clock = self._build(base_cooldown_seconds=60, max_cooldown_seconds=600)
        for _ in range(3):
            store.record_failure(event_id=42)
        clock.advance_ms(61_000)
        for _ in range(3):
            store.record_failure(event_id=42)
        cd2 = store.is_quarantined(event_id=42, now_ms=clock.now_ms()) - clock.now_ms()
        # Marker had expired → cycle restarted at 1 → 60s cooldown again
        self.assertEqual(cd2, 60_000)

    def test_record_failure_returns_false_below_threshold(self) -> None:
        store, _, _ = self._build()
        self.assertFalse(store.record_failure(event_id=42))
        self.assertFalse(store.record_failure(event_id=42))

    def test_max_cooldown_clamped_to_at_least_base(self) -> None:
        from schema_inspector.queue.event_circuit_breaker import EventCircuitBreaker

        store = EventCircuitBreaker(
            _FakeRedisBackend(), lane="hydrate",
            base_cooldown_seconds=60, max_cooldown_seconds=10,
        )
        self.assertEqual(store.max_cooldown_seconds, 60)


# --------------------------------------------------------------------------
# Fakes (same as test_live_tier_1_quarantine.py)
# --------------------------------------------------------------------------


class _Clock:
    def __init__(self, *, start_ms: int) -> None:
        self._now_ms = int(start_ms)

    def now_ms(self) -> int:
        return self._now_ms

    def advance_ms(self, delta_ms: int) -> None:
        self._now_ms += int(delta_ms)


class _FakeRedisBackend:
    def __init__(self, clock: _Clock | None = None) -> None:
        self.values: dict[str, str] = {}
        self.expirations: dict[str, int] = {}
        self._clock = clock or _Clock(start_ms=0)

    def _now_ms(self) -> int:
        return self._clock.now_ms()

    def _expire_check(self, key: str) -> None:
        exp = self.expirations.get(key)
        if exp is None:
            return
        if self._now_ms() >= exp:
            self.values.pop(key, None)
            self.expirations.pop(key, None)

    def get(self, key: str):
        self._expire_check(key)
        return self.values.get(key)

    def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None, px: int | None = None) -> bool:
        self._expire_check(key)
        if nx and key in self.values:
            return False
        self.values[key] = str(value)
        if ex is not None:
            self.expirations[key] = self._now_ms() + int(ex) * 1000
        elif px is not None:
            self.expirations[key] = self._now_ms() + int(px)
        else:
            self.expirations.pop(key, None)
        return True

    def incr(self, key: str) -> int:
        self._expire_check(key)
        current = int(self.values.get(key, "0"))
        current += 1
        self.values[key] = str(current)
        return current

    def expire(self, key: str, seconds: int) -> int:
        if key not in self.values:
            return 0
        self.expirations[key] = self._now_ms() + int(seconds) * 1000
        return 1

    def delete(self, key: str) -> int:
        existed = key in self.values
        self.values.pop(key, None)
        self.expirations.pop(key, None)
        return 1 if existed else 0

    def scan_iter(self, match: str | None = None):
        for key in list(self.values.keys()):
            self._expire_check(key)
            if key not in self.values:
                continue
            if match is None:
                yield key
                continue
            if "*" in match:
                prefix, _, suffix = match.partition("*")
                if key.startswith(prefix) and key.endswith(suffix):
                    yield key
            elif key == match:
                yield key

    def keys(self, pattern: str | None = None):
        return list(self.scan_iter(match=pattern))


class _FlakyBackend:
    def __init__(self, *, get_raises: bool = False, incr_raises: bool = False) -> None:
        self.get_raises = get_raises
        self.incr_raises = incr_raises

    def get(self, key: str):
        if self.get_raises:
            raise RuntimeError("simulated GET failure")
        return None

    def incr(self, key: str) -> int:
        if self.incr_raises:
            raise RuntimeError("simulated INCR failure")
        return 1

    def expire(self, key: str, seconds: int) -> int:
        return 1

    def set(self, key, value, **kwargs):  # pragma: no cover
        return True

    def delete(self, key: str) -> int:  # pragma: no cover
        return 1

    def scan_iter(self, match=None):  # pragma: no cover
        return iter(())


if __name__ == "__main__":
    unittest.main()
