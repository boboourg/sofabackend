"""Unit tests for ``LiveTier1RetryQuarantineStore``.

Covers:

* threshold trigger after K real-work failures
* sliding-window TTL (each failure refreshes window)
* exponential backoff across re-trigger cycles
* cap on cooldown duration
* success clears both counter and quarantine marker
* global-cap brake activation
* fail-open on Redis exceptions
"""

from __future__ import annotations

import json
import unittest


class LiveTier1RetryQuarantineStoreTests(unittest.TestCase):
    def _build(self, **overrides):
        from schema_inspector.queue.live_tier_1_quarantine import (
            LiveTier1RetryQuarantineStore,
        )

        clock = _Clock(start_ms=1_700_000_000_000)
        backend = _FakeRedisBackend(clock=clock)
        defaults = dict(
            threshold=3,
            window_seconds=600,
            base_cooldown_seconds=60,
            max_cooldown_seconds=600,
            global_cap_pct=25,
            now_ms_factory=clock.now_ms,
        )
        defaults.update(overrides)
        store = LiveTier1RetryQuarantineStore(backend, **defaults)
        return store, backend, clock

    def test_not_quarantined_initially(self) -> None:
        store, _, clock = self._build()
        self.assertEqual(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)

    def test_high_value_failures_never_quarantine(self) -> None:
        store, backend, clock = self._build()
        for _ in range(5):
            self.assertFalse(store.record_failure(event_id=99, high_value=True))
        self.assertEqual(store.is_quarantined(event_id=99, now_ms=clock.now_ms()), 0)
        self.assertNotIn("live:tier1_retry_failed:99", backend.values)

    def test_threshold_triggers_quarantine_with_base_cooldown(self) -> None:
        store, backend, clock = self._build()
        self.assertFalse(store.record_failure(event_id=42))
        self.assertFalse(store.record_failure(event_id=42))
        # third failure trips threshold
        self.assertTrue(store.record_failure(event_id=42))
        until_ms = store.is_quarantined(event_id=42, now_ms=clock.now_ms())
        self.assertGreater(until_ms, clock.now_ms())
        # base_cooldown_seconds=60 → ~60s in the future
        self.assertEqual(until_ms - clock.now_ms(), 60_000)

    def test_success_clears_counter_and_quarantine(self) -> None:
        store, backend, clock = self._build()
        for _ in range(3):
            store.record_failure(event_id=42)
        self.assertGreater(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)
        store.record_success(event_id=42)
        self.assertEqual(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)
        # And counter is reset — next 2 failures must not re-trigger
        store.record_failure(event_id=42)
        store.record_failure(event_id=42)
        self.assertEqual(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)

    def test_exponential_backoff_on_re_trigger(self) -> None:
        store, backend, clock = self._build()
        # First trigger
        for _ in range(3):
            store.record_failure(event_id=42)
        first_until = store.is_quarantined(event_id=42, now_ms=clock.now_ms())
        first_cooldown = first_until - clock.now_ms()
        self.assertEqual(first_cooldown, 60_000)
        # Re-trigger BEFORE first quarantine expires → cycles=2 → 120s cooldown
        for _ in range(3):
            store.record_failure(event_id=42)
        second_until = store.is_quarantined(event_id=42, now_ms=clock.now_ms())
        second_cooldown = second_until - clock.now_ms()
        self.assertEqual(second_cooldown, 120_000)
        # Third trigger → cycles=3 → 240s
        for _ in range(3):
            store.record_failure(event_id=42)
        third_cooldown = (
            store.is_quarantined(event_id=42, now_ms=clock.now_ms()) - clock.now_ms()
        )
        self.assertEqual(third_cooldown, 240_000)

    def test_cooldown_caps_at_max(self) -> None:
        store, _, clock = self._build(base_cooldown_seconds=60, max_cooldown_seconds=200)
        # Force many cycles
        for cycle in range(10):
            for _ in range(3):
                store.record_failure(event_id=42)
        cooldown = (
            store.is_quarantined(event_id=42, now_ms=clock.now_ms()) - clock.now_ms()
        )
        self.assertEqual(cooldown, 200_000)

    def test_sliding_window_ttl_expiry_clears_counter(self) -> None:
        store, backend, clock = self._build(window_seconds=600)
        store.record_failure(event_id=42)
        store.record_failure(event_id=42)
        # Advance time past window TTL → counter expires; only one failure
        # was within window, so threshold not reached.
        clock.advance_ms(601_000)
        store.record_failure(event_id=42)
        store.record_failure(event_id=42)
        # Only 2 failures within current window → no quarantine
        self.assertEqual(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)
        # One more → 3 within window → triggers
        store.record_failure(event_id=42)
        self.assertGreater(
            store.is_quarantined(event_id=42, now_ms=clock.now_ms()), clock.now_ms()
        )

    def test_quarantine_ttl_expires_naturally(self) -> None:
        store, _, clock = self._build(base_cooldown_seconds=60)
        for _ in range(3):
            store.record_failure(event_id=42)
        # Wait past cooldown
        clock.advance_ms(61_000)
        self.assertEqual(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)

    def test_is_quarantined_returns_zero_for_unknown_event(self) -> None:
        store, _, clock = self._build()
        self.assertEqual(store.is_quarantined(event_id=999, now_ms=clock.now_ms()), 0)

    def test_quarantined_count_via_scan_iter(self) -> None:
        store, backend, _ = self._build()
        for event_id in (1, 2, 3):
            for _ in range(3):
                store.record_failure(event_id=event_id)
        # Sanity: one extra event_id with sub-threshold failures should NOT count
        store.record_failure(event_id=99)
        store.record_failure(event_id=99)
        self.assertEqual(store.quarantined_count(), 3)

    def test_global_cap_exceeded_true_when_above_threshold(self) -> None:
        # cap_pct=25, inprogress=8, quarantined=3 → 3 > 2 (25% of 8) → True
        store, _, _ = self._build(global_cap_pct=25)
        for event_id in (1, 2, 3):
            for _ in range(3):
                store.record_failure(event_id=event_id)
        self.assertTrue(store.global_cap_exceeded(inprogress_event_count=8))

    def test_global_cap_exceeded_false_when_below_threshold(self) -> None:
        # cap_pct=25, inprogress=20, quarantined=3 → 3 <= 5 (25% of 20) → False
        store, _, _ = self._build(global_cap_pct=25)
        for event_id in (1, 2, 3):
            for _ in range(3):
                store.record_failure(event_id=event_id)
        self.assertFalse(store.global_cap_exceeded(inprogress_event_count=20))

    def test_global_cap_exceeded_returns_false_for_zero_inprogress(self) -> None:
        store, _, _ = self._build()
        for _ in range(3):
            store.record_failure(event_id=42)
        # 0 inprogress → cannot evaluate → fail-open returns False
        self.assertFalse(store.global_cap_exceeded(inprogress_event_count=0))

    def test_is_quarantined_fail_open_on_backend_exception(self) -> None:
        from schema_inspector.queue.live_tier_1_quarantine import (
            LiveTier1RetryQuarantineStore,
        )

        clock = _Clock(start_ms=1_700_000_000_000)
        backend = _FlakyBackend(get_raises=True)
        store = LiveTier1RetryQuarantineStore(
            backend, threshold=3, window_seconds=600, now_ms_factory=clock.now_ms
        )
        # Backend.get raises → should not crash, should return 0 (no quarantine)
        self.assertEqual(store.is_quarantined(event_id=42, now_ms=clock.now_ms()), 0)

    def test_record_failure_fail_open_on_incr_exception(self) -> None:
        from schema_inspector.queue.live_tier_1_quarantine import (
            LiveTier1RetryQuarantineStore,
        )

        clock = _Clock(start_ms=1_700_000_000_000)
        backend = _FlakyBackend(incr_raises=True)
        store = LiveTier1RetryQuarantineStore(
            backend, threshold=3, window_seconds=600, now_ms_factory=clock.now_ms
        )
        # incr raises → record_failure returns False, no crash
        self.assertFalse(store.record_failure(event_id=42))

    def test_env_overrides_threshold(self) -> None:
        import os
        from schema_inspector.queue.live_tier_1_quarantine import (
            LiveTier1RetryQuarantineStore,
        )

        original = os.environ.pop("LIVE_TIER_1_QUARANTINE_THRESHOLD", None)
        try:
            os.environ["LIVE_TIER_1_QUARANTINE_THRESHOLD"] = "5"
            clock = _Clock(start_ms=1_700_000_000_000)
            backend = _FakeRedisBackend(clock=clock)
            store = LiveTier1RetryQuarantineStore(backend, now_ms_factory=clock.now_ms)
            self.assertEqual(store.threshold, 5)
        finally:
            if original is None:
                os.environ.pop("LIVE_TIER_1_QUARANTINE_THRESHOLD", None)
            else:
                os.environ["LIVE_TIER_1_QUARANTINE_THRESHOLD"] = original

    def test_env_invalid_value_falls_back_to_default(self) -> None:
        import os
        from schema_inspector.queue.live_tier_1_quarantine import (
            LiveTier1RetryQuarantineStore,
        )

        original = os.environ.pop("LIVE_TIER_1_QUARANTINE_THRESHOLD", None)
        try:
            os.environ["LIVE_TIER_1_QUARANTINE_THRESHOLD"] = "not_a_number"
            clock = _Clock(start_ms=1_700_000_000_000)
            backend = _FakeRedisBackend(clock=clock)
            store = LiveTier1RetryQuarantineStore(backend, now_ms_factory=clock.now_ms)
            self.assertEqual(store.threshold, 3)  # default
        finally:
            if original is None:
                os.environ.pop("LIVE_TIER_1_QUARANTINE_THRESHOLD", None)
            else:
                os.environ["LIVE_TIER_1_QUARANTINE_THRESHOLD"] = original

    def test_cycles_increment_only_when_quarantine_marker_still_present(self) -> None:
        """Re-trigger AFTER cooldown expires should re-enter cycle 1, not 2.

        Otherwise an event that has one timeout per hour for a day would
        exponentially escalate to the cap even though the system recovered
        between each failure.
        """
        store, _, clock = self._build(base_cooldown_seconds=60, max_cooldown_seconds=600)
        # First trigger
        for _ in range(3):
            store.record_failure(event_id=42)
        first_cooldown = (
            store.is_quarantined(event_id=42, now_ms=clock.now_ms()) - clock.now_ms()
        )
        self.assertEqual(first_cooldown, 60_000)
        # Wait past cooldown
        clock.advance_ms(61_000)
        # Re-trigger from a clean slate
        for _ in range(3):
            store.record_failure(event_id=42)
        second_cooldown = (
            store.is_quarantined(event_id=42, now_ms=clock.now_ms()) - clock.now_ms()
        )
        # No active marker → cycles starts at 1 again → 60s cooldown
        self.assertEqual(second_cooldown, 60_000)

    def test_record_failure_returns_false_below_threshold(self) -> None:
        store, _, _ = self._build()
        self.assertFalse(store.record_failure(event_id=42))
        self.assertFalse(store.record_failure(event_id=42))

    def test_max_cooldown_at_least_base(self) -> None:
        """Construction must not allow ``max_cooldown_seconds < base_cooldown_seconds``."""
        from schema_inspector.queue.live_tier_1_quarantine import (
            LiveTier1RetryQuarantineStore,
        )

        backend = _FakeRedisBackend()
        store = LiveTier1RetryQuarantineStore(
            backend, base_cooldown_seconds=60, max_cooldown_seconds=10
        )
        # Max should be clamped up to base
        self.assertEqual(store.max_cooldown_seconds, 60)


# --------------------------------------------------------------------------
# Fakes
# --------------------------------------------------------------------------


class _Clock:
    def __init__(self, *, start_ms: int) -> None:
        self._now_ms = int(start_ms)

    def now_ms(self) -> int:
        return self._now_ms

    def advance_ms(self, delta_ms: int) -> None:
        self._now_ms += int(delta_ms)


class _FakeRedisBackend:
    """Minimal Redis stand-in with TTL semantics tied to ``_Clock``."""

    def __init__(self, clock: _Clock | None = None) -> None:
        self.values: dict[str, str] = {}
        self.expirations: dict[str, int] = {}  # key -> epoch_ms when expires
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
    """Backend that selectively raises for fail-open assertions."""

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

    def set(self, key, value, **kwargs):  # pragma: no cover - not reached when incr raises
        return True

    def delete(self, key: str) -> int:  # pragma: no cover
        return 1

    def scan_iter(self, match=None):  # pragma: no cover
        return iter(())


if __name__ == "__main__":
    unittest.main()
