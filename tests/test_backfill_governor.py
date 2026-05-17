"""Tests for the Task 5 backfill governor.

Pins:
- governor returns None when all live signals are healthy
- governor blocks when oldest_hot_score_age breaches CRIT
- governor blocks when tier_1 quarantine count breaches CRIT
- governor is best-effort: Redis errors do NOT block backfill
- CompositeBackpressure returns first non-None reason and prefixes
  the source class name
"""

from __future__ import annotations

import time
import unittest

from schema_inspector.queue.backfill_governor import (
    BackfillGovernor,
    BackfillGovernorThresholds,
    CompositeBackpressure,
)


class BackfillGovernorThresholdsFromEnvTests(unittest.TestCase):
    def test_defaults_when_env_missing(self) -> None:
        thresholds = BackfillGovernorThresholds.from_env(env={})
        self.assertEqual(thresholds.oldest_hot_score_age_max_seconds, 1800)
        self.assertEqual(thresholds.tier_1_quarantined_max_events, 10)

    def test_env_overrides_oldest_hot_age(self) -> None:
        thresholds = BackfillGovernorThresholds.from_env(
            env={"SOFASCORE_BACKFILL_GOVERNOR_OLDEST_HOT_AGE_MAX_SECONDS": "2400"}
        )
        self.assertEqual(thresholds.oldest_hot_score_age_max_seconds, 2400)
        self.assertEqual(thresholds.tier_1_quarantined_max_events, 10)

    def test_env_overrides_tier_1_quarantined(self) -> None:
        thresholds = BackfillGovernorThresholds.from_env(
            env={"SOFASCORE_BACKFILL_GOVERNOR_TIER_1_QUARANTINED_MAX": "25"}
        )
        self.assertEqual(thresholds.tier_1_quarantined_max_events, 25)
        self.assertEqual(thresholds.oldest_hot_score_age_max_seconds, 1800)

    def test_unparseable_env_falls_back_to_default(self) -> None:
        thresholds = BackfillGovernorThresholds.from_env(
            env={"SOFASCORE_BACKFILL_GOVERNOR_OLDEST_HOT_AGE_MAX_SECONDS": "not-an-int"}
        )
        self.assertEqual(thresholds.oldest_hot_score_age_max_seconds, 1800)

    def test_zero_or_negative_clamped_to_default(self) -> None:
        for raw in ("0", "-100"):
            with self.subTest(raw=raw):
                thresholds = BackfillGovernorThresholds.from_env(
                    env={"SOFASCORE_BACKFILL_GOVERNOR_OLDEST_HOT_AGE_MAX_SECONDS": raw}
                )
                self.assertEqual(thresholds.oldest_hot_score_age_max_seconds, 1800)


class _StubBackend:
    """In-memory Redis stub. Hot zset member->score + keys store."""

    def __init__(self) -> None:
        self.hot_zset: list[tuple[str, float]] = []
        self.quarantine_keys: list[str] = []
        self.zrange_raises = False
        self.scan_iter_raises = False

    def zrange(self, key: str, start: int, end: int, withscores: bool = False):
        if self.zrange_raises:
            raise RuntimeError("forced zrange error")
        if key != "zset:live:hot":
            return []
        items = sorted(self.hot_zset, key=lambda x: x[1])
        if end == -1:
            sliced = items[start:]
        else:
            sliced = items[start : end + 1]
        if withscores:
            return [(m, s) for m, s in sliced]
        return [m for m, _ in sliced]

    def scan_iter(self, match: str, count: int = 100):
        if self.scan_iter_raises:
            raise RuntimeError("forced scan_iter error")
        prefix = match.rstrip("*")
        return (k for k in self.quarantine_keys if k.startswith(prefix))


class BackfillGovernorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.now_ms = 1_700_000_000_000
        # Override time.time() during this test so tests are deterministic.
        self._real_time = time.time
        time.time = lambda: self.now_ms / 1000.0  # type: ignore[assignment]

    def tearDown(self) -> None:
        time.time = self._real_time  # type: ignore[assignment]

    def _governor(self, backend, *, hot_age_max=1800, quarantined_max=10):
        # Disable cache so each test sees the live state.
        return BackfillGovernor(
            redis_backend=backend,
            thresholds=BackfillGovernorThresholds(
                oldest_hot_score_age_max_seconds=hot_age_max,
                tier_1_quarantined_max_events=quarantined_max,
            ),
            cache_ttl_seconds=0.0,
        )

    def test_returns_none_when_all_signals_healthy(self) -> None:
        backend = _StubBackend()
        # Fresh hot zset entry — 30 seconds old.
        backend.hot_zset = [("event-1", float(self.now_ms - 30_000))]
        backend.quarantine_keys = []
        governor = self._governor(backend)
        self.assertIsNone(governor.blocking_reason())

    def test_blocks_when_oldest_hot_age_breaches_crit(self) -> None:
        backend = _StubBackend()
        # Stale: 2000 seconds old > default 1800
        backend.hot_zset = [("event-1", float(self.now_ms - 2_000_000))]
        governor = self._governor(backend)
        reason = governor.blocking_reason()
        self.assertIsNotNone(reason)
        self.assertIn("oldest_hot_score_age", reason or "")
        self.assertIn(">", reason or "")

    def test_blocks_when_tier_1_quarantine_breaches_crit(self) -> None:
        backend = _StubBackend()
        backend.hot_zset = [("event-1", float(self.now_ms - 10_000))]
        backend.quarantine_keys = [
            f"live:tier1_quarantine:{i}" for i in range(15)  # >10 default
        ]
        governor = self._governor(backend)
        reason = governor.blocking_reason()
        self.assertIsNotNone(reason)
        self.assertIn("tier_1_quarantined_events", reason or "")
        self.assertIn("15>10", reason or "")

    def test_oldest_hot_age_check_runs_first(self) -> None:
        backend = _StubBackend()
        backend.hot_zset = [("event-1", float(self.now_ms - 2_000_000))]
        backend.quarantine_keys = [
            f"live:tier1_quarantine:{i}" for i in range(20)
        ]
        governor = self._governor(backend)
        reason = governor.blocking_reason()
        # oldest_hot_age wins because we check it first.
        self.assertIn("oldest_hot_score_age", reason or "")

    def test_redis_error_does_not_block(self) -> None:
        backend = _StubBackend()
        backend.zrange_raises = True
        backend.scan_iter_raises = True
        governor = self._governor(backend)
        # Both probes fail → both return None → governor returns None
        # ("can't measure → don't block backfill").
        self.assertIsNone(governor.blocking_reason())

    def test_caches_result_within_ttl(self) -> None:
        backend = _StubBackend()
        backend.hot_zset = [("event-1", float(self.now_ms - 10_000))]
        # Custom clock — start at t=100, governor sees identical state
        # but second call should be cached because TTL = 5s.
        clock_val = [100.0]
        governor = BackfillGovernor(
            redis_backend=backend,
            thresholds=BackfillGovernorThresholds(),
            cache_ttl_seconds=5.0,
            clock=lambda: clock_val[0],
        )

        first = governor.blocking_reason()
        backend.hot_zset = [("event-1", float(self.now_ms - 9_999_000))]  # stale now
        clock_val[0] = 102.0  # +2s, still within TTL
        second = governor.blocking_reason()
        self.assertEqual(first, second)
        self.assertIsNone(first)

        clock_val[0] = 106.0  # +6s, past TTL
        third = governor.blocking_reason()
        self.assertIsNotNone(third)


class BackfillGovernorGradientTests(unittest.TestCase):
    """Gradient (soft) throttle in the WARN zone between half-threshold and
    the CRIT threshold.

    Current binary behaviour (block above 1800s, no-op below) creates a
    cliff: one slightly-old event flips the planner from full speed to
    full pause and historical lag never drains. With gradient, the
    planner gets a smooth ramp:

      age <= warn       (default warn = max/2 = 900s)  : full speed
      warn < age <= max (900s..1800s)                  : skip a deterministic
                                                          fraction of ticks
                                                          proportional to
                                                          (age - warn) / (max - warn)
      age > max         (>1800s)                       : full pause (existing)

    Throttle is communicated via the same ``blocking_reason()`` channel —
    planners that already pause on any non-None reason will Just Work:
    they skip the throttled tick exactly the same way they skip a CRIT
    tick. The only difference is that the next call after a soft-skip
    has the chance to come back as None and let the tick proceed.
    """

    def setUp(self) -> None:
        self.now_ms = 1_700_000_000_000
        self._real_time = time.time
        time.time = lambda: self.now_ms / 1000.0  # type: ignore[assignment]

    def tearDown(self) -> None:
        time.time = self._real_time  # type: ignore[assignment]

    def _governor(
        self,
        backend,
        *,
        hot_age_max=1800,
        hot_age_warn=None,
        quarantined_max=10,
    ):
        kwargs = {
            "oldest_hot_score_age_max_seconds": hot_age_max,
            "tier_1_quarantined_max_events": quarantined_max,
        }
        if hot_age_warn is not None:
            kwargs["oldest_hot_score_age_warn_seconds"] = hot_age_warn
        return BackfillGovernor(
            redis_backend=backend,
            thresholds=BackfillGovernorThresholds(**kwargs),
            cache_ttl_seconds=0.0,
        )

    def test_no_throttle_when_age_below_warn(self) -> None:
        backend = _StubBackend()
        # age = 500s, max=1800, warn defaults to 900 → below warn → no-op
        backend.hot_zset = [("event-1", float(self.now_ms - 500_000))]
        governor = self._governor(backend)
        for _ in range(10):
            self.assertIsNone(governor.blocking_reason())

    def test_full_pause_above_max_unchanged_behaviour(self) -> None:
        backend = _StubBackend()
        backend.hot_zset = [("event-1", float(self.now_ms - 2_000_000))]
        governor = self._governor(backend)
        for _ in range(5):
            reason = governor.blocking_reason()
            self.assertIsNotNone(reason)
            self.assertIn("oldest_hot_score_age=", reason or "")
            self.assertIn(">1800s", reason or "")

    def test_default_warn_is_half_of_max(self) -> None:
        thresholds = BackfillGovernorThresholds(oldest_hot_score_age_max_seconds=1800)
        self.assertEqual(thresholds.oldest_hot_score_age_warn_seconds, 900)

    def test_partial_throttle_in_warn_zone_skips_half_at_midpoint(self) -> None:
        backend = _StubBackend()
        # Midpoint between warn(900) and max(1800) → 1350s.
        # Fraction = (1350 - 900) / (1800 - 900) = 0.5 → ~50% ticks skipped.
        backend.hot_zset = [("event-1", float(self.now_ms - 1_350_000))]
        governor = self._governor(backend)
        skipped = 0
        total = 100
        for _ in range(total):
            if governor.blocking_reason() is not None:
                skipped += 1
        # Allow some leeway for the deterministic counter — anywhere
        # close to 50% means the throttle is at the right magnitude.
        self.assertGreaterEqual(skipped, 40)
        self.assertLessEqual(skipped, 60)

    def test_partial_throttle_skips_few_near_warn(self) -> None:
        backend = _StubBackend()
        # age=1000s → fraction = (1000 - 900) / 900 = ~0.11 → ~11% skips
        backend.hot_zset = [("event-1", float(self.now_ms - 1_000_000))]
        governor = self._governor(backend)
        skipped = 0
        total = 100
        for _ in range(total):
            if governor.blocking_reason() is not None:
                skipped += 1
        self.assertGreaterEqual(skipped, 5)
        self.assertLessEqual(skipped, 20)

    def test_partial_throttle_skips_most_near_max(self) -> None:
        backend = _StubBackend()
        # age=1750s → fraction = (1750 - 900) / 900 = ~0.944 → ~94% skips
        backend.hot_zset = [("event-1", float(self.now_ms - 1_750_000))]
        governor = self._governor(backend)
        skipped = 0
        total = 100
        for _ in range(total):
            if governor.blocking_reason() is not None:
                skipped += 1
        self.assertGreaterEqual(skipped, 88)

    def test_throttle_reason_label(self) -> None:
        """Throttled-tick reason should clearly identify itself."""
        backend = _StubBackend()
        backend.hot_zset = [("event-1", float(self.now_ms - 1_350_000))]
        governor = self._governor(backend)
        # Run until we get a throttled reason (within 5 tries at midpoint).
        reasons: list[str] = []
        for _ in range(10):
            r = governor.blocking_reason()
            if r is not None:
                reasons.append(r)
        self.assertTrue(reasons, "expected at least one throttled tick at midpoint")
        first = reasons[0]
        self.assertIn("throttle", first.lower())
        # Reason should NOT use the binary > comparator (that is the
        # CRIT/full-pause format).
        self.assertNotIn(">1800s", first)

    def test_env_overrides_warn_threshold(self) -> None:
        thresholds = BackfillGovernorThresholds.from_env(
            env={
                "SOFASCORE_BACKFILL_GOVERNOR_OLDEST_HOT_AGE_MAX_SECONDS": "1800",
                "SOFASCORE_BACKFILL_GOVERNOR_OLDEST_HOT_AGE_WARN_SECONDS": "600",
            }
        )
        self.assertEqual(thresholds.oldest_hot_score_age_max_seconds, 1800)
        self.assertEqual(thresholds.oldest_hot_score_age_warn_seconds, 600)


class CompositeBackpressureTests(unittest.TestCase):
    def test_returns_none_when_all_checks_clear(self) -> None:
        class _A:
            def blocking_reason(self):
                return None

        class _B:
            def blocking_reason(self):
                return None

        composite = CompositeBackpressure([_A(), _B()])
        self.assertIsNone(composite.blocking_reason())

    def test_returns_first_non_none_with_class_prefix(self) -> None:
        class FastCheck:
            def blocking_reason(self):
                return None

        class SlowCheck:
            def blocking_reason(self):
                return "stream:hydrate lag=999>500"

        composite = CompositeBackpressure([FastCheck(), SlowCheck()])
        reason = composite.blocking_reason()
        self.assertIsNotNone(reason)
        self.assertTrue(reason.startswith("SlowCheck:"))
        self.assertIn("stream:hydrate lag=999>500", reason)

    def test_swallows_check_exceptions(self) -> None:
        class BrokenCheck:
            def blocking_reason(self):
                raise RuntimeError("boom")

        class GoodCheck:
            def blocking_reason(self):
                return "downstream=1>0"

        composite = CompositeBackpressure([BrokenCheck(), GoodCheck()])
        reason = composite.blocking_reason()
        self.assertIn("GoodCheck:", reason or "")

    def test_skips_none_entries(self) -> None:
        class GoodCheck:
            def blocking_reason(self):
                return "x=1>0"

        composite = CompositeBackpressure([None, GoodCheck()])
        reason = composite.blocking_reason()
        self.assertIn("GoodCheck:", reason or "")


if __name__ == "__main__":
    unittest.main()
