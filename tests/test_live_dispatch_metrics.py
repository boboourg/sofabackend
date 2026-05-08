"""F-7 Phase 0 observability tests: dispatch metrics counters + snapshot.

These tests guard the cumulative-counter contract that the F-7 staged
rollout depends on for verification:

* claim_attempts / claim_succeeded / claim_failed_blocked (per tier)
* clear_called (per tier)
* published (per tier)
* dispatch_metrics_snapshot returning the flat int dict
* tier_active_counts deriving counts from hot/warm/cold zsets

Backends without HINCRBY (older test fakes, in-memory dev fallback) MUST
gracefully degrade — metrics may never fail the live path.
"""
from __future__ import annotations

import unittest

from schema_inspector.queue.live_state import (
    LIVE_COLD_ZSET,
    LIVE_DISPATCH_METRICS_KEY,
    LIVE_HOT_ZSET,
    LIVE_WARM_ZSET,
    LiveEventState,
    LiveEventStateStore,
)


class _MetricsBackend:
    """Fake Redis backend supporting the surface used by metrics paths."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.claims: dict[str, object] = {}

    # --- hash ops -------------------------------------------------------

    def hset(self, name: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(name, {}).update(dict(mapping))
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))

    def hincrby(self, name: str, field: str, amount: int = 1) -> int:
        bucket = self.hashes.setdefault(name, {})
        try:
            current = int(bucket.get(field, 0))
        except (TypeError, ValueError):
            current = 0
        new_value = current + int(amount)
        bucket[field] = str(new_value)
        return new_value

    def hsetnx(self, name: str, field: str, value: object) -> int:
        bucket = self.hashes.setdefault(name, {})
        if field in bucket:
            return 0
        bucket[field] = value
        return 1

    # --- zset ops -------------------------------------------------------

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[str(member)] = float(score)
        return len(mapping)

    def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    def zrangebyscore(self, key: str, min_score: float, max_score: float):
        return [
            member
            for member, score in sorted(self.zsets.get(key, {}).items(), key=lambda item: (item[1], item[0]))
            if min_score <= score <= max_score
        ]

    # --- claim string ops ----------------------------------------------

    def set(
        self,
        key: str,
        value: object,
        *,
        nx: bool | None = None,
        px: int | None = None,
    ) -> bool:
        del px
        if nx and key in self.claims:
            return False
        self.claims[key] = value
        return True

    def delete(self, key: str) -> int:
        if key in self.claims:
            del self.claims[key]
            return 1
        return 0


class _BackendWithoutMetrics:
    """Backend missing hincrby/hsetnx — graceful-degrade path."""

    def __init__(self) -> None:
        self.claims: dict[str, object] = {}
        self.hashes: dict[str, dict[str, object]] = {}

    def hset(self, name: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(name, {}).update(dict(mapping))
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        return 0

    def zrem(self, key: str, *members: str) -> int:
        return 0

    def zrangebyscore(self, key: str, min_score: float, max_score: float):
        return []

    def set(self, key, value, *, nx=None, px=None) -> bool:
        del px
        if nx and key in self.claims:
            return False
        self.claims[key] = value
        return True

    def delete(self, key: str) -> int:
        return self.claims.pop(key, None) is not None


class DispatchMetricsTests(unittest.TestCase):
    def test_claim_dispatch_increments_attempts_and_success_on_first_call(self) -> None:
        backend = _MetricsBackend()
        store = LiveEventStateStore(backend)

        ok = store.claim_dispatch(7001, now_ms=1_000_000, lease_ms=90_000, tier="tier_1")

        self.assertTrue(ok)
        snapshot = store.dispatch_metrics_snapshot()
        self.assertEqual(snapshot["claim_attempts:total"], 1)
        self.assertEqual(snapshot["claim_attempts:tier_1"], 1)
        self.assertEqual(snapshot["claim_succeeded:total"], 1)
        self.assertEqual(snapshot["claim_succeeded:tier_1"], 1)
        self.assertNotIn("claim_failed_blocked:total", snapshot)
        self.assertIn("started_at_ms", snapshot)

    def test_claim_dispatch_increments_blocked_when_held(self) -> None:
        backend = _MetricsBackend()
        store = LiveEventStateStore(backend)

        self.assertTrue(store.claim_dispatch(7001, now_ms=1_000_000, lease_ms=90_000, tier="tier_2"))
        self.assertFalse(store.claim_dispatch(7001, now_ms=1_000_001, lease_ms=90_000, tier="tier_2"))

        snapshot = store.dispatch_metrics_snapshot()
        self.assertEqual(snapshot["claim_attempts:total"], 2)
        self.assertEqual(snapshot["claim_attempts:tier_2"], 2)
        self.assertEqual(snapshot["claim_succeeded:total"], 1)
        self.assertEqual(snapshot["claim_succeeded:tier_2"], 1)
        self.assertEqual(snapshot["claim_failed_blocked:total"], 1)
        self.assertEqual(snapshot["claim_failed_blocked:tier_2"], 1)

    def test_claim_dispatch_without_tier_only_increments_total(self) -> None:
        backend = _MetricsBackend()
        store = LiveEventStateStore(backend)

        store.claim_dispatch(7001, now_ms=1_000_000, lease_ms=90_000)

        snapshot = store.dispatch_metrics_snapshot()
        self.assertEqual(snapshot["claim_attempts:total"], 1)
        self.assertEqual(snapshot["claim_succeeded:total"], 1)
        # No per-tier subkey should be created when tier omitted.
        for tier in ("tier_1", "tier_2", "tier_3"):
            self.assertNotIn(f"claim_attempts:{tier}", snapshot)
            self.assertNotIn(f"claim_succeeded:{tier}", snapshot)

    def test_unknown_tier_value_does_not_create_garbage_keys(self) -> None:
        backend = _MetricsBackend()
        store = LiveEventStateStore(backend)

        store.claim_dispatch(7001, now_ms=1_000_000, lease_ms=90_000, tier="bogus")

        snapshot = store.dispatch_metrics_snapshot()
        self.assertEqual(snapshot["claim_attempts:total"], 1)
        # Unrecognised tier strings increment :total only — never a
        # per-tier subkey, otherwise we'd silently create cardinality.
        self.assertNotIn("claim_attempts:bogus", snapshot)

    def test_clear_dispatch_claim_increments_per_tier(self) -> None:
        backend = _MetricsBackend()
        store = LiveEventStateStore(backend)

        store.clear_dispatch_claim(7001, tier="tier_3")

        snapshot = store.dispatch_metrics_snapshot()
        self.assertEqual(snapshot["clear_called:total"], 1)
        self.assertEqual(snapshot["clear_called:tier_3"], 1)

    def test_clear_dispatch_claim_without_tier_increments_total_only(self) -> None:
        backend = _MetricsBackend()
        store = LiveEventStateStore(backend)

        store.clear_dispatch_claim(7001)

        snapshot = store.dispatch_metrics_snapshot()
        self.assertEqual(snapshot["clear_called:total"], 1)
        for tier in ("tier_1", "tier_2", "tier_3"):
            self.assertNotIn(f"clear_called:{tier}", snapshot)

    def test_record_published_increments_per_tier(self) -> None:
        backend = _MetricsBackend()
        store = LiveEventStateStore(backend)

        store.record_published("tier_1")
        store.record_published("tier_1")
        store.record_published("tier_2")

        snapshot = store.dispatch_metrics_snapshot()
        self.assertEqual(snapshot["published:total"], 3)
        self.assertEqual(snapshot["published:tier_1"], 2)
        self.assertEqual(snapshot["published:tier_2"], 1)
        self.assertNotIn("published:tier_3", snapshot)

    def test_started_at_ms_is_set_only_once(self) -> None:
        backend = _MetricsBackend()
        store = LiveEventStateStore(backend)

        store.record_published("tier_1")
        first = store.dispatch_metrics_snapshot()["started_at_ms"]

        store.record_published("tier_1")
        store.record_published("tier_2")
        second = store.dispatch_metrics_snapshot()["started_at_ms"]

        self.assertEqual(first, second)

    def test_metrics_persist_across_store_instances_on_same_backend(self) -> None:
        # Cumulative-not-reset contract: a planner restart that gets a
        # fresh LiveEventStateStore against the same Redis backend must
        # still see prior counters.
        backend = _MetricsBackend()
        store_a = LiveEventStateStore(backend)
        store_a.record_published("tier_1")
        store_a.record_published("tier_2")

        store_b = LiveEventStateStore(backend)
        store_b.record_published("tier_1")

        snapshot = store_b.dispatch_metrics_snapshot()
        self.assertEqual(snapshot["published:total"], 3)
        self.assertEqual(snapshot["published:tier_1"], 2)
        self.assertEqual(snapshot["published:tier_2"], 1)

    def test_metrics_hash_key_constant(self) -> None:
        # Lock the prod key name so an accidental rename is caught at
        # CI time before it silently splits cumulative history.
        self.assertEqual(LIVE_DISPATCH_METRICS_KEY, "live:dispatch_metrics")

    def test_snapshot_empty_before_any_increments(self) -> None:
        backend = _MetricsBackend()
        store = LiveEventStateStore(backend)
        self.assertEqual(store.dispatch_metrics_snapshot(), {})

    def test_backend_without_hincrby_does_not_raise(self) -> None:
        backend = _BackendWithoutMetrics()
        store = LiveEventStateStore(backend)

        # Both paths must work even though the backend cannot record
        # metrics — claim_dispatch / clear_dispatch_claim still gate the
        # live publish loop.
        self.assertTrue(store.claim_dispatch(7001, now_ms=1_000, lease_ms=90_000, tier="tier_1"))
        store.clear_dispatch_claim(7001, tier="tier_1")
        store.record_published("tier_1")

        # Snapshot is empty because no HINCRBY was applied.
        self.assertEqual(store.dispatch_metrics_snapshot(), {})


class TierActiveCountsTests(unittest.TestCase):
    def test_tier_active_counts_tallies_each_lane_by_dispatch_tier(self) -> None:
        backend = _MetricsBackend()
        store = LiveEventStateStore(backend)

        store.upsert(_make_state(7001, dispatch_tier="tier_1", next_poll_at=100), lane="hot")
        store.upsert(_make_state(7002, dispatch_tier="tier_1", next_poll_at=100), lane="hot")
        store.upsert(_make_state(7003, dispatch_tier="tier_2", next_poll_at=100), lane="hot")
        store.upsert(_make_state(7004, dispatch_tier="tier_3", next_poll_at=100), lane="warm")
        store.upsert(_make_state(7005, dispatch_tier=None, next_poll_at=100), lane="cold")

        counts = store.tier_active_counts()
        self.assertEqual(counts["tier_1"], 2)
        self.assertEqual(counts["tier_2"], 1)
        self.assertEqual(counts["tier_3"], 1)
        self.assertEqual(counts["unknown"], 1)

    def test_tier_active_counts_returns_zeroes_when_lanes_empty(self) -> None:
        backend = _MetricsBackend()
        store = LiveEventStateStore(backend)
        counts = store.tier_active_counts()
        self.assertEqual(counts, {"tier_1": 0, "tier_2": 0, "tier_3": 0, "unknown": 0})


def _make_state(event_id: int, *, dispatch_tier: str | None, next_poll_at: int) -> LiveEventState:
    return LiveEventState(
        event_id=event_id,
        sport_slug="football",
        status_type="inprogress",
        poll_profile="hot",
        last_seen_at=1,
        last_ingested_at=1,
        last_changed_at=1,
        next_poll_at=next_poll_at,
        hot_until=next_poll_at,
        home_score=0,
        away_score=0,
        version_hint=None,
        is_finalized=False,
        dispatch_tier=dispatch_tier,
    )


if __name__ == "__main__":
    unittest.main()
