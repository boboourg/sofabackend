"""F-7 Phase 0: /ops/queues/summary surface tests.

Verifies that collect_queue_summary surfaces the new dispatch metrics +
per-tier counts when the live state store provides them, and gracefully
degrades to empty dicts when it does not (older test fakes / dev paths).
"""
from __future__ import annotations

import unittest

from schema_inspector.ops.queue_summary import collect_queue_summary
from schema_inspector.queue.live_state import LiveEventState, LiveEventStateStore


class _SummaryBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.claims: dict[str, object] = {}

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

    def set(self, key, value, *, nx=None, px=None):
        del px
        if nx and key in self.claims:
            return False
        self.claims[key] = value
        return True

    def delete(self, key):
        return self.claims.pop(key, None) is not None


class QueueSummaryDispatchMetricsTests(unittest.IsolatedAsyncioTestCase):
    async def test_summary_exposes_dispatch_metrics_and_tier_counts(self) -> None:
        backend = _SummaryBackend()
        store = LiveEventStateStore(backend)

        # Seed two tier_1 events in hot lane and one tier_3 in warm.
        store.upsert(_make_state(7011, dispatch_tier="tier_1", next_poll_at=100), lane="hot")
        store.upsert(_make_state(7012, dispatch_tier="tier_1", next_poll_at=100), lane="hot")
        store.upsert(_make_state(7013, dispatch_tier="tier_3", next_poll_at=200), lane="warm")

        # Simulate a few publish cycles via the metrics-aware methods.
        store.record_published("tier_1")
        store.record_published("tier_1")
        store.record_published("tier_3")
        store.claim_dispatch(7011, now_ms=100, lease_ms=90_000, tier="tier_1")
        store.claim_dispatch(7011, now_ms=101, lease_ms=90_000, tier="tier_1")  # blocked
        store.clear_dispatch_claim(7011, tier="tier_1")

        summary = await collect_queue_summary(
            stream_queue=None,
            live_state_store=store,
            redis_backend=None,
        )

        self.assertEqual(summary.live_lanes["hot"], 2)
        self.assertEqual(summary.live_lanes["warm"], 1)
        self.assertEqual(summary.live_lanes["cold"], 0)

        self.assertEqual(summary.live_tier_counts["tier_1"], 2)
        self.assertEqual(summary.live_tier_counts["tier_2"], 0)
        self.assertEqual(summary.live_tier_counts["tier_3"], 1)
        self.assertEqual(summary.live_tier_counts["unknown"], 0)

        metrics = summary.live_dispatch_metrics
        self.assertEqual(metrics["claim_attempts:total"], 2)
        self.assertEqual(metrics["claim_attempts:tier_1"], 2)
        self.assertEqual(metrics["claim_succeeded:total"], 1)
        self.assertEqual(metrics["claim_succeeded:tier_1"], 1)
        self.assertEqual(metrics["claim_failed_blocked:total"], 1)
        self.assertEqual(metrics["claim_failed_blocked:tier_1"], 1)
        self.assertEqual(metrics["clear_called:total"], 1)
        self.assertEqual(metrics["clear_called:tier_1"], 1)
        self.assertEqual(metrics["published:total"], 3)
        self.assertEqual(metrics["published:tier_1"], 2)
        self.assertEqual(metrics["published:tier_3"], 1)
        self.assertIn("started_at_ms", metrics)

    async def test_summary_gracefully_handles_store_without_metrics_methods(self) -> None:
        # Older fakes / live state stores without dispatch_metrics_snapshot
        # or tier_active_counts must not break the /ops payload.
        class _MinimalStore:
            backend = _SummaryBackend()

            def __init__(self) -> None:
                pass

            def _lane_key(self, lane: str) -> str:
                return f"zset:live:{lane}"

        summary = await collect_queue_summary(
            stream_queue=None,
            live_state_store=_MinimalStore(),
            redis_backend=None,
        )
        self.assertEqual(summary.live_dispatch_metrics, {})
        self.assertEqual(summary.live_tier_counts, {})

    async def test_summary_handles_store_none(self) -> None:
        summary = await collect_queue_summary(
            stream_queue=None,
            live_state_store=None,
            redis_backend=None,
        )
        self.assertEqual(summary.live_dispatch_metrics, {})
        self.assertEqual(summary.live_tier_counts, {})
        self.assertEqual(summary.live_lanes, {"hot": 0, "warm": 0, "cold": 0})


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
