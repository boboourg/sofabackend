"""Tests for monitoring/dedupe.py — RedisDedupeStore + NullDedupeStore.

Pins the contract:
- First alert for a (signal, severity) pair → should_send=True
- Repeat alert within TTL → should_send=False
- Repeat alert after TTL → should_send=True
- mark_sent stores last_sent + first_sent + count, refreshes TTL
- record_resolved clears all dedupe state for the signal
- Redis errors do not crash: fall back to should_send=True (fail-open)
- get_repeat_count returns increment from mark_sent calls
- NullDedupeStore always sends, never marks
"""

from __future__ import annotations

import unittest
from typing import Any

from schema_inspector.monitoring.dedupe import (
    NullDedupeStore,
    RedisDedupeStore,
)


class _StubRedis:
    """Hash-backed stub Redis. Supports hget, hset, hdel, hincrby, expire."""

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.ttl_calls: list[tuple[str, int]] = []
        self.raise_on_hget = False
        self.raise_on_hset = False

    def hget(self, key: str, field: str) -> str | None:
        if self.raise_on_hget:
            raise RuntimeError("forced hget error")
        return self.hashes.get(key, {}).get(field)

    def hset(self, key: str, field: str, value: Any) -> int:
        if self.raise_on_hset:
            raise RuntimeError("forced hset error")
        bucket = self.hashes.setdefault(key, {})
        existed = field in bucket
        bucket[field] = str(value)
        return 0 if existed else 1

    def hdel(self, key: str, *fields: str) -> int:
        bucket = self.hashes.get(key, {})
        removed = 0
        for field in fields:
            if field in bucket:
                del bucket[field]
                removed += 1
        return removed

    def hincrby(self, key: str, field: str, increment: int) -> int:
        bucket = self.hashes.setdefault(key, {})
        current = int(bucket.get(field, "0") or 0)
        current += int(increment)
        bucket[field] = str(current)
        return current

    def expire(self, key: str, seconds: int) -> int:
        self.ttl_calls.append((key, int(seconds)))
        return 1


class RedisDedupeStoreTests(unittest.TestCase):
    def _make(self, clock_value: float = 1_000_000.0) -> RedisDedupeStore:
        backend = _StubRedis()
        return RedisDedupeStore(backend, clock=lambda: clock_value)

    def test_first_alert_should_send(self) -> None:
        store = self._make()
        self.assertTrue(store.should_send("sig", "CRIT", ttl_seconds=300))

    def test_repeat_within_ttl_suppressed(self) -> None:
        backend = _StubRedis()
        now = [1000.0]
        store = RedisDedupeStore(backend, clock=lambda: now[0])
        store.mark_sent("sig", "CRIT", ttl_seconds=300)
        # 60 seconds later — still within 300s TTL
        now[0] = 1060.0
        self.assertFalse(store.should_send("sig", "CRIT", ttl_seconds=300))

    def test_repeat_after_ttl_should_send(self) -> None:
        backend = _StubRedis()
        now = [1000.0]
        store = RedisDedupeStore(backend, clock=lambda: now[0])
        store.mark_sent("sig", "CRIT", ttl_seconds=300)
        now[0] = 1301.0  # 301 seconds later
        self.assertTrue(store.should_send("sig", "CRIT", ttl_seconds=300))

    def test_mark_sent_stores_first_sent_once(self) -> None:
        backend = _StubRedis()
        now = [1000.0]
        store = RedisDedupeStore(backend, clock=lambda: now[0])

        store.mark_sent("sig", "CRIT", ttl_seconds=300)
        first_initial = store.get_first_alerted_at_epoch("sig", "CRIT")
        self.assertEqual(first_initial, 1000.0)

        now[0] = 2000.0
        store.mark_sent("sig", "CRIT", ttl_seconds=300)
        first_after_second_mark = store.get_first_alerted_at_epoch("sig", "CRIT")
        # first_sent does NOT advance — it captures the start of the breach
        self.assertEqual(first_after_second_mark, 1000.0)

    def test_mark_sent_increments_count(self) -> None:
        store = self._make()
        store.mark_sent("sig", "CRIT", ttl_seconds=300)
        store.mark_sent("sig", "CRIT", ttl_seconds=300)
        store.mark_sent("sig", "CRIT", ttl_seconds=300)
        self.assertEqual(store.get_repeat_count("sig", "CRIT"), 3)

    def test_mark_sent_refreshes_sliding_ttl(self) -> None:
        backend = _StubRedis()
        store = RedisDedupeStore(backend, clock=lambda: 1000.0)
        store.mark_sent("sig", "CRIT", ttl_seconds=300)
        self.assertTrue(any(call[0] == "monitoring:dedupe" for call in backend.ttl_calls))

    def test_record_resolved_clears_both_severities(self) -> None:
        store = self._make()
        store.mark_sent("sig", "WARN", ttl_seconds=600)
        store.mark_sent("sig", "CRIT", ttl_seconds=1800)
        self.assertGreater(store.get_repeat_count("sig", "WARN"), 0)
        self.assertGreater(store.get_repeat_count("sig", "CRIT"), 0)

        store.record_resolved("sig")
        # Counts reset to 0 (HDEL removes the count field).
        self.assertEqual(store.get_repeat_count("sig", "WARN"), 0)
        self.assertEqual(store.get_repeat_count("sig", "CRIT"), 0)

    def test_should_send_fail_open_on_redis_error(self) -> None:
        backend = _StubRedis()
        backend.raise_on_hget = True
        store = RedisDedupeStore(backend, clock=lambda: 1000.0)
        # Redis broken → don't suppress alerts (fail-open).
        self.assertTrue(store.should_send("sig", "CRIT", ttl_seconds=300))

    def test_mark_sent_swallows_redis_error(self) -> None:
        backend = _StubRedis()
        backend.raise_on_hset = True
        store = RedisDedupeStore(backend, clock=lambda: 1000.0)
        # Should not raise — sink will retry next tick.
        store.mark_sent("sig", "CRIT", ttl_seconds=300)

    def test_warn_and_crit_dedupe_independently(self) -> None:
        backend = _StubRedis()
        now = [1000.0]
        store = RedisDedupeStore(backend, clock=lambda: now[0])
        store.mark_sent("sig", "WARN", ttl_seconds=600)
        # CRIT was never marked, so should_send for CRIT is True.
        self.assertTrue(store.should_send("sig", "CRIT", ttl_seconds=1800))
        # But WARN is still within TTL.
        self.assertFalse(store.should_send("sig", "WARN", ttl_seconds=600))


class NullDedupeStoreTests(unittest.TestCase):
    def test_always_should_send(self) -> None:
        store = NullDedupeStore()
        self.assertTrue(store.should_send("x", "CRIT", 300))
        store.mark_sent("x", "CRIT", 300)
        self.assertTrue(store.should_send("x", "CRIT", 300))

    def test_repeat_count_is_zero(self) -> None:
        store = NullDedupeStore()
        store.mark_sent("x", "CRIT", 300)
        self.assertEqual(store.get_repeat_count("x", "CRIT"), 0)

    def test_first_alerted_at_is_none(self) -> None:
        store = NullDedupeStore()
        store.mark_sent("x", "CRIT", 300)
        self.assertIsNone(store.get_first_alerted_at_epoch("x", "CRIT"))


if __name__ == "__main__":
    unittest.main()
