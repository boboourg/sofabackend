from __future__ import annotations

import unittest

from schema_inspector.queue.proxy_state import ProxyStateStore


class _FakeProxyBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}
        self.zsets: dict[str, dict[str, float]] = {}

    def hset(self, key: str, mapping: dict[str, object]) -> None:
        self.hashes.setdefault(key, {}).update(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))

    def zadd(self, key: str, mapping: dict[str, float]) -> None:
        self.zsets.setdefault(key, {}).update(mapping)

    def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    def zrangebyscore(self, key: str, min_score: float, max_score: float) -> list[str]:
        return [
            member
            for member, score in sorted(self.zsets.get(key, {}).items(), key=lambda item: (item[1], item[0]))
            if min_score <= score <= max_score
        ]


class QueueProxyStateTests(unittest.TestCase):
    def test_proxy_state_enters_and_leaves_cooldown(self) -> None:
        store = ProxyStateStore(_FakeProxyBackend())

        failed = store.record_failure(
            "proxy_1",
            status_code=403,
            challenge_reason="access_denied",
            observed_at_ms=1_000,
            cooldown_ms=30_000,
        )
        self.assertEqual(failed.status, "cooldown")
        self.assertEqual(failed.cooldown_until, 31_000)
        self.assertFalse(store.is_available("proxy_1", now_ms=5_000))

        recovered = store.record_success("proxy_1", observed_at_ms=31_001, latency_ms=125)
        self.assertEqual(recovered.status, "available")
        self.assertTrue(store.is_available("proxy_1", now_ms=31_001))
        self.assertEqual(recovered.recent_successes, 1)


if __name__ == "__main__":
    unittest.main()
