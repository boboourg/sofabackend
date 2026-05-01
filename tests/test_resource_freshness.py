from __future__ import annotations

import unittest

from schema_inspector.queue.freshness import FreshnessStore


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.set_calls: list[dict[str, object]] = []

    def exists(self, key: str, *, now_ms: int | None = None) -> bool:
        del now_ms
        return key in self.values

    def set(self, key: str, value: str, *, px: int | None = None, now_ms: int | None = None) -> bool:
        del now_ms
        self.values[key] = value
        self.set_calls.append({"key": key, "value": value, "px": px})
        return True


class _FailingExistsBackend(_FakeRedisBackend):
    def exists(self, key: str, *, now_ms: int | None = None) -> bool:
        del key, now_ms
        raise RuntimeError("redis unavailable")


class FreshnessStoreTests(unittest.TestCase):
    def test_is_fresh_returns_true_when_key_exists(self) -> None:
        backend = _FakeRedisBackend()
        backend.values["freshness:player:10"] = "1"
        store = FreshnessStore(backend, log_interval_seconds=300)

        self.assertTrue(store.is_fresh("freshness:player:10"))

    def test_is_fresh_returns_false_when_key_missing(self) -> None:
        store = FreshnessStore(_FakeRedisBackend(), log_interval_seconds=300)

        self.assertFalse(store.is_fresh("freshness:player:10"))

    def test_mark_fetched_sets_key_with_ttl_ms(self) -> None:
        backend = _FakeRedisBackend()
        store = FreshnessStore(backend, log_interval_seconds=300)

        store.mark_fetched("freshness:player:10", ttl_seconds=86_400)

        self.assertEqual(backend.values["freshness:player:10"], "1")
        self.assertEqual(
            backend.set_calls[-1],
            {"key": "freshness:player:10", "value": "1", "px": 86_400_000},
        )

    def test_is_fresh_returns_false_on_redis_exception(self) -> None:
        store = FreshnessStore(_FailingExistsBackend(), log_interval_seconds=300)

        self.assertFalse(store.is_fresh("freshness:player:10"))

    def test_hits_misses_and_marks_counters_increment(self) -> None:
        backend = _FakeRedisBackend()
        backend.values["freshness:player:10"] = "1"
        store = FreshnessStore(backend, log_interval_seconds=300)

        store.is_fresh("freshness:player:10")
        store.is_fresh("freshness:player:11")
        store.mark_fetched("freshness:player:12", ttl_seconds=86_400)

        self.assertEqual(
            store.snapshot_stats(),
            {"hits": 1, "misses": 1, "marks": 1},
        )


if __name__ == "__main__":
    unittest.main()
