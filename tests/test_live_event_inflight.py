from __future__ import annotations

import unittest


class LiveEventInFlightStoreTests(unittest.TestCase):
    def test_claim_blocks_duplicate_until_release(self) -> None:
        from schema_inspector.queue.live_inflight import LiveEventInFlightStore

        backend = _FakeRedisBackend()
        store = LiveEventInFlightStore(backend, ttl_ms=60_000)

        self.assertTrue(store.claim(event_id=42, owner="worker-a"))
        self.assertFalse(store.claim(event_id=42, owner="worker-b"))

        store.release(event_id=42, owner="worker-a")

        self.assertTrue(store.claim(event_id=42, owner="worker-b"))

    def test_release_does_not_delete_another_owner(self) -> None:
        from schema_inspector.queue.live_inflight import LiveEventInFlightStore

        backend = _FakeRedisBackend()
        store = LiveEventInFlightStore(backend, ttl_ms=60_000)

        self.assertTrue(store.claim(event_id=42, owner="worker-a"))
        store.release(event_id=42, owner="worker-b")

        self.assertFalse(store.claim(event_id=42, owner="worker-c"))


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    def set(self, key: str, value: str, *, nx: bool = False, px: int | None = None) -> bool:
        del px
        if nx and key in self.values:
            return False
        self.values[key] = str(value)
        return True

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def delete(self, key: str) -> int:
        if key not in self.values:
            return 0
        del self.values[key]
        return 1


if __name__ == "__main__":
    unittest.main()
