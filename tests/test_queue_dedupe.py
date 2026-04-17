from __future__ import annotations

import unittest

from schema_inspector.queue.dedupe import DedupeStore


class _FakeDedupeBackend:
    def __init__(self) -> None:
        self.values: dict[str, int | None] = {}

    def set(self, key: str, value: str, *, nx: bool = False, px: int | None = None, now_ms: int = 0) -> bool:
        current_expire = self.values.get(key)
        if current_expire is not None and current_expire <= now_ms:
            del self.values[key]
            current_expire = None
        if nx and key in self.values:
            return False
        self.values[key] = None if px is None else now_ms + px
        return True

    def exists(self, key: str, *, now_ms: int = 0) -> bool:
        current_expire = self.values.get(key)
        if current_expire is None:
            return key in self.values
        if current_expire <= now_ms:
            del self.values[key]
            return False
        return True


class QueueDedupeTests(unittest.TestCase):
    def test_dedupe_store_claims_job_once_per_ttl_window(self) -> None:
        store = DedupeStore(_FakeDedupeBackend())

        first = store.claim_job("dedupe:job:discover:1", ttl_ms=1000, now_ms=0)
        second = store.claim_job("dedupe:job:discover:1", ttl_ms=1000, now_ms=200)
        third = store.claim_job("dedupe:job:discover:1", ttl_ms=1000, now_ms=1200)

        self.assertTrue(first)
        self.assertFalse(second)
        self.assertTrue(third)

    def test_dedupe_store_tracks_fresh_resource_windows(self) -> None:
        store = DedupeStore(_FakeDedupeBackend())

        store.mark_fresh("fresh:event:1:statistics", ttl_ms=500, now_ms=100)

        self.assertTrue(store.is_fresh("fresh:event:1:statistics", now_ms=200))
        self.assertFalse(store.is_fresh("fresh:event:1:statistics", now_ms=700))


if __name__ == "__main__":
    unittest.main()
