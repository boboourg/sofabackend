from __future__ import annotations

import unittest

from schema_inspector.queue.leases import LeaseManager


class _FakeLeaseBackend:
    def __init__(self) -> None:
        self.values: dict[str, tuple[str, int | None]] = {}

    def set(self, key: str, value: str, *, nx: bool = False, px: int | None = None, now_ms: int = 0) -> bool:
        current = self.values.get(key)
        if current is not None and current[1] is not None and current[1] <= now_ms:
            del self.values[key]
            current = None
        if nx and current is not None:
            return False
        expire_at = None if px is None else now_ms + px
        self.values[key] = (value, expire_at)
        return True

    def get(self, key: str, *, now_ms: int = 0) -> str | None:
        current = self.values.get(key)
        if current is None:
            return None
        value, expire_at = current
        if expire_at is not None and expire_at <= now_ms:
            del self.values[key]
            return None
        return value

    def pexpire(self, key: str, px: int, *, now_ms: int = 0) -> bool:
        current = self.values.get(key)
        if current is None:
            return False
        value, expire_at = current
        if expire_at is not None and expire_at <= now_ms:
            del self.values[key]
            return False
        self.values[key] = (value, now_ms + px)
        return True

    def delete(self, key: str) -> int:
        if key in self.values:
            del self.values[key]
            return 1
        return 0


class QueueLeaseTests(unittest.TestCase):
    def test_lease_acquire_renew_release_cycle(self) -> None:
        manager = LeaseManager(_FakeLeaseBackend())

        lease = manager.acquire("lock:job:alpha", "token-a", ttl_ms=1000, now_ms=0)
        conflict = manager.acquire("lock:job:alpha", "token-b", ttl_ms=1000, now_ms=10)
        renewed = manager.renew("lock:job:alpha", "token-a", ttl_ms=1500, now_ms=500)
        released = manager.release("lock:job:alpha", "token-a", now_ms=600)
        reacquired = manager.acquire("lock:job:alpha", "token-b", ttl_ms=1000, now_ms=601)

        self.assertIsNotNone(lease)
        self.assertEqual(lease.token, "token-a")
        self.assertIsNone(conflict)
        self.assertTrue(renewed)
        self.assertTrue(released)
        self.assertIsNotNone(reacquired)
        self.assertEqual(reacquired.token, "token-b")


if __name__ == "__main__":
    unittest.main()
