from __future__ import annotations

import unittest

from schema_inspector.live_bootstrap import LiveBootstrapCoordinator


class LiveBootstrapCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    async def test_postgres_true_populates_redis_hot_cache(self) -> None:
        sql = _FakeSqlExecutor({42: True})
        redis = _FakeRedis()
        coordinator = LiveBootstrapCoordinator(redis_backend=redis, worker_id="w1")

        self.assertTrue(await coordinator.is_bootstrapped(sql, event_id=42))
        self.assertEqual(redis.values["live:bootstrap_done:42"][0], "1")

    async def test_redis_hot_cache_short_circuits_postgres(self) -> None:
        sql = _FakeSqlExecutor({42: False})
        redis = _FakeRedis()
        redis.set("live:bootstrap_done:42", "1")
        coordinator = LiveBootstrapCoordinator(redis_backend=redis, worker_id="w1")

        self.assertTrue(await coordinator.is_bootstrapped(sql, event_id=42))
        self.assertEqual(sql.fetches, 0)

    async def test_single_flight_lock_allows_one_worker(self) -> None:
        redis = _FakeRedis()
        first = LiveBootstrapCoordinator(redis_backend=redis, worker_id="w1")
        second = LiveBootstrapCoordinator(redis_backend=redis, worker_id="w2")

        self.assertTrue(first.acquire_hydrate_lock(event_id=42))
        self.assertFalse(second.acquire_hydrate_lock(event_id=42))

    async def test_mark_and_reset_update_postgres_and_cache(self) -> None:
        sql = _FakeSqlExecutor({42: False})
        redis = _FakeRedis()
        coordinator = LiveBootstrapCoordinator(redis_backend=redis, worker_id="w1")

        await coordinator.mark_bootstrapped(sql, event_id=42)
        self.assertTrue(sql.bootstrap[42])
        self.assertIn("live:bootstrap_done:42", redis.values)

        await coordinator.reset_bootstrap(sql, event_id=42)
        self.assertFalse(sql.bootstrap[42])
        self.assertNotIn("live:bootstrap_done:42", redis.values)

    async def test_no_redis_backend_is_safe(self) -> None:
        sql = _FakeSqlExecutor({42: True})
        coordinator = LiveBootstrapCoordinator(redis_backend=None, worker_id="w1")

        self.assertTrue(await coordinator.is_bootstrapped(sql, event_id=42))
        self.assertFalse(coordinator.acquire_hydrate_lock(event_id=42))

    def test_release_hydrate_lock_after_acquire_clears_redis(self) -> None:
        """Task 6 (2026-05-15): explicit release must delete the
        Redis key so the next attempt acquires immediately."""

        redis = _FakeRedis()
        coordinator = LiveBootstrapCoordinator(redis_backend=redis, worker_id="w1")

        self.assertTrue(coordinator.acquire_hydrate_lock(event_id=42))
        self.assertIn("live:hydrate_lock:42", redis.values)
        self.assertTrue(coordinator.release_hydrate_lock(event_id=42))
        self.assertNotIn("live:hydrate_lock:42", redis.values)

    def test_release_hydrate_lock_after_release_enables_next_acquire(self) -> None:
        """End-to-end: w1 acquire + release, w2 acquire — no contention."""

        redis = _FakeRedis()
        w1 = LiveBootstrapCoordinator(redis_backend=redis, worker_id="w1")
        w2 = LiveBootstrapCoordinator(redis_backend=redis, worker_id="w2")

        self.assertTrue(w1.acquire_hydrate_lock(event_id=42))
        self.assertFalse(w2.acquire_hydrate_lock(event_id=42))
        w1.release_hydrate_lock(event_id=42)
        # After w1 released, w2 should now claim cleanly.
        self.assertTrue(w2.acquire_hydrate_lock(event_id=42))

    def test_release_hydrate_lock_owner_mismatch_keeps_lock(self) -> None:
        """Defensive: never release a lock owned by a different worker
        (avoids accidentally clearing a lock taken after our TTL expired
        mid-fanout)."""

        redis = _FakeRedis()
        w1 = LiveBootstrapCoordinator(redis_backend=redis, worker_id="w1")
        w2 = LiveBootstrapCoordinator(redis_backend=redis, worker_id="w2")

        self.assertTrue(w1.acquire_hydrate_lock(event_id=42))
        # w2 tries to release w1's lock — must be a no-op.
        self.assertFalse(w2.release_hydrate_lock(event_id=42))
        self.assertIn("live:hydrate_lock:42", redis.values)

    def test_release_hydrate_lock_idempotent_after_expiry(self) -> None:
        """Release when key absent (already expired) is a clean no-op."""

        redis = _FakeRedis()
        coordinator = LiveBootstrapCoordinator(redis_backend=redis, worker_id="w1")

        self.assertFalse(coordinator.release_hydrate_lock(event_id=42))
        # Should not raise on the second call either.
        self.assertFalse(coordinator.release_hydrate_lock(event_id=42))

    def test_release_hydrate_lock_no_redis_is_safe(self) -> None:
        """No backend wired -> release silently returns False."""

        coordinator = LiveBootstrapCoordinator(redis_backend=None, worker_id="w1")
        self.assertFalse(coordinator.release_hydrate_lock(event_id=42))


class _FakeSqlExecutor:
    def __init__(self, bootstrap: dict[int, bool]) -> None:
        self.bootstrap = dict(bootstrap)
        self.fetches = 0

    async def fetchval(self, query: str, event_id: int):
        self.fetches += 1
        assert "live_bootstrap_done_at IS NOT NULL" in query
        return self.bootstrap.get(int(event_id), False)

    async def execute(self, query: str, event_id: int):
        if "live_bootstrap_done_at = now()" in query:
            self.bootstrap[int(event_id)] = True
        elif "live_bootstrap_done_at = NULL" in query:
            self.bootstrap[int(event_id)] = False
        return "UPDATE 1"


class _FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, tuple[object, dict[str, object]]] = {}

    def get(self, key: str):
        value = self.values.get(str(key))
        return None if value is None else value[0]

    def set(self, key: str, value: object, **kwargs) -> bool:
        key = str(key)
        if kwargs.get("nx") and key in self.values:
            return False
        self.values[key] = (value, dict(kwargs))
        return True

    def delete(self, key: str) -> int:
        return 1 if self.values.pop(str(key), None) is not None else 0


if __name__ == "__main__":
    unittest.main()
