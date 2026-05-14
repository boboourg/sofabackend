from __future__ import annotations

import asyncio
import unittest
from types import SimpleNamespace

from schema_inspector.queue.streams import StreamEntry
from schema_inspector.services.service_app import DelayedEnvelopeStore, ServiceApp


class DelayedEnvelopeStoreTests(unittest.TestCase):
    def test_save_entry_increments_attempt_for_exponential_backoff(self) -> None:
        backend = _FakeRedisBackend()
        store = DelayedEnvelopeStore(backend)

        store.save_entry(
            StreamEntry(
                stream="stream:etl:test",
                message_id="1-1",
                values={
                    "job_id": "job-1",
                    "job_type": "discover_sport_surface",
                    "attempt": "2",
                },
            )
        )

        job = store.load("job-1")

        self.assertIsNotNone(job)
        self.assertEqual(job.attempt, 3)


class ServiceAppTests(unittest.TestCase):
    def test_build_live_worker_wires_shared_inflight_store(self) -> None:
        app = SimpleNamespace(
            redis_backend=_FakeRedisBackend(),
            stream_queue=_FakeStreamQueue(),
            live_state_store=None,
            database=None,
        )
        service_app = ServiceApp(app)

        worker = service_app.build_live_worker(lane="tier_1", consumer_name="worker-live-tier-1-1")

        self.assertIs(worker.in_flight_store, service_app.live_event_inflight_store)


class LiveStateSweepWiringTests(unittest.TestCase):
    """Pin the contract for ``_build_live_state_sweep_wiring``.

    2026-05-14 regression: the callback closure referenced ``self.database``
    where ``self`` is a ``ServiceApp`` (database lives on ``self.app.database``).
    Prod planner logged ``AttributeError("'ServiceApp' object has no
    attribute 'database'")`` once per minute for 20+ minutes — the sweeper
    never ran, ``oldest_hot_score_age_seconds`` ballooned to >900s.

    The original P0.B test suite ``test_live_state_sweeper.py`` only
    exercises the ``LiveStateSweeper`` class and the PlannerDaemon tick
    callback contract — neither knows what wiring ServiceApp performs.
    This test fills that gap by constructing a real ServiceApp with a
    minimal-but-realistic fake ``HybridApp`` and invoking the wired
    callback end-to-end.
    """

    def test_callback_acquires_connection_from_app_database(self) -> None:
        fake_db = _FakeDatabase()
        live_state_store = _FakeLiveStateStore()
        app = SimpleNamespace(
            redis_backend=_FakeRedisBackend(),
            stream_queue=_FakeStreamQueue(),
            live_state_store=live_state_store,
            database=fake_db,
        )
        service_app = ServiceApp(app)

        callback, interval_ms = service_app._build_live_state_sweep_wiring()

        self.assertIsNotNone(callback, "wiring must return a callable on default env")
        self.assertGreater(interval_ms, 0)
        # The critical assertion: invoking the callback must NOT raise
        # AttributeError (the prod regression). It is OK if the fake
        # database returns no rows — we only care that the callback can
        # acquire a connection via the app.database pathway.
        asyncio.run(callback(now_ms=1_700_000_000_000))
        self.assertTrue(
            fake_db.connection_calls >= 1,
            "callback must reach app.database.connection()",
        )

    def test_callback_disabled_when_interval_zero(self) -> None:
        # Sentinel: env opt-out path must short-circuit before invoking
        # any database-dependent wiring.
        import os
        previous = os.environ.get("SOFASCORE_LIVE_STATE_SWEEP_INTERVAL_MS")
        os.environ["SOFASCORE_LIVE_STATE_SWEEP_INTERVAL_MS"] = "0"
        try:
            app = SimpleNamespace(
                redis_backend=_FakeRedisBackend(),
                stream_queue=_FakeStreamQueue(),
                live_state_store=_FakeLiveStateStore(),
                database=_FakeDatabase(),
            )
            service_app = ServiceApp(app)
            callback, interval_ms = service_app._build_live_state_sweep_wiring()
            self.assertIsNone(callback)
            self.assertEqual(interval_ms, 0)
        finally:
            if previous is None:
                os.environ.pop("SOFASCORE_LIVE_STATE_SWEEP_INTERVAL_MS", None)
            else:
                os.environ["SOFASCORE_LIVE_STATE_SWEEP_INTERVAL_MS"] = previous


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(key, {}).update(dict(mapping))
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))

    def hdel(self, key: str, field: str) -> int:
        bucket = self.hashes.setdefault(key, {})
        existed = field in bucket
        bucket.pop(field, None)
        return 1 if existed else 0


class _FakeStreamQueue:
    def __init__(self) -> None:
        self.groups: list[tuple[str, str]] = []

    def ensure_group(self, stream: str, group: str) -> None:
        self.groups.append((stream, group))


class _FakeConnection:
    async def fetch(self, query: str, *args):
        del query, args
        return []


class _FakeConnectionContext:
    def __init__(self, connection: _FakeConnection) -> None:
        self._connection = connection

    async def __aenter__(self) -> _FakeConnection:
        return self._connection

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        return None


class _FakeDatabase:
    def __init__(self) -> None:
        self.connection_calls = 0

    def connection(self) -> _FakeConnectionContext:
        self.connection_calls += 1
        return _FakeConnectionContext(_FakeConnection())


class _FakeLiveStateStore:
    """Minimal live-state-store shape needed by LiveStateSweeper wiring."""

    def __init__(self) -> None:
        self.backend = _FakeLiveStateBackend()
        self.hot_zset_key = "zset:live:hot"
        self.warm_zset_key = "zset:live:warm"
        self.cold_zset_key = "zset:live:cold"


class _FakeLiveStateBackend:
    def zrange(self, key: str, start: int, end: int, withscores: bool = False):
        del key, start, end, withscores
        return []

    def zrem(self, key: str, member: str) -> int:
        del key, member
        return 0


if __name__ == "__main__":
    unittest.main()
