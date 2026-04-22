from __future__ import annotations

import unittest

from schema_inspector.queue.live_state import LIVE_WARM_ZSET, LiveEventStateStore
from schema_inspector.workers.live_worker import LiveWorker


class _FakeLiveBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.claims: dict[str, object] = {}

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(key, {}).update(mapping)
        return 1

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[str(member)] = float(score)
        return 1

    def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                removed += 1
                del bucket[member]
        return removed

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


class _FakeStreamQueue:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, object]]] = []

    def publish(self, stream: str, payload: dict[str, object]) -> None:
        self.published.append((stream, dict(payload)))


class LiveWorkerTests(unittest.TestCase):
    def test_track_event_updates_state_without_immediate_stream_publish(self) -> None:
        backend = _FakeLiveBackend()
        queue = _FakeStreamQueue()
        store = LiveEventStateStore(backend)
        worker = LiveWorker(now_ms_factory=lambda: 1_800_000_000_000)
        self.assertTrue(
            store.claim_dispatch(
                15400165,
                now_ms=1_800_000_000_000,
                lease_ms=90_000,
            )
        )

        result = worker.track_event(
            sport_slug="football",
            event_id=15400165,
            status_type="scheduled",
            minutes_to_start=20,
            trace_id="trace-1",
            live_state_store=store,
            stream_queue=queue,
        )

        self.assertEqual(result.decision.lane, "warm")
        self.assertIsNone(result.job)
        self.assertEqual(result.next_poll_at, 1_800_000_600_000)
        self.assertEqual(backend.hashes["live:event:15400165"]["poll_profile"], "warm")
        self.assertEqual(backend.zsets[LIVE_WARM_ZSET]["15400165"], float(1_800_000_600_000))
        self.assertEqual(queue.published, [])
        self.assertEqual(backend.claims, {})


if __name__ == "__main__":
    unittest.main()
