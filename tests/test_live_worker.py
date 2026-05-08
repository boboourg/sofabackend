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

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))

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

    def test_track_event_assigns_tier_1_dispatch_for_top_live_football(self) -> None:
        store = LiveEventStateStore(_FakeLiveBackend())
        worker = LiveWorker(now_ms_factory=lambda: 1_800_000_000_000)

        result = worker.track_event(
            sport_slug="football",
            event_id=15235532,
            status_type="inprogress",
            minutes_to_start=0,
            trace_id="trace-2",
            detail_id=1,
            tournament_user_count=317795,
            live_state_store=store,
            stream_queue=_FakeStreamQueue(),
        )

        self.assertEqual(result.decision.lane, "hot")
        self.assertEqual(result.stream, "stream:etl:live_tier_1")
        self.assertEqual(result.next_poll_at, 1_800_000_005_000)
        self.assertEqual(store.fetch(15235532).dispatch_tier, "tier_1")

    def test_track_event_clears_dispatch_claim_with_tier(self) -> None:
        # F-7 Phase 0: clear_dispatch_claim must receive the resolved
        # dispatch_tier so the per-tier clear counter reflects which
        # stream actually drained a job (asymmetry between claim/clear
        # rates per tier is the diagnostic signal).
        store = _ClearRecordingStore()
        worker = LiveWorker(now_ms_factory=lambda: 1_800_000_000_000)

        worker.track_event(
            sport_slug="football",
            event_id=15235532,
            status_type="inprogress",
            minutes_to_start=0,
            trace_id="trace-clear",
            detail_id=1,
            tournament_user_count=317795,
            live_state_store=store,
            stream_queue=_FakeStreamQueue(),
        )

        self.assertEqual(store.clear_calls, [(15235532, "tier_1")])


class _ClearRecordingStore:
    """Live state store fake that records every clear_dispatch_claim call
    so we can assert the tier kwarg threading."""

    def __init__(self) -> None:
        self.upserts: list[object] = []
        self.clear_calls: list[tuple[int, str | None]] = []
        self.backend = _FakeLiveBackend()
        self.hot_zset_key = "zset:live:hot"
        self.warm_zset_key = "zset:live:warm"
        self.cold_zset_key = "zset:live:cold"

    def upsert(self, state, *, lane: str | None = None) -> None:
        del lane
        self.upserts.append(state)

    def clear_dispatch_claim(self, event_id: int, *, tier: str | None = None) -> None:
        self.clear_calls.append((int(event_id), tier))


if __name__ == "__main__":
    unittest.main()
