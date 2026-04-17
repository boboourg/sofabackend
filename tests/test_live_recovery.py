from __future__ import annotations

import unittest

from schema_inspector.storage.live_state_repository import EventLiveStateHistoryRecord


class LiveRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_rebuild_uses_latest_state_per_event(self) -> None:
        from schema_inspector.ops.recovery import rebuild_live_state_from_postgres
        from schema_inspector.queue.live_state import LiveEventStateStore

        backend = _FakeRedisBackend()
        store = LiveEventStateStore(backend)
        repository = _FakeHistoryRepository(
            rows=(
                EventLiveStateHistoryRecord(
                    event_id=501,
                    observed_status_type="scheduled",
                    poll_profile="warm",
                    home_score=None,
                    away_score=None,
                    period_label=None,
                    observed_at="2026-04-17T10:00:00+00:00",
                ),
                EventLiveStateHistoryRecord(
                    event_id=501,
                    observed_status_type="inprogress",
                    poll_profile="hot",
                    home_score=2,
                    away_score=1,
                    period_label="2nd",
                    observed_at="2026-04-17T10:05:00+00:00",
                ),
            )
        )

        report = await rebuild_live_state_from_postgres(
            repository=repository,
            sql_executor=object(),
            live_state_store=store,
            now_ms=1_800_000_000_000,
        )

        self.assertEqual(report.restored_hot, 1)
        self.assertEqual(report.restored_warm, 0)
        self.assertIn("501", backend.sorted_sets[store.hot_zset_key])
        self.assertNotIn("501", backend.sorted_sets.get(store.warm_zset_key, {}))


class _FakeHistoryRepository:
    def __init__(self, *, rows) -> None:
        self.rows = rows

    async def fetch_latest_live_state_history(self, executor):
        del executor
        return self.rows

    async def fetch_terminal_states(self, executor):
        del executor
        return ()


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.hashes = {}
        self.sorted_sets = {}

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        self.hashes[key] = dict(mapping)
        return 1

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.sorted_sets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[str(member)] = float(score)
        return len(mapping)

    def zrem(self, key: str, *members: str) -> int:
        bucket = self.sorted_sets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    def zrangebyscore(self, key: str, min_score: float, max_score: float):
        bucket = self.sorted_sets.get(key, {})
        return [member for member, score in bucket.items() if min_score <= score <= max_score]


if __name__ == "__main__":
    unittest.main()
