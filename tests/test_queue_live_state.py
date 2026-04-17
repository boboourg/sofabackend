from __future__ import annotations

import unittest

from schema_inspector.queue.live_state import LiveEventState, LiveEventStateStore


class QueueLiveStateTests(unittest.TestCase):
    def test_live_state_store_uses_mapping_keyword_for_redis_hset(self) -> None:
        backend = _StrictRedisStyleBackend()
        store = LiveEventStateStore(backend)

        store.upsert(
            LiveEventState(
                event_id=14019275,
                sport_slug="football",
                status_type="inprogress",
                poll_profile="hot",
                last_seen_at=1_800_000_000_000,
                last_ingested_at=1_800_000_000_000,
                last_changed_at=1_800_000_000_000,
                next_poll_at=1_800_000_030_000,
                hot_until=1_800_000_030_000,
                home_score=1,
                away_score=0,
                version_hint=None,
                is_finalized=False,
            ),
            lane="hot",
        )

        self.assertEqual(backend.last_hset_name, "live:event:14019275")
        self.assertIsInstance(backend.last_hset_mapping, dict)
        self.assertEqual(backend.last_hset_mapping["sport_slug"], "football")


class _StrictRedisStyleBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.last_hset_name: str | None = None
        self.last_hset_mapping: dict[str, object] | None = None

    def hset(self, name: str, key: str | None = None, value: object | None = None, mapping: dict[str, object] | None = None):
        if mapping is None:
            raise TypeError("mapping keyword is required for redis-style hset")
        self.last_hset_name = name
        self.last_hset_mapping = dict(mapping)
        self.hashes.setdefault(name, {}).update(dict(mapping))
        return len(mapping)

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        self.zsets.setdefault(key, {}).update(mapping)
        return len(mapping)

    def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                removed += 1
                del bucket[member]
        return removed

    def zrangebyscore(self, key: str, min_score: float, max_score: float):
        return [
            member
            for member, score in sorted(self.zsets.get(key, {}).items(), key=lambda item: (item[1], item[0]))
            if min_score <= score <= max_score
        ]

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))


if __name__ == "__main__":
    unittest.main()
