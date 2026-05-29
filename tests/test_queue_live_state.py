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

    def test_live_state_store_filters_none_values_before_hset(self) -> None:
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
                home_score=None,
                away_score=None,
                version_hint=None,
                is_finalized=False,
            ),
            lane="hot",
        )

        assert backend.last_hset_mapping is not None
        self.assertNotIn("home_score", backend.last_hset_mapping)
        self.assertNotIn("away_score", backend.last_hset_mapping)
        self.assertNotIn("version_hint", backend.last_hset_mapping)
        self.assertEqual(backend.last_hset_mapping["status_type"], "inprogress")


class _StrictRedisStyleBackend:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, object]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.last_hset_name: str | None = None
        self.last_hset_mapping: dict[str, object] | None = None

    def hset(self, name: str, key: str | None = None, value: object | None = None, mapping: dict[str, object] | None = None):
        if mapping is None:
            raise TypeError("mapping keyword is required for redis-style hset")
        if any(item is None for item in mapping.values()):
            raise TypeError("None values are not allowed in redis-style hset mapping")
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


class _EvalRedisBackend:
    """In-memory Redis stub supporting EVAL of the live_state lane scripts.

    Records eval calls and reproduces the script semantics in Python against
    decoded ('0'/'1') string hash values, matching decode_responses=True on
    the prod client.
    """

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.zsets: dict[str, dict[str, float]] = {}
        self.eval_calls: list[tuple[int, tuple[str, ...]]] = []

    def hset(self, name, key=None, value=None, mapping=None):
        bucket = self.hashes.setdefault(name, {})
        for k, v in dict(mapping or {}).items():
            bucket[str(k)] = str(v)
        return len(mapping or {})

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def zadd(self, key, mapping):
        self.zsets.setdefault(key, {}).update(mapping)
        return len(mapping)

    def zrem(self, key, *members):
        bucket = self.zsets.setdefault(key, {})
        removed = 0
        for m in members:
            if m in bucket:
                del bucket[m]
                removed += 1
        return removed

    def eval(self, script, numkeys, *args):
        self.eval_calls.append((numkeys, tuple(args)))
        keys = list(args[:numkeys])
        argv = list(args[numkeys:])
        member = argv[0]
        for lane_key in keys[:3]:
            self.zsets.setdefault(lane_key, {}).pop(member, None)
        if numkeys == 3:  # remove_from_lanes script
            return 1
        # move_lane script: KEYS[4]=target, KEYS[5]=event hash
        target_key, hash_key = keys[3], keys[4]
        finalized = self.hashes.get(hash_key, {}).get("is_finalized")
        if finalized in ("1", "true"):
            return 0
        self.zsets.setdefault(target_key, {})[member] = float(argv[1])
        return 1


class QueueLiveStateAtomicLaneTests(unittest.TestCase):
    def _membership(self, backend, store, member):
        return [
            store.hot_zset_key in backend.zsets and member in backend.zsets[store.hot_zset_key],
            store.warm_zset_key in backend.zsets and member in backend.zsets[store.warm_zset_key],
            store.cold_zset_key in backend.zsets and member in backend.zsets[store.cold_zset_key],
        ]

    def test_move_lane_uses_single_eval_when_backend_supports_it(self) -> None:
        backend = _EvalRedisBackend()
        store = LiveEventStateStore(backend)
        store.move_lane(14019275, lane="hot", next_poll_at=1_800_000_030_000)
        self.assertEqual(len(backend.eval_calls), 1)
        numkeys, _argv = backend.eval_calls[0]
        self.assertEqual(numkeys, 5)

    def test_move_lane_never_leaves_event_in_two_lanes(self) -> None:
        backend = _EvalRedisBackend()
        store = LiveEventStateStore(backend)
        store.move_lane(42, lane="hot", next_poll_at=1_800_000_000_000)
        store.move_lane(42, lane="warm", next_poll_at=1_800_000_060_000)
        membership = self._membership(backend, store, "42")
        self.assertEqual(sum(membership), 1, "event must be in exactly one lane")
        self.assertTrue(membership[1], "event must be in the warm lane")

    def test_move_lane_refuses_finalized_event_via_script(self) -> None:
        backend = _EvalRedisBackend()
        store = LiveEventStateStore(backend)
        backend.hashes[store._key(42)] = {"is_finalized": "1"}
        store.move_lane(42, lane="hot", next_poll_at=1_800_000_000_000)
        self.assertEqual(
            sum(self._membership(backend, store, "42")),
            0,
            "finalized event must not be (re-)added to any lane",
        )

    def test_move_lane_falls_back_to_discrete_ops_without_eval(self) -> None:
        backend = _StrictRedisStyleBackend()  # no eval attribute
        store = LiveEventStateStore(backend)
        backend.zadd(store.hot_zset_key, {"42": 1.0})
        store.move_lane(42, lane="warm", next_poll_at=1_800_000_060_000)
        membership = [
            "42" in backend.zsets.get(store.hot_zset_key, {}),
            "42" in backend.zsets.get(store.warm_zset_key, {}),
            "42" in backend.zsets.get(store.cold_zset_key, {}),
        ]
        self.assertEqual(sum(membership), 1)
        self.assertTrue(membership[1])

    def test_finalize_removes_from_all_lanes_atomically(self) -> None:
        backend = _EvalRedisBackend()
        store = LiveEventStateStore(backend)
        for lane_key in (store.hot_zset_key, store.warm_zset_key, store.cold_zset_key):
            backend.zadd(lane_key, {"42": 1.0})
        store.remove_from_lanes(42)
        self.assertEqual(len(backend.eval_calls), 1)
        self.assertEqual(backend.eval_calls[0][0], 3)
        self.assertEqual(sum(self._membership(backend, store, "42")), 0)


if __name__ == "__main__":
    unittest.main()
