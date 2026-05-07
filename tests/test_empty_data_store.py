from __future__ import annotations

import unittest

from schema_inspector.queue.empty_data import DEFAULT_HASH_KEY, EmptyDataStore, _build_field


class _FakeRedis:
    def __init__(self):
        self.hashes: dict[str, dict[str, str]] = {}

    def hset(self, name, mapping=None, **kwargs):
        # Accept either keyword/positional `mapping`. The store's mark_*
        # method tries the modern keyword form first and falls back to
        # positional, so cover both.
        if mapping is None and "mapping" in kwargs:
            mapping = kwargs["mapping"]
        if mapping is None:
            return 0
        self.hashes.setdefault(name, {}).update({k: str(v) for k, v in mapping.items()})
        return len(mapping)

    def hget(self, name, field):
        return self.hashes.get(name, {}).get(field)

    def hdel(self, name, *fields):
        bucket = self.hashes.setdefault(name, {})
        removed = 0
        for f in fields:
            if f in bucket:
                del bucket[f]
                removed += 1
        return removed


class _BrokenRedis:
    def hset(self, *a, **kw):
        raise RuntimeError("boom-write")

    def hget(self, *a, **kw):
        raise RuntimeError("boom-read")


class EmptyDataStoreTests(unittest.TestCase):
    def test_field_layout(self) -> None:
        self.assertEqual(_build_field("/api/v1/x", 750), "/api/v1/x|750")

    def test_unmarked_target_is_not_recently_empty(self) -> None:
        store = EmptyDataStore(_FakeRedis())
        self.assertFalse(
            store.is_empty_recently(
                endpoint_pattern="/api/v1/player/{player_id}/last-year-summary",
                entity_id=750,
                ttl_seconds=14 * 86400,
                now_ms=1_000_000,
            )
        )

    def test_mark_then_is_recently_empty(self) -> None:
        backend = _FakeRedis()
        store = EmptyDataStore(backend)
        store.mark_empty(
            endpoint_pattern="/api/v1/player/{player_id}/last-year-summary",
            entity_id=4747,
            when_ms=1_000_000,
        )
        # Stored under the documented hash key + field layout.
        self.assertEqual(
            backend.hashes[DEFAULT_HASH_KEY],
            {"/api/v1/player/{player_id}/last-year-summary|4747": "1000000"},
        )
        self.assertTrue(
            store.is_empty_recently(
                endpoint_pattern="/api/v1/player/{player_id}/last-year-summary",
                entity_id=4747,
                ttl_seconds=14 * 86400,
                now_ms=1_000_000 + 60_000,  # 60 s later, well within TTL
            )
        )

    def test_mark_then_expired_returns_false(self) -> None:
        store = EmptyDataStore(_FakeRedis())
        store.mark_empty(
            endpoint_pattern="/api/v1/player/{player_id}/last-year-summary",
            entity_id=4747,
            when_ms=0,
        )
        self.assertFalse(
            store.is_empty_recently(
                endpoint_pattern="/api/v1/player/{player_id}/last-year-summary",
                entity_id=4747,
                ttl_seconds=14 * 86400,
                now_ms=20 * 86400 * 1000,  # 20 days later
            )
        )

    def test_zero_ttl_means_never_recently_empty(self) -> None:
        backend = _FakeRedis()
        store = EmptyDataStore(backend)
        store.mark_empty(
            endpoint_pattern="x",
            entity_id=1,
            when_ms=1,
        )
        self.assertFalse(
            store.is_empty_recently(
                endpoint_pattern="x",
                entity_id=1,
                ttl_seconds=0,
                now_ms=1,
            )
        )

    def test_clear_resets_marker(self) -> None:
        backend = _FakeRedis()
        store = EmptyDataStore(backend)
        store.mark_empty(endpoint_pattern="x", entity_id=1, when_ms=1)
        store.clear(endpoint_pattern="x", entity_id=1)
        self.assertFalse(
            store.is_empty_recently(
                endpoint_pattern="x", entity_id=1, ttl_seconds=86400, now_ms=2
            )
        )

    def test_garbage_value_treated_as_unmarked(self) -> None:
        backend = _FakeRedis()
        backend.hashes[DEFAULT_HASH_KEY] = {"x|1": "not-an-int"}
        store = EmptyDataStore(backend)
        self.assertFalse(
            store.is_empty_recently(
                endpoint_pattern="x", entity_id=1, ttl_seconds=86400, now_ms=1
            )
        )

    def test_redis_failure_fails_open_for_reads_and_writes(self) -> None:
        store = EmptyDataStore(_BrokenRedis())
        # mark_empty must not raise
        store.mark_empty(endpoint_pattern="x", entity_id=1, when_ms=1)
        # is_empty_recently must return False (fail-open)
        self.assertFalse(
            store.is_empty_recently(
                endpoint_pattern="x", entity_id=1, ttl_seconds=86400, now_ms=1
            )
        )

    def test_no_backend_is_a_noop(self) -> None:
        store = EmptyDataStore(None)
        # Must not raise on any operation.
        store.mark_empty(endpoint_pattern="x", entity_id=1)
        store.clear(endpoint_pattern="x", entity_id=1)
        self.assertFalse(
            store.is_empty_recently(
                endpoint_pattern="x", entity_id=1, ttl_seconds=86400
            )
        )


if __name__ == "__main__":
    unittest.main()
