from __future__ import annotations

import unittest

from schema_inspector.queue.pagination_done import (
    DEFAULT_HASH_KEY,
    PaginationDoneStore,
    _build_field,
)


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, dict[str, str]] = {}
        self.hget_calls: list[tuple[str, str]] = []
        self.hset_calls: list[tuple[str, dict[str, str]]] = []
        self.hdel_calls: list[tuple[str, str]] = []

    def hget(self, key, field):
        self.hget_calls.append((key, field))
        return self.store.get(key, {}).get(field)

    def hset(self, key, mapping=None, **kwargs):
        if mapping is None:
            mapping = kwargs.get("mapping")
        self.hset_calls.append((key, dict(mapping or {})))
        bucket = self.store.setdefault(key, {})
        bucket.update(mapping or {})
        return len(mapping or {})

    def hdel(self, key, field):
        self.hdel_calls.append((key, field))
        return 1 if self.store.get(key, {}).pop(field, None) is not None else 0


class _BrokenRedis:
    def hget(self, key, field):
        raise RuntimeError("boom-read")

    def hset(self, key, *args, **kwargs):
        raise RuntimeError("boom-write")


PATTERN = "/api/v1/player/{player_id}/events/last/{page}"


class BuildFieldTests(unittest.TestCase):
    def test_field_includes_pattern_and_entity_id(self) -> None:
        self.assertEqual(_build_field(PATTERN, 750), f"{PATTERN}|750")

    def test_distinct_entities_distinct_fields(self) -> None:
        self.assertNotEqual(_build_field(PATTERN, 1), _build_field(PATTERN, 2))


class PaginationDoneStoreTests(unittest.TestCase):
    def test_unmarked_is_not_recent(self) -> None:
        store = PaginationDoneStore(_FakeRedis())
        self.assertFalse(
            store.is_completed_recently(
                endpoint_pattern=PATTERN,
                entity_id=750,
                audit_interval_seconds=14 * 86400,
                now_ms=10_000_000,
            )
        )

    def test_mark_then_recent(self) -> None:
        backend = _FakeRedis()
        store = PaginationDoneStore(backend)
        store.mark_completed(endpoint_pattern=PATTERN, entity_id=750, when_ms=10_000_000)
        self.assertTrue(
            store.is_completed_recently(
                endpoint_pattern=PATTERN,
                entity_id=750,
                audit_interval_seconds=14 * 86400,
                now_ms=10_000_000 + 60_000,  # +60s
            )
        )
        # Default hash key used:
        self.assertEqual(backend.hset_calls[0][0], DEFAULT_HASH_KEY)

    def test_mark_then_expired_is_not_recent(self) -> None:
        store = PaginationDoneStore(_FakeRedis())
        store.mark_completed(endpoint_pattern=PATTERN, entity_id=750, when_ms=0)
        # 14 days + 1 hour later -> beyond audit window.
        self.assertFalse(
            store.is_completed_recently(
                endpoint_pattern=PATTERN,
                entity_id=750,
                audit_interval_seconds=14 * 86400,
                now_ms=(14 * 86400 + 3600) * 1000,
            )
        )

    def test_zero_audit_interval_means_never_recent(self) -> None:
        # Defensive: audit_interval=0 must not silently treat everything as fresh.
        store = PaginationDoneStore(_FakeRedis())
        store.mark_completed(endpoint_pattern=PATTERN, entity_id=750, when_ms=10_000)
        self.assertFalse(
            store.is_completed_recently(
                endpoint_pattern=PATTERN,
                entity_id=750,
                audit_interval_seconds=0,
                now_ms=10_001,
            )
        )

    def test_clear_resets_marker(self) -> None:
        backend = _FakeRedis()
        store = PaginationDoneStore(backend)
        store.mark_completed(endpoint_pattern=PATTERN, entity_id=750, when_ms=10_000)
        store.clear(endpoint_pattern=PATTERN, entity_id=750)
        self.assertFalse(
            store.is_completed_recently(
                endpoint_pattern=PATTERN,
                entity_id=750,
                audit_interval_seconds=14 * 86400,
                now_ms=10_001,
            )
        )

    def test_garbage_value_treated_as_unmarked(self) -> None:
        backend = _FakeRedis()
        backend.store[DEFAULT_HASH_KEY] = {_build_field(PATTERN, 750): "not-a-number"}
        store = PaginationDoneStore(backend)
        self.assertFalse(
            store.is_completed_recently(
                endpoint_pattern=PATTERN,
                entity_id=750,
                audit_interval_seconds=14 * 86400,
                now_ms=10_001,
            )
        )

    def test_fail_open_on_redis_errors(self) -> None:
        store = PaginationDoneStore(_BrokenRedis())
        self.assertFalse(
            store.is_completed_recently(
                endpoint_pattern=PATTERN,
                entity_id=750,
                audit_interval_seconds=14 * 86400,
                now_ms=10_001,
            )
        )
        # Mark must not propagate.
        store.mark_completed(endpoint_pattern=PATTERN, entity_id=750)


if __name__ == "__main__":
    unittest.main()
