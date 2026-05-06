from __future__ import annotations

import unittest

from schema_inspector.queue.resource_cursor import build_cursor_field
from schema_inspector.queue.resource_negative_cache import (
    DEFAULT_TTL_SECONDS,
    KEY_PREFIX,
    ResourceNegativeCache,
)


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, tuple[str, int | None]] = {}
        self.exists_calls: list[str] = []
        self.set_calls: list[tuple[str, str, int | None]] = []
        self.delete_calls: list[str] = []

    def exists(self, key: str) -> int:
        self.exists_calls.append(key)
        return 1 if key in self.store else 0

    def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self.set_calls.append((key, value, ex))
        self.store[key] = (str(value), ex)
        return True

    def delete(self, key: str) -> int:
        self.delete_calls.append(key)
        return 1 if self.store.pop(key, None) is not None else 0


class _BrokenRedis:
    def exists(self, key):
        raise RuntimeError("boom-exists")

    def set(self, key, value, ex=None):
        raise RuntimeError("boom-set")


PATTERN = "/api/v1/team/{team_id}/players"


class ResourceNegativeCacheTests(unittest.TestCase):
    def test_default_ttl_constant(self) -> None:
        # 7 days expressed in seconds.
        self.assertEqual(DEFAULT_TTL_SECONDS, 7 * 24 * 3600)

    def test_miss_when_unmarked(self) -> None:
        backend = _FakeRedis()
        cache = ResourceNegativeCache(backend, env={})
        self.assertFalse(
            cache.is_negatively_cached(endpoint_pattern=PATTERN, path_params={"team_id": 1})
        )

    def test_mark_then_hit(self) -> None:
        backend = _FakeRedis()
        cache = ResourceNegativeCache(backend, env={})
        cache.mark_404(endpoint_pattern=PATTERN, path_params={"team_id": 42})
        self.assertTrue(
            cache.is_negatively_cached(endpoint_pattern=PATTERN, path_params={"team_id": 42})
        )
        # Default TTL applied.
        _, _, ex = backend.set_calls[0]
        self.assertEqual(ex, DEFAULT_TTL_SECONDS)

    def test_key_matches_cursor_field_with_prefix(self) -> None:
        backend = _FakeRedis()
        cache = ResourceNegativeCache(backend, env={})
        cache.mark_404(endpoint_pattern=PATTERN, path_params={"team_id": 1234})
        key = backend.set_calls[0][0]
        self.assertTrue(key.startswith(KEY_PREFIX))
        self.assertEqual(
            key,
            KEY_PREFIX + build_cursor_field(PATTERN, {"team_id": 1234}),
        )

    def test_distinct_path_params_get_distinct_keys(self) -> None:
        backend = _FakeRedis()
        cache = ResourceNegativeCache(backend, env={})
        cache.mark_404(
            endpoint_pattern="/api/v1/team/{team_id}/events/last/{page}",
            path_params={"team_id": 1, "page": 0},
        )
        cache.mark_404(
            endpoint_pattern="/api/v1/team/{team_id}/events/last/{page}",
            path_params={"team_id": 1, "page": 1},
        )
        self.assertEqual(len(backend.set_calls), 2)
        self.assertNotEqual(backend.set_calls[0][0], backend.set_calls[1][0])

    def test_env_override_ttl(self) -> None:
        backend = _FakeRedis()
        cache = ResourceNegativeCache(
            backend,
            env={"SCHEMA_INSPECTOR_RESOURCE_NEGATIVE_TTL_SECONDS": "3600"},
        )
        cache.mark_404(endpoint_pattern=PATTERN, path_params={"team_id": 7})
        _, _, ex = backend.set_calls[0]
        self.assertEqual(ex, 3600)

    def test_explicit_ttl_argument_wins(self) -> None:
        backend = _FakeRedis()
        cache = ResourceNegativeCache(backend, env={})
        cache.mark_404(
            endpoint_pattern=PATTERN, path_params={"team_id": 7}, ttl_seconds=60
        )
        _, _, ex = backend.set_calls[0]
        self.assertEqual(ex, 60)

    def test_clear_removes_key(self) -> None:
        backend = _FakeRedis()
        cache = ResourceNegativeCache(backend, env={})
        cache.mark_404(endpoint_pattern=PATTERN, path_params={"team_id": 9})
        self.assertTrue(
            cache.is_negatively_cached(endpoint_pattern=PATTERN, path_params={"team_id": 9})
        )
        cache.clear(endpoint_pattern=PATTERN, path_params={"team_id": 9})
        self.assertFalse(
            cache.is_negatively_cached(endpoint_pattern=PATTERN, path_params={"team_id": 9})
        )

    def test_fail_open_on_redis_errors(self) -> None:
        cache = ResourceNegativeCache(_BrokenRedis(), env={})
        # Reads return False instead of propagating.
        self.assertFalse(
            cache.is_negatively_cached(endpoint_pattern=PATTERN, path_params={"team_id": 1})
        )
        # Marks swallow the error silently.
        cache.mark_404(endpoint_pattern=PATTERN, path_params={"team_id": 1})

    def test_invalid_env_falls_back_to_default(self) -> None:
        cache = ResourceNegativeCache(
            _FakeRedis(),
            env={"SCHEMA_INSPECTOR_RESOURCE_NEGATIVE_TTL_SECONDS": "not-a-number"},
        )
        self.assertEqual(cache.ttl_seconds, DEFAULT_TTL_SECONDS)


if __name__ == "__main__":
    unittest.main()
