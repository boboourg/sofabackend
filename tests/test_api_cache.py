"""Tests for schema_inspector.api_cache (N4 Layer B response cache).

Pins the contract:
- NullResponseCache: always cache-miss, ignores puts
- InProcessResponseCache: dict-backed, TTL respected, threadsafe
- RedisResponseCache: serialize/deserialize round-trip, fail-open on
  Redis errors, TTL passed to SET EX
- build_response_cache: env flag drives backend choice
- CacheTTLPolicy: env-overridable values, sane defaults
"""

from __future__ import annotations

import base64
import json
import unittest
from typing import Any

from schema_inspector.api_cache import (
    CacheTTLPolicy,
    CachedResponse,
    InProcessResponseCache,
    NullResponseCache,
    RedisResponseCache,
    build_response_cache,
)


def _make_response(body: bytes = b'{"ok":true}', status: int = 200) -> CachedResponse:
    return CachedResponse(
        status_code=status,
        body=body,
        cache_control=f"public, max-age={status}",
    )


# ---------------------------------------------------------------------------
# NullResponseCache
# ---------------------------------------------------------------------------


class NullResponseCacheTests(unittest.TestCase):
    def test_get_always_returns_none(self) -> None:
        cache = NullResponseCache()
        cache.put(("k",), _make_response(), ttl_seconds=300)
        self.assertIsNone(cache.get(("k",)))

    def test_put_does_not_raise(self) -> None:
        cache = NullResponseCache()
        # Smoke: any input is silently dropped.
        cache.put(None, _make_response(), 0)
        cache.put("anything", _make_response(), 10)


# ---------------------------------------------------------------------------
# InProcessResponseCache
# ---------------------------------------------------------------------------


class InProcessResponseCacheTests(unittest.TestCase):
    def test_get_after_put_returns_response(self) -> None:
        clock = [1000.0]
        cache = InProcessResponseCache(clock=lambda: clock[0])
        response = _make_response()
        cache.put(("a",), response, ttl_seconds=60)
        self.assertEqual(cache.get(("a",)), response)

    def test_get_after_ttl_returns_none(self) -> None:
        clock = [1000.0]
        cache = InProcessResponseCache(clock=lambda: clock[0])
        cache.put(("k",), _make_response(), ttl_seconds=10)
        clock[0] = 1011.0  # 11s later, past TTL
        self.assertIsNone(cache.get(("k",)))

    def test_zero_ttl_does_not_store(self) -> None:
        cache = InProcessResponseCache()
        cache.put(("k",), _make_response(), ttl_seconds=0)
        self.assertIsNone(cache.get(("k",)))

    def test_negative_ttl_does_not_store(self) -> None:
        cache = InProcessResponseCache()
        cache.put(("k",), _make_response(), ttl_seconds=-1)
        self.assertIsNone(cache.get(("k",)))

    def test_overwrite_extends_ttl(self) -> None:
        clock = [1000.0]
        cache = InProcessResponseCache(clock=lambda: clock[0])
        cache.put(("k",), _make_response(b"first"), ttl_seconds=10)
        clock[0] = 1009.0  # 1s before original expiry
        cache.put(("k",), _make_response(b"second"), ttl_seconds=10)
        clock[0] = 1018.0  # 9s after original expiry but only 9s after second put
        entry = cache.get(("k",))
        self.assertIsNotNone(entry)
        self.assertEqual(entry.body, b"second")


# ---------------------------------------------------------------------------
# RedisResponseCache
# ---------------------------------------------------------------------------


class _StubRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.ttls: dict[str, int] = {}
        self.get_calls: list[str] = []
        self.set_calls: list[tuple[str, str, int]] = []
        self.raise_on_get = False
        self.raise_on_set = False

    def get(self, key: str) -> str | None:
        if self.raise_on_get:
            raise RuntimeError("forced get error")
        self.get_calls.append(key)
        return self.store.get(key)

    def set(self, key: str, value: str, *, ex: int | None = None) -> bool:
        if self.raise_on_set:
            raise RuntimeError("forced set error")
        self.store[key] = value
        if ex is not None:
            self.ttls[key] = ex
        self.set_calls.append((key, value, ex or 0))
        return True


class RedisResponseCacheTests(unittest.TestCase):
    def test_get_returns_none_when_redis_empty(self) -> None:
        cache = RedisResponseCache(_StubRedis())
        self.assertIsNone(cache.get(("absent",)))

    def test_round_trip_preserves_body_bytes(self) -> None:
        backend = _StubRedis()
        cache = RedisResponseCache(backend)
        original = _make_response(body=b"\x00\x01binary\xff\xfe", status=200)
        cache.put(("k",), original, ttl_seconds=60)
        recovered = cache.get(("k",))
        self.assertIsNotNone(recovered)
        self.assertEqual(recovered.status_code, 200)
        self.assertEqual(recovered.body, b"\x00\x01binary\xff\xfe")
        self.assertEqual(recovered.cache_control, original.cache_control)

    def test_put_passes_ttl_to_redis_set_ex(self) -> None:
        backend = _StubRedis()
        cache = RedisResponseCache(backend)
        cache.put(("k",), _make_response(), ttl_seconds=42.7)
        # ex value rounded down to int (we max(1, int(ttl))).
        self.assertEqual(backend.set_calls[0][2], 42)

    def test_zero_ttl_does_not_store(self) -> None:
        backend = _StubRedis()
        cache = RedisResponseCache(backend)
        cache.put(("k",), _make_response(), ttl_seconds=0)
        self.assertEqual(backend.set_calls, [])

    def test_key_prefix_used(self) -> None:
        backend = _StubRedis()
        cache = RedisResponseCache(backend, key_prefix="myapp:")
        cache.put(("k",), _make_response(), ttl_seconds=10)
        self.assertTrue(backend.set_calls[0][0].startswith("myapp:"))

    def test_get_failure_returns_none(self) -> None:
        backend = _StubRedis()
        backend.raise_on_get = True
        cache = RedisResponseCache(backend)
        self.assertIsNone(cache.get(("k",)))

    def test_put_failure_swallowed(self) -> None:
        backend = _StubRedis()
        backend.raise_on_set = True
        cache = RedisResponseCache(backend)
        # Must not raise.
        cache.put(("k",), _make_response(), ttl_seconds=10)

    def test_malformed_cached_entry_returns_none(self) -> None:
        backend = _StubRedis()
        backend.store["api:resp:" + "0" * 64] = "not json"
        cache = RedisResponseCache(backend)
        # The cache.get computes its own hash so the manual key above
        # won't be looked up — but the parse-fail path is exercised via
        # the deterministic key below.
        deterministic_key = ("p", ("q1", "v1"))
        backend.store[cache._redis_key(deterministic_key)] = "not json"
        self.assertIsNone(cache.get(deterministic_key))

    def test_value_encoding_is_json(self) -> None:
        backend = _StubRedis()
        cache = RedisResponseCache(backend)
        response = _make_response(body=b"hello", status=200)
        cache.put(("k",), response, ttl_seconds=10)
        _, raw_value, _ = backend.set_calls[0]
        decoded = json.loads(raw_value)
        self.assertEqual(decoded["status_code"], 200)
        self.assertEqual(base64.b64decode(decoded["body_b64"]), b"hello")
        self.assertEqual(decoded["cache_control"], response.cache_control)


# ---------------------------------------------------------------------------
# build_response_cache factory
# ---------------------------------------------------------------------------


class BuildResponseCacheTests(unittest.TestCase):
    def test_default_is_memory(self) -> None:
        cache = build_response_cache(env={})
        self.assertIsInstance(cache, InProcessResponseCache)

    def test_null_backend(self) -> None:
        cache = build_response_cache(env={"SOFASCORE_API_RESPONSE_CACHE_BACKEND": "null"})
        self.assertIsInstance(cache, NullResponseCache)

    def test_redis_backend_with_redis_supplied(self) -> None:
        cache = build_response_cache(
            redis_backend=_StubRedis(),
            env={"SOFASCORE_API_RESPONSE_CACHE_BACKEND": "redis"},
        )
        self.assertIsInstance(cache, RedisResponseCache)

    def test_redis_backend_without_redis_falls_back_to_memory(self) -> None:
        cache = build_response_cache(
            redis_backend=None,
            env={"SOFASCORE_API_RESPONSE_CACHE_BACKEND": "redis"},
        )
        self.assertIsInstance(cache, InProcessResponseCache)

    def test_custom_key_prefix_applied(self) -> None:
        backend = _StubRedis()
        cache = build_response_cache(
            redis_backend=backend,
            env={
                "SOFASCORE_API_RESPONSE_CACHE_BACKEND": "redis",
                "SOFASCORE_API_CACHE_KEY_PREFIX": "custom:",
            },
        )
        self.assertIsInstance(cache, RedisResponseCache)
        cache.put(("k",), _make_response(), ttl_seconds=10)
        self.assertTrue(backend.set_calls[0][0].startswith("custom:"))


# ---------------------------------------------------------------------------
# CacheTTLPolicy
# ---------------------------------------------------------------------------


class CacheTTLPolicyTests(unittest.TestCase):
    def test_production_defaults(self) -> None:
        policy = CacheTTLPolicy.from_env(env={})
        self.assertEqual(policy.live_seconds, 5)
        self.assertEqual(policy.inprogress_seconds, 5)
        self.assertEqual(policy.notstarted_seconds, 60)
        self.assertEqual(policy.finalized_seconds, 3600)
        self.assertEqual(policy.scheduled_seconds, 60)
        self.assertEqual(policy.static_seconds, 3600)

    def test_env_overrides_apply(self) -> None:
        policy = CacheTTLPolicy.from_env(
            env={
                "SOFASCORE_API_CACHE_TTL_LIVE_SECONDS": "10",
                "SOFASCORE_API_CACHE_TTL_FINALIZED_SECONDS": "7200",
                "SOFASCORE_API_CACHE_TTL_STATIC_SECONDS": "86400",
            }
        )
        self.assertEqual(policy.live_seconds, 10)
        self.assertEqual(policy.finalized_seconds, 7200)
        self.assertEqual(policy.static_seconds, 86400)

    def test_invalid_env_falls_back_to_default(self) -> None:
        policy = CacheTTLPolicy.from_env(
            env={"SOFASCORE_API_CACHE_TTL_LIVE_SECONDS": "not-a-number"}
        )
        self.assertEqual(policy.live_seconds, 5)


if __name__ == "__main__":
    unittest.main()
