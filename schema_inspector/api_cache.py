"""Response cache backends for the local API server.

N4 Layer B (2026-05-14, see docs/N4_API_PERFORMANCE_PLAN.md).

Two implementations + a Protocol so ``local_api_server.py`` can swap
backends via env flag without touching call sites:

* :class:`NullResponseCache` — no-op for testing / disable.
* :class:`RedisResponseCache` — production backend, shared across
  uvicorn workers and surviving service restart.

Selection happens in :func:`build_response_cache` which reads
``SOFASCORE_API_RESPONSE_CACHE_BACKEND`` (``memory`` / ``redis`` / ``null``)
and returns an appropriate ``ResponseCache`` instance. ``memory`` is the
default to preserve backward compatibility — flipping to ``redis``
on prod is a one-line env change + restart, no code redeploy.

Key encoding: cache key is the existing
``_response_cache_key(raw_path, raw_query) -> tuple[str, tuple[...]]``
result. We hash it with sha256 so the Redis key is a fixed-size
hex digest regardless of how long the URL was.

Value encoding: JSON ``{status_code, body_b64, cache_control}``. Body
is base64-encoded because Redis values are binary-safe but JSON is not.
The +33% encoding overhead is acceptable for a cache (storage is cheap;
network egress per request is dominated by deserialize speed, and Redis
client decompresses inline).

Fail-open: every Redis operation is wrapped in try/except, ``get``
errors return ``None`` (cache miss), ``put`` errors are logged and
swallowed. The API must never crash because Redis is degraded.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Protocol


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CachedResponse:
    """Backend-agnostic response container.

    Mirrors the shape of ``local_api_server.SerializedApiResponse`` but
    decoupled to avoid circular imports. The local API server constructs
    one of these to put into the cache and converts back to its own
    type on the read path.
    """

    status_code: int
    body: bytes
    cache_control: str


class ResponseCache(Protocol):
    """Generic interface for response caches."""

    def get(self, key: Any) -> CachedResponse | None: ...

    def put(self, key: Any, response: CachedResponse, ttl_seconds: float) -> None: ...


# ---------------------------------------------------------------------------
# NullResponseCache — no-op
# ---------------------------------------------------------------------------


class NullResponseCache:
    """Always cache-miss, ignores puts. Useful for testing / kill-switch."""

    def get(self, key: Any) -> CachedResponse | None:
        del key
        return None

    def put(self, key: Any, response: CachedResponse, ttl_seconds: float) -> None:
        del key, response, ttl_seconds


# ---------------------------------------------------------------------------
# InProcessResponseCache — preserved behavior, shared with legacy callers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _CachedEntry:
    response: CachedResponse
    expires_at: float


class InProcessResponseCache:
    """Threadsafe LRU dict-backed cache. Per-process, lost on restart.

    Uses an OrderedDict for O(1) LRU eviction. When ``max_entries`` is
    reached the least-recently-used entry is evicted before the new one
    is inserted, bounding memory regardless of how many distinct paths
    the server serves.
    """

    def __init__(self, *, max_entries: int = 8192, clock: Any = None) -> None:
        self._entries: OrderedDict[Any, _CachedEntry] = OrderedDict()
        self._max_entries = max(1, max_entries)
        self._lock = threading.Lock()
        self._clock = clock or time.monotonic

    def get(self, key: Any) -> CachedResponse | None:
        now = float(self._clock())
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                del self._entries[key]
                return None
            # Promote to MRU position.
            self._entries.move_to_end(key)
            return entry.response

    def put(self, key: Any, response: CachedResponse, ttl_seconds: float) -> None:
        if ttl_seconds <= 0:
            return
        entry = _CachedEntry(
            response=response,
            expires_at=float(self._clock()) + float(ttl_seconds),
        )
        with self._lock:
            if key in self._entries:
                self._entries.move_to_end(key)
            self._entries[key] = entry
            # Evict LRU entries when over capacity.
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)


# ---------------------------------------------------------------------------
# RedisResponseCache — production backend
# ---------------------------------------------------------------------------


class RedisResponseCache:
    """Redis-backed shared response cache.

    Uses synchronous redis-py operations (``backend.get`` / ``backend.set``
    with ``ex=ttl_seconds``). The local API server's HTTP handlers are
    synchronous wrt the cache call so a sync client is the right shape.
    """

    DEFAULT_KEY_PREFIX = "api:resp:"

    def __init__(self, backend: Any, *, key_prefix: str | None = None) -> None:
        self.backend = backend
        self.key_prefix = key_prefix or self.DEFAULT_KEY_PREFIX

    def _redis_key(self, key: Any) -> str:
        digest = hashlib.sha256(repr(key).encode("utf-8")).hexdigest()
        return f"{self.key_prefix}{digest}"

    def get(self, key: Any) -> CachedResponse | None:
        try:
            raw = self.backend.get(self._redis_key(key))
        except Exception as exc:  # noqa: BLE001 — fail-open on Redis errors
            logger.warning("RedisResponseCache.get failed: %r", exc)
            return None
        if raw in (None, ""):
            return None
        try:
            data = json.loads(raw)
            return CachedResponse(
                status_code=int(data["status_code"]),
                body=base64.b64decode(data["body_b64"]),
                cache_control=str(data["cache_control"]),
            )
        except Exception as exc:  # noqa: BLE001 — malformed cache entry
            logger.warning("RedisResponseCache.get parse failed: %r", exc)
            return None

    def put(self, key: Any, response: CachedResponse, ttl_seconds: float) -> None:
        if ttl_seconds <= 0:
            return
        try:
            value = json.dumps(
                {
                    "status_code": int(response.status_code),
                    "body_b64": base64.b64encode(response.body).decode("ascii"),
                    "cache_control": str(response.cache_control),
                },
                separators=(",", ":"),
            )
            self.backend.set(
                self._redis_key(key),
                value,
                ex=max(1, int(ttl_seconds)),
            )
        except Exception as exc:  # noqa: BLE001 — fail-open
            logger.warning("RedisResponseCache.put failed: %r", exc)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_response_cache(
    *,
    redis_backend: Any = None,
    env: dict[str, str] | None = None,
) -> ResponseCache:
    """Build the configured ResponseCache implementation.

    Decision tree:

      1. ``SOFASCORE_API_RESPONSE_CACHE_BACKEND=null`` → :class:`NullResponseCache`.
      2. ``SOFASCORE_API_RESPONSE_CACHE_BACKEND=redis`` and Redis backend
         supplied → :class:`RedisResponseCache`.
      3. Default / ``memory`` → :class:`InProcessResponseCache` (legacy).

    Backward compat: default = memory. Flip ``=redis`` via .env to
    activate the Layer B shared cache.
    """

    resolved_env = dict(os.environ) if env is None else dict(env)
    backend_name = (resolved_env.get("SOFASCORE_API_RESPONSE_CACHE_BACKEND") or "memory").strip().lower()
    if backend_name == "null":
        return NullResponseCache()
    if backend_name == "redis":
        if redis_backend is None:
            logger.warning(
                "SOFASCORE_API_RESPONSE_CACHE_BACKEND=redis but no Redis backend "
                "supplied — falling back to InProcessResponseCache",
            )
            return InProcessResponseCache()
        key_prefix = (
            resolved_env.get("SOFASCORE_API_CACHE_KEY_PREFIX")
            or RedisResponseCache.DEFAULT_KEY_PREFIX
        )
        return RedisResponseCache(redis_backend, key_prefix=key_prefix)
    return InProcessResponseCache()


# ---------------------------------------------------------------------------
# TTL resolver — centralised env-overridable policy
# ---------------------------------------------------------------------------


def _env_int(env: dict[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class CacheTTLPolicy:
    """Env-backed TTL policy. All values in seconds.

    Defaults are production-appropriate. Override via env vars when
    lower latency or a different staleness budget is needed.

    live / inprogress: 5s — tight enough to reflect real-time score
        changes within one polling interval; 2s was burning the L1 dict
        excessively (too many distinct live-event paths).
    notstarted / scheduled: 60s — lineup/odds change infrequently.
    finalized: 3600s — finished results are immutable; a 30s TTL just
        burned cache capacity for no benefit.
    static: 3600s — tournament/category metadata almost never changes.
    """

    live_seconds: int = 5
    inprogress_seconds: int = 5
    notstarted_seconds: int = 60
    finalized_seconds: int = 3600
    scheduled_seconds: int = 60
    static_seconds: int = 3600

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "CacheTTLPolicy":
        resolved = dict(os.environ) if env is None else dict(env)
        return cls(
            live_seconds=_env_int(resolved, "SOFASCORE_API_CACHE_TTL_LIVE_SECONDS", 5),
            inprogress_seconds=_env_int(
                resolved, "SOFASCORE_API_CACHE_TTL_INPROGRESS_SECONDS", 5
            ),
            notstarted_seconds=_env_int(
                resolved, "SOFASCORE_API_CACHE_TTL_NOTSTARTED_SECONDS", 60
            ),
            finalized_seconds=_env_int(
                resolved, "SOFASCORE_API_CACHE_TTL_FINALIZED_SECONDS", 3600
            ),
            scheduled_seconds=_env_int(
                resolved, "SOFASCORE_API_CACHE_TTL_SCHEDULED_SECONDS", 60
            ),
            static_seconds=_env_int(
                resolved, "SOFASCORE_API_CACHE_TTL_STATIC_SECONDS", 3600
            ),
        )
