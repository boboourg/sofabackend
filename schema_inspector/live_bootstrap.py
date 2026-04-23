"""Persistent live bootstrap coordination.

Postgres is the source of truth. Redis is only a short-lived hot cache plus
single-flight guard so a slow proxy window cannot stack duplicate hydrates.
"""

from __future__ import annotations

from typing import Any


BOOTSTRAP_CACHE_KEY = "live:bootstrap_done:{event_id}"
HYDRATE_LOCK_KEY = "live:hydrate_lock:{event_id}"


class LiveBootstrapCoordinator:
    def __init__(
        self,
        *,
        redis_backend: Any = None,
        worker_id: str = "unknown",
        cache_ttl_s: int = 900,
        lock_ttl_ms: int = 60_000,
    ) -> None:
        self.redis_backend = redis_backend
        self.worker_id = str(worker_id)
        self.cache_ttl_s = int(cache_ttl_s)
        self.lock_ttl_ms = int(lock_ttl_ms)

    async def is_bootstrapped(self, executor: Any, *, event_id: int) -> bool:
        event_id = int(event_id)
        cache_key = BOOTSTRAP_CACHE_KEY.format(event_id=event_id)
        cached = self._redis_get(cache_key)
        if _as_text(cached) == "1":
            return True

        value = await executor.fetchval(
            "SELECT live_bootstrap_done_at IS NOT NULL FROM event WHERE id = $1",
            event_id,
        )
        if bool(value):
            self._redis_set(cache_key, "1", ex=self.cache_ttl_s)
        return bool(value)

    async def mark_bootstrapped(self, executor: Any, *, event_id: int) -> None:
        event_id = int(event_id)
        await executor.execute("UPDATE event SET live_bootstrap_done_at = now() WHERE id = $1", event_id)
        self._redis_set(BOOTSTRAP_CACHE_KEY.format(event_id=event_id), "1", ex=self.cache_ttl_s)

    async def reset_bootstrap(self, executor: Any, *, event_id: int) -> None:
        event_id = int(event_id)
        await executor.execute("UPDATE event SET live_bootstrap_done_at = NULL WHERE id = $1", event_id)
        self._redis_delete(BOOTSTRAP_CACHE_KEY.format(event_id=event_id))

    def acquire_hydrate_lock(self, *, event_id: int) -> bool:
        key = HYDRATE_LOCK_KEY.format(event_id=int(event_id))
        return bool(self._redis_set(key, self.worker_id, nx=True, px=self.lock_ttl_ms))

    def _redis_get(self, key: str) -> object | None:
        if self.redis_backend is None:
            return None
        try:
            return self.redis_backend.get(key)
        except Exception:
            return None

    def _redis_set(self, key: str, value: object, **kwargs: object) -> bool:
        if self.redis_backend is None:
            return False
        try:
            return bool(self.redis_backend.set(key, value, **kwargs))
        except TypeError:
            fallback_kwargs = dict(kwargs)
            fallback_kwargs.pop("px", None)
            return bool(self.redis_backend.set(key, value, **fallback_kwargs))
        except Exception:
            return False

    def _redis_delete(self, key: str) -> int:
        if self.redis_backend is None:
            return 0
        try:
            return int(self.redis_backend.delete(key))
        except Exception:
            return 0


def _as_text(value: object | None) -> str | None:
    if value in (None, "", b""):
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)
