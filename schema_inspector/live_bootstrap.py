"""Persistent live bootstrap coordination.

Postgres is the source of truth. Redis is only a short-lived hot cache plus
single-flight guard so a slow proxy window cannot stack duplicate hydrates.

2026-05-15 Task 6 fix: lock lifecycle was previously implicit (release
only via TTL expiry). When a bootstrap fan-out crashed half-way the lock
sat in Redis for the full TTL (60 s by default) and every subsequent
poll cycle for the same event saw acquire_hydrate_lock() return False
-> early-return -> never bootstrapped. Result on prod: 49% of live
events sitting indefinitely without sub-endpoint snapshots. The fix
adds an explicit ``release_hydrate_lock()`` that the orchestrator's
``finally`` block calls regardless of success/failure, mirroring the
inflight-lock pattern in LiveEventInFlightStore.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


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
        acquired = bool(self._redis_set(key, self.worker_id, nx=True, px=self.lock_ttl_ms))
        if acquired:
            logger.info(
                "live_bootstrap: lock acquired event_id=%s worker=%s ttl_ms=%s",
                event_id,
                self.worker_id,
                self.lock_ttl_ms,
            )
        else:
            logger.debug(
                "live_bootstrap: lock contention event_id=%s worker=%s",
                event_id,
                self.worker_id,
            )
        return acquired

    def release_hydrate_lock(self, *, event_id: int) -> bool:
        """Explicit lock release. Idempotent — safe to call multiple times.

        Compares the current lock owner against ``self.worker_id``; only
        deletes the key when this worker still owns it (avoids racing a
        worker that grabbed the lock after our TTL expired and started
        its own bootstrap).

        Returns True if the lock was deleted by this call, False
        otherwise (no key, or owned by someone else, or Redis error).
        """

        if self.redis_backend is None:
            return False
        key = HYDRATE_LOCK_KEY.format(event_id=int(event_id))
        try:
            current = _as_text(self.redis_backend.get(key))
        except Exception as exc:  # noqa: BLE001 — defensive
            logger.debug(
                "live_bootstrap: release GET failed event_id=%s: %r", event_id, exc
            )
            return False
        if current is None:
            # Lock already gone (expired or someone else cleared it). OK.
            return False
        if current != self.worker_id:
            # Someone else owns it now (our TTL expired mid-fanout).
            logger.warning(
                "live_bootstrap: release skipped, owner mismatch event_id=%s "
                "expected=%s actual=%s",
                event_id,
                self.worker_id,
                current,
            )
            return False
        try:
            deleted = int(self.redis_backend.delete(key))
        except Exception as exc:  # noqa: BLE001
            logger.debug(
                "live_bootstrap: release DELETE failed event_id=%s: %r",
                event_id,
                exc,
            )
            return False
        if deleted > 0:
            logger.info(
                "live_bootstrap: lock released event_id=%s worker=%s",
                event_id,
                self.worker_id,
            )
        return deleted > 0

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
