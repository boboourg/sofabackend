"""Resource-level freshness store backed by Redis.

NOT to be confused with the existing FreshnessPolicy in event hydration:
that policy manages claim/lease semantics for event-level processing.
This module is purely about HTTP resource caching, e.g. "have we recently
fetched player/{N}; if yes, skip the redundant request".
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


class FreshnessStore:
    """Small Redis TTL wrapper for cross-run HTTP resource freshness."""

    def __init__(self, backend: Any, log_interval_seconds: int = 300) -> None:
        self._backend = backend
        self._hits = 0
        self._misses = 0
        self._marks = 0
        self._last_log_at = time.monotonic()
        self._log_interval = int(log_interval_seconds)

    def is_fresh(self, key: str) -> bool:
        """Return True when the Redis freshness key exists.

        Redis owns expiry; readers do not extend the TTL. On backend errors we
        fail open by returning False, preserving the old fetch behavior.
        """

        try:
            observed_at = _now_ms()
            exists = bool(_call_backend(self._backend.exists, key, now_ms=observed_at))
        except Exception as exc:
            logger.warning("FreshnessStore.is_fresh failed (fail-open): %s", exc)
            self._maybe_log_stats()
            return False
        if exists:
            self._hits += 1
            self._maybe_log_stats()
            return True
        self._misses += 1
        self._maybe_log_stats()
        return False

    def mark_fetched(self, key: str, ttl_seconds: int) -> None:
        """Mark a resource as freshly fetched with a Redis TTL."""

        try:
            ttl_ms = int(ttl_seconds) * 1000
            observed_at = _now_ms()
            _call_backend(self._backend.set, key, "1", px=ttl_ms, now_ms=observed_at)
            self._marks += 1
        except Exception as exc:
            logger.warning("FreshnessStore.mark_fetched failed: %s", exc)
        finally:
            self._maybe_log_stats()

    def snapshot_stats(self) -> dict[str, int]:
        return {
            "hits": self._hits,
            "misses": self._misses,
            "marks": self._marks,
        }

    def _maybe_log_stats(self) -> None:
        if self._log_interval <= 0:
            return
        now = time.monotonic()
        if now - self._last_log_at < self._log_interval:
            return
        self._last_log_at = now
        logger.info(
            "FreshnessStore stats: hits=%s misses=%s marks=%s",
            self._hits,
            self._misses,
            self._marks,
        )


def _now_ms() -> int:
    return int(time.time() * 1000)


def _call_backend(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except TypeError:
        filtered_kwargs = {key: value for key, value in kwargs.items() if key != "now_ms"}
        return method(*args, **filtered_kwargs)
