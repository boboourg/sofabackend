"""Alert dedupe stores.

The monitoring daemon checks the dedupe store before firing an alert for
a given ``(signal_name, severity)`` pair. If the previous alert for the
same key was sent within the TTL window, the new alert is suppressed.

Two implementations:

* :class:`RedisDedupeStore` — production store, uses Redis hash
  ``monitoring:dedupe`` with key ``<signal>:<severity>`` and value
  ``<unix_timestamp>``. A whole-hash TTL keeps the store cleaning itself.
* :class:`NullDedupeStore` — fail-open store for development / tests.
  Always returns ``True`` from ``should_send`` and ignores marks.

The store also exposes :meth:`record_resolved` which a daemon calls when
a signal returns to ``OK``. That call clears all dedupe state for the
signal so a future re-breach is announced immediately rather than being
suppressed by stale dedupe data.
"""

from __future__ import annotations

import logging
import time
from typing import Any, Protocol


logger = logging.getLogger(__name__)


# Sentinel returned by ``last_sent_seconds`` when there is no record.
_NEVER_SENT = -1


class DedupeStore(Protocol):
    """Protocol for an alert dedupe store."""

    def should_send(
        self, signal_name: str, severity: str, ttl_seconds: int
    ) -> bool: ...

    def mark_sent(self, signal_name: str, severity: str, ttl_seconds: int) -> None: ...

    def record_resolved(self, signal_name: str) -> None: ...

    def get_repeat_count(self, signal_name: str, severity: str) -> int: ...

    def get_first_alerted_at_epoch(
        self, signal_name: str, severity: str
    ) -> float | None: ...


class NullDedupeStore:
    """Always send, never mark, never resolve. For tests / no-Redis dev."""

    def should_send(self, signal_name: str, severity: str, ttl_seconds: int) -> bool:
        del signal_name, severity, ttl_seconds
        return True

    def mark_sent(self, signal_name: str, severity: str, ttl_seconds: int) -> None:
        del signal_name, severity, ttl_seconds

    def record_resolved(self, signal_name: str) -> None:
        del signal_name

    def get_repeat_count(self, signal_name: str, severity: str) -> int:
        del signal_name, severity
        return 0

    def get_first_alerted_at_epoch(
        self, signal_name: str, severity: str
    ) -> float | None:
        del signal_name, severity
        return None


class RedisDedupeStore:
    """Redis-hash backed dedupe with TTL.

    Layout (one hash per daemon instance, default key
    ``monitoring:dedupe``):

      HSET monitoring:dedupe <signal>:<severity>:last_sent  <epoch>
      HSET monitoring:dedupe <signal>:<severity>:first_sent <epoch>
      HSET monitoring:dedupe <signal>:<severity>:count      <int>

    The whole hash gets a sliding TTL on every write so that stale entries
    eventually expire even when the daemon restarts and never explicitly
    clears them. Default TTL = max(warn_ttl, crit_ttl) * 4 — generous but
    bounded.

    Fail-open: every method swallows ``Exception`` and falls back to
    "should_send = True" (which means we'd rather deliver a duplicate
    than silently drop an alert).
    """

    def __init__(
        self,
        backend: Any,
        *,
        hash_key: str = "monitoring:dedupe",
        sliding_ttl_seconds: int = 7200,
        clock: Any = None,
    ) -> None:
        self.backend = backend
        self.hash_key = str(hash_key)
        self.sliding_ttl_seconds = int(sliding_ttl_seconds)
        self._clock = clock or time.time

    # --------------------------------------------------------------- helpers

    def _last_field(self, signal: str, severity: str) -> str:
        return f"{signal}:{severity}:last_sent"

    def _first_field(self, signal: str, severity: str) -> str:
        return f"{signal}:{severity}:first_sent"

    def _count_field(self, signal: str, severity: str) -> str:
        return f"{signal}:{severity}:count"

    def _hget_float(self, field: str) -> float | None:
        try:
            raw = self.backend.hget(self.hash_key, field)
        except Exception as exc:  # noqa: BLE001
            logger.warning("RedisDedupeStore.hget failed (%s): %r", field, exc)
            return None
        if raw in (None, ""):
            return None
        try:
            return float(raw)
        except (TypeError, ValueError):
            return None

    def _hget_int(self, field: str) -> int:
        try:
            raw = self.backend.hget(self.hash_key, field)
        except Exception as exc:  # noqa: BLE001
            logger.warning("RedisDedupeStore.hget failed (%s): %r", field, exc)
            return 0
        if raw in (None, ""):
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    # ---------------------------------------------------------------- public

    def should_send(self, signal_name: str, severity: str, ttl_seconds: int) -> bool:
        last = self._hget_float(self._last_field(signal_name, severity))
        if last is None:
            return True
        try:
            now = float(self._clock())
        except Exception:  # noqa: BLE001
            return True
        return (now - last) >= float(ttl_seconds)

    def mark_sent(self, signal_name: str, severity: str, ttl_seconds: int) -> None:
        try:
            now = float(self._clock())
        except Exception:  # noqa: BLE001
            return
        last_field = self._last_field(signal_name, severity)
        first_field = self._first_field(signal_name, severity)
        count_field = self._count_field(signal_name, severity)
        try:
            self.backend.hset(self.hash_key, last_field, str(now))
            # Set first_sent only if absent — track when the current breach
            # series began so we can include "Repeat #N (first at …)" in
            # repeat alerts.
            existing_first = self.backend.hget(self.hash_key, first_field)
            if existing_first in (None, ""):
                self.backend.hset(self.hash_key, first_field, str(now))
            self.backend.hincrby(self.hash_key, count_field, 1)
            # Refresh sliding TTL on every write.
            expire = getattr(self.backend, "expire", None)
            if callable(expire):
                expire(self.hash_key, max(self.sliding_ttl_seconds, ttl_seconds * 4))
        except Exception as exc:  # noqa: BLE001
            logger.warning("RedisDedupeStore.mark_sent failed: %r", exc)

    def record_resolved(self, signal_name: str) -> None:
        """Clear dedupe entries for both WARN and CRIT severities of one signal."""

        try:
            for severity in ("WARN", "CRIT"):
                self.backend.hdel(
                    self.hash_key,
                    self._last_field(signal_name, severity),
                    self._first_field(signal_name, severity),
                    self._count_field(signal_name, severity),
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("RedisDedupeStore.record_resolved failed: %r", exc)

    def get_repeat_count(self, signal_name: str, severity: str) -> int:
        return self._hget_int(self._count_field(signal_name, severity))

    def get_first_alerted_at_epoch(
        self, signal_name: str, severity: str
    ) -> float | None:
        return self._hget_float(self._first_field(signal_name, severity))

    def last_sent_at_epoch(self, signal_name: str, severity: str) -> float:
        """Diagnostic helper. Returns ``_NEVER_SENT`` if no record."""

        value = self._hget_float(self._last_field(signal_name, severity))
        return _NEVER_SENT if value is None else value
