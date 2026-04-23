from __future__ import annotations

import time
from typing import Callable, Generic, TypeVar


TKey = TypeVar("TKey")
TValue = TypeVar("TValue")


class ExpiringValueCache(Generic[TKey, TValue]):
    """Small process-local cache for short-circuiting repeat writes."""

    def __init__(
        self,
        *,
        ttl_seconds: float = 600.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ttl_seconds = ttl_seconds
        self._clock = clock
        self._entries: dict[TKey, tuple[float, TValue]] = {}

    def get(self, key: TKey) -> TValue | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if expires_at <= self._clock():
            self._entries.pop(key, None)
            return None
        return value

    def set(self, key: TKey, value: TValue) -> None:
        self._entries[key] = (self._clock() + self._ttl_seconds, value)
