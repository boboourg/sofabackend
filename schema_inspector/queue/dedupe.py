"""Dedupe and freshness windows for ETL jobs and resources."""

from __future__ import annotations

import time
from typing import Any


class DedupeStore:
    """Implements short-lived NX keys for queue dedupe and freshness hints."""

    def __init__(self, backend: Any) -> None:
        self.backend = backend

    def claim_job(self, key: str, *, ttl_ms: int, now_ms: int | None = None) -> bool:
        observed_at = _resolve_now_ms(now_ms)
        return bool(_call_backend(self.backend.set, key, "1", nx=True, px=ttl_ms, now_ms=observed_at))

    def mark_fresh(self, key: str, *, ttl_ms: int, now_ms: int | None = None) -> None:
        observed_at = _resolve_now_ms(now_ms)
        _call_backend(self.backend.set, key, "1", nx=False, px=ttl_ms, now_ms=observed_at)

    def is_fresh(self, key: str, *, now_ms: int | None = None) -> bool:
        observed_at = _resolve_now_ms(now_ms)
        return bool(_call_backend(self.backend.exists, key, now_ms=observed_at))


def _resolve_now_ms(now_ms: int | None) -> int:
    if now_ms is not None:
        return int(now_ms)
    return int(time.time() * 1000)


def _call_backend(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except TypeError:
        filtered_kwargs = {key: value for key, value in kwargs.items() if key != "now_ms"}
        return method(*args, **filtered_kwargs)
