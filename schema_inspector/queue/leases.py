"""Lease helpers for distributed ETL work coordination."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Lease:
    key: str
    token: str
    ttl_ms: int
    acquired_at_ms: int
    expires_at_ms: int


class LeaseManager:
    """Implements SET NX PX style leases using a redis-like backend."""

    def __init__(self, backend: Any) -> None:
        self.backend = backend

    def acquire(self, key: str, token: str, *, ttl_ms: int, now_ms: int | None = None) -> Lease | None:
        observed_at = _resolve_now_ms(now_ms)
        created = _call_backend(self.backend.set, key, token, nx=True, px=ttl_ms, now_ms=observed_at)
        if not created:
            return None
        return Lease(
            key=key,
            token=token,
            ttl_ms=ttl_ms,
            acquired_at_ms=observed_at,
            expires_at_ms=observed_at + ttl_ms,
        )

    def renew(self, key: str, token: str, *, ttl_ms: int, now_ms: int | None = None) -> bool:
        observed_at = _resolve_now_ms(now_ms)
        current = _call_backend(self.backend.get, key, now_ms=observed_at)
        if current != token:
            return False
        return bool(_call_backend(self.backend.pexpire, key, ttl_ms, now_ms=observed_at))

    def release(self, key: str, token: str, *, now_ms: int | None = None) -> bool:
        observed_at = _resolve_now_ms(now_ms)
        current = _call_backend(self.backend.get, key, now_ms=observed_at)
        if current != token:
            return False
        return bool(_call_backend(self.backend.delete, key))


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
