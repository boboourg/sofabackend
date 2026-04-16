"""Shared rate-limit state for Redis-backed ETL coordination."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class RateLimitState:
    scope_key: str
    cooldown_until: int | None
    burst_failures: int
    recent_429: int
    recent_403: int
    backoff_multiplier: float


class RateLimitStateStore:
    """Tracks shared cooldown and backoff hints by host/sport/route scope."""

    def __init__(self, backend: Any) -> None:
        self.backend = backend

    def load(self, scope_key: str) -> RateLimitState:
        raw = self.backend.hgetall(self._key(scope_key))
        return RateLimitState(
            scope_key=scope_key,
            cooldown_until=_maybe_int(raw.get("cooldown_until")),
            burst_failures=_maybe_int(raw.get("burst_failures")) or 0,
            recent_429=_maybe_int(raw.get("recent_429")) or 0,
            recent_403=_maybe_int(raw.get("recent_403")) or 0,
            backoff_multiplier=float(raw.get("backoff_multiplier", 1.0)),
        )

    def note_status(self, scope_key: str, *, status_code: int, observed_at_ms: int, cooldown_ms: int = 0) -> RateLimitState:
        previous = self.load(scope_key)
        recent_429 = previous.recent_429 + (1 if status_code == 429 else 0)
        recent_403 = previous.recent_403 + (1 if status_code == 403 else 0)
        burst_failures = previous.burst_failures + (1 if status_code in {403, 429} else 0)
        cooldown_until = observed_at_ms + cooldown_ms if cooldown_ms > 0 else previous.cooldown_until
        backoff_multiplier = max(1.0, 1.0 + (0.25 * burst_failures))
        state = RateLimitState(
            scope_key=scope_key,
            cooldown_until=cooldown_until,
            burst_failures=burst_failures,
            recent_429=recent_429,
            recent_403=recent_403,
            backoff_multiplier=backoff_multiplier,
        )
        self.backend.hset(
            self._key(scope_key),
            {
                "cooldown_until": state.cooldown_until,
                "burst_failures": state.burst_failures,
                "recent_429": state.recent_429,
                "recent_403": state.recent_403,
                "backoff_multiplier": state.backoff_multiplier,
            },
        )
        return state

    def is_limited(self, scope_key: str, *, now_ms: int) -> bool:
        state = self.load(scope_key)
        return state.cooldown_until is not None and now_ms < state.cooldown_until

    @staticmethod
    def _key(scope_key: str) -> str:
        return f"ratelimit:{scope_key}"


def _maybe_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    return int(value)
