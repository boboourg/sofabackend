"""Shared proxy cooldown and health state for ETL workers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

PROXY_COOLDOWN_ZSET = "zset:proxy:cooldown"


@dataclass(frozen=True)
class ProxyState:
    proxy_id: str
    status: str
    cooldown_until: int | None
    recent_failures: int
    recent_successes: int
    last_status_code: int | None
    last_challenge_reason: str | None
    last_used_at: int | None
    avg_latency_ms: int | None


class ProxyStateStore:
    """Stores proxy health metadata in a redis-like hash/zset backend."""

    def __init__(self, backend: Any, *, cooldown_zset_key: str = PROXY_COOLDOWN_ZSET) -> None:
        self.backend = backend
        self.cooldown_zset_key = cooldown_zset_key

    def load(self, proxy_id: str) -> ProxyState:
        raw = self.backend.hgetall(self._key(proxy_id))
        return ProxyState(
            proxy_id=proxy_id,
            status=str(raw.get("status", "available")),
            cooldown_until=_maybe_int(raw.get("cooldown_until")),
            recent_failures=_maybe_int(raw.get("recent_failures")) or 0,
            recent_successes=_maybe_int(raw.get("recent_successes")) or 0,
            last_status_code=_maybe_int(raw.get("last_status_code")),
            last_challenge_reason=_maybe_str(raw.get("last_challenge_reason")),
            last_used_at=_maybe_int(raw.get("last_used_at")),
            avg_latency_ms=_maybe_int(raw.get("avg_latency_ms")),
        )

    def record_failure(
        self,
        proxy_id: str,
        *,
        status_code: int | None,
        challenge_reason: str | None,
        observed_at_ms: int,
        cooldown_ms: int,
    ) -> ProxyState:
        previous = self.load(proxy_id)
        cooldown_until = observed_at_ms + cooldown_ms
        state = ProxyState(
            proxy_id=proxy_id,
            status="cooldown",
            cooldown_until=cooldown_until,
            recent_failures=previous.recent_failures + 1,
            recent_successes=previous.recent_successes,
            last_status_code=status_code,
            last_challenge_reason=challenge_reason,
            last_used_at=observed_at_ms,
            avg_latency_ms=previous.avg_latency_ms,
        )
        self._write(state)
        self.backend.zadd(self.cooldown_zset_key, {proxy_id: float(cooldown_until)})
        return state

    def record_success(self, proxy_id: str, *, observed_at_ms: int, latency_ms: int | None) -> ProxyState:
        previous = self.load(proxy_id)
        avg_latency = _average_latency(previous.avg_latency_ms, latency_ms)
        state = ProxyState(
            proxy_id=proxy_id,
            status="available",
            cooldown_until=None,
            recent_failures=previous.recent_failures,
            recent_successes=previous.recent_successes + 1,
            last_status_code=200,
            last_challenge_reason=None,
            last_used_at=observed_at_ms,
            avg_latency_ms=avg_latency,
        )
        self._write(state)
        self.backend.zrem(self.cooldown_zset_key, proxy_id)
        return state

    def is_available(self, proxy_id: str, *, now_ms: int) -> bool:
        state = self.load(proxy_id)
        if state.cooldown_until is None:
            return True
        return now_ms >= state.cooldown_until

    def due_for_release(self, *, now_ms: int) -> tuple[str, ...]:
        return tuple(self.backend.zrangebyscore(self.cooldown_zset_key, float("-inf"), float(now_ms)))

    def _write(self, state: ProxyState) -> None:
        _hset_mapping(
            self.backend,
            self._key(state.proxy_id),
            {
                "status": state.status,
                "cooldown_until": state.cooldown_until,
                "recent_failures": state.recent_failures,
                "recent_successes": state.recent_successes,
                "last_status_code": state.last_status_code,
                "last_challenge_reason": state.last_challenge_reason,
                "last_used_at": state.last_used_at,
                "avg_latency_ms": state.avg_latency_ms,
            },
        )

    @staticmethod
    def _key(proxy_id: str) -> str:
        return f"proxy:{proxy_id}"


def _maybe_int(value: object) -> int | None:
    if value in (None, ""):
        return None
    return int(value)


def _maybe_str(value: object) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _average_latency(previous: int | None, latest: int | None) -> int | None:
    if latest is None:
        return previous
    if previous is None:
        return int(latest)
    return int(round((previous + latest) / 2))


def _hset_mapping(backend: Any, name: str, mapping: dict[str, object]) -> None:
    serialized = {key: _redis_scalar(value) for key, value in mapping.items()}
    try:
        backend.hset(name, mapping=serialized)
    except TypeError:
        backend.hset(name, serialized)


def _redis_scalar(value: object) -> object:
    return "" if value is None else value
