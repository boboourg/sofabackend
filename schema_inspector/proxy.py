"""Proxy pool for schema inspector requests."""

from __future__ import annotations

import time
from dataclasses import dataclass

from .runtime import ProxyEndpoint


@dataclass
class _ProxyState:
    endpoint: ProxyEndpoint
    cooldown_until: float = 0.0
    failure_count: int = 0
    success_count: int = 0
    consecutive_failures: int = 0

    def available(self, now: float) -> bool:
        return now >= self.cooldown_until


class ProxyPool:
    """Round-robin proxy pool with cooldown after failures."""

    def __init__(self, endpoints: tuple[ProxyEndpoint, ...], *, clock=None) -> None:
        self._clock = clock or time.monotonic
        self._states = [_ProxyState(endpoint=item) for item in endpoints]
        self._by_name = {state.endpoint.name: state for state in self._states}
        self._cursor = 0

    def acquire(self) -> ProxyEndpoint | None:
        if not self._states:
            return None

        now = self._clock()
        for offset in range(len(self._states)):
            index = (self._cursor + offset) % len(self._states)
            state = self._states[index]
            if state.available(now):
                self._cursor = (index + 1) % len(self._states)
                return state.endpoint
        return None

    def record_success(self, proxy_name: str) -> None:
        state = self._by_name[proxy_name]
        state.success_count += 1
        state.consecutive_failures = 0

    def record_failure(self, proxy_name: str) -> None:
        state = self._by_name[proxy_name]
        state.failure_count += 1
        state.consecutive_failures += 1
        state.cooldown_until = self._clock() + state.endpoint.cooldown_seconds
