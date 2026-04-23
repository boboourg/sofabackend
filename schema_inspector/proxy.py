"""Proxy pool for schema inspector requests."""

from __future__ import annotations

import asyncio
import random
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
    in_use: bool = False

    def available(self, now: float) -> bool:
        return (not self.in_use) and now >= self.cooldown_until


class ProxyLease:
    """An acquired proxy slot that must be released after the request completes."""

    def __init__(self, pool: ProxyPool, state: _ProxyState, *, pre_request_delay: float) -> None:
        self._pool = pool
        self._state = state
        self.endpoint = state.endpoint
        self.pre_request_delay = max(0.0, float(pre_request_delay))
        self._released = False

    async def release(self, *, success: bool) -> None:
        if self._released:
            return
        self._released = True
        await self._pool._release(self._state, success=success)


class ProxyPool:
    """Round-robin proxy pool with per-proxy serialization and cooldowns."""

    def __init__(
        self,
        endpoints: tuple[ProxyEndpoint, ...],
        *,
        clock=None,
        default_success_cooldown_seconds: float = 1.5,
        jitter_seconds: float = 1.0,
        randomizer: random.Random | None = None,
    ) -> None:
        self._clock = clock or time.monotonic
        self._states = [_ProxyState(endpoint=item) for item in endpoints]
        self._by_name = {state.endpoint.name: state for state in self._states}
        self._cursor = 0
        self._success_cooldown_seconds = max(0.0, float(default_success_cooldown_seconds))
        self._jitter_seconds = max(0.0, float(jitter_seconds))
        self._random = randomizer or random.Random()
        self._condition = asyncio.Condition()

    async def acquire(self) -> ProxyLease | None:
        if not self._states:
            return None

        while True:
            async with self._condition:
                lease = self._try_acquire_locked()
                if lease is not None:
                    return lease
                delay = self._next_available_delay_locked()
                if delay is None:
                    await self._condition.wait()
                else:
                    try:
                        await asyncio.wait_for(self._condition.wait(), timeout=delay)
                    except TimeoutError:
                        pass

    def try_acquire_nowait(self) -> ProxyLease | None:
        if not self._states:
            return None
        return self._try_acquire()

    def next_available_delay(self) -> float | None:
        if not self._states:
            return None
        return self._next_available_delay()

    def record_success(self, proxy_name: str) -> None:
        state = self._by_name[proxy_name]
        now = self._clock()
        state.success_count += 1
        state.consecutive_failures = 0
        state.cooldown_until = max(state.cooldown_until, now + self._post_use_delay())

    def record_failure(self, proxy_name: str) -> None:
        state = self._by_name[proxy_name]
        now = self._clock()
        state.failure_count += 1
        state.consecutive_failures += 1
        state.cooldown_until = max(
            state.cooldown_until,
            now + max(state.endpoint.cooldown_seconds, self._post_use_delay()),
        )

    async def _release(self, state: _ProxyState, *, success: bool) -> None:
        async with self._condition:
            state.in_use = False
            if success:
                self.record_success(state.endpoint.name)
            else:
                self.record_failure(state.endpoint.name)
            self._condition.notify_all()

    def _try_acquire(self) -> ProxyLease | None:
        now = self._clock()
        for offset in range(len(self._states)):
            index = (self._cursor + offset) % len(self._states)
            state = self._states[index]
            if state.available(now):
                state.in_use = True
                self._cursor = (index + 1) % len(self._states)
                return ProxyLease(self, state, pre_request_delay=self._request_jitter_delay())
        return None

    def _try_acquire_locked(self) -> ProxyLease | None:
        return self._try_acquire()

    def _next_available_delay(self) -> float | None:
        now = self._clock()
        delays: list[float] = []
        has_in_use = False
        for state in self._states:
            if state.in_use:
                has_in_use = True
                continue
            delays.append(max(0.0, state.cooldown_until - now))
        if delays:
            return min(delays)
        if has_in_use:
            return None
        return None

    def _next_available_delay_locked(self) -> float | None:
        return self._next_available_delay()

    def _post_use_delay(self) -> float:
        return self._success_cooldown_seconds + self._request_jitter_delay()

    def _request_jitter_delay(self) -> float:
        if self._jitter_seconds <= 0.0:
            return 0.0
        return float(self._random.uniform(0.0, self._jitter_seconds))
