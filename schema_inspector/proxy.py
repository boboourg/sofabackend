"""Proxy pool for schema inspector requests."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from urllib.parse import urlparse

from .runtime import ProxyEndpoint

logger = logging.getLogger(__name__)


@dataclass
class _ProxyState:
    """Per-endpoint state inside ``ProxyPool``.

    Concurrency model: ``in_use_count`` tracks the number of currently
    leased ``ProxyLease`` instances for this endpoint, capped at
    ``max_in_use``. With the default ``max_in_use=1`` the pool behaves
    exactly like the historical single-flight model (one bool flag,
    one lease per endpoint at a time). With ``max_in_use>1`` (Variant
    A-lite for Smartproxy gateway-style providers), several concurrent
    fetch tasks can share the same proxy_url credential — Smartproxy
    handles the rotating-exit-IP responsibility upstream, so each
    concurrent CONNECT-tunnel still gets a distinct exit IP.

    Failure cooldown semantics intentionally unchanged: ``cooldown_until``
    blocks ALL future acquires on this endpoint (regardless of
    ``in_use_count``) for ``cooldown_seconds`` after a failure.
    Documented risk: at ``max_in_use=4`` a single transient failure
    parks all 4 slots for the cooldown window. If that proves too
    aggressive in production, a separate
    ``SCHEMA_INSPECTOR_PROXY_FAILURE_COOLDOWN_SECONDS`` env knob can be
    added in a follow-up patch.
    """

    endpoint: ProxyEndpoint
    cooldown_until: float = 0.0
    failure_count: int = 0
    success_count: int = 0
    consecutive_failures: int = 0
    in_use_count: int = 0
    max_in_use: int = 1

    def available(self, now: float) -> bool:
        return (self.in_use_count < self.max_in_use) and now >= self.cooldown_until


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
        proxy_state_store=None,
        proxy_health_cache_ttl_seconds: float = 5.0,
        now_ms_factory=None,
        max_in_use_per_endpoint: int = 1,
    ) -> None:
        self._clock = clock or time.monotonic
        # Variant A-lite: cap concurrent leases per endpoint. Default
        # max_in_use_per_endpoint=1 preserves the legacy single-flight
        # semantics exactly. >1 unlocks several concurrent CONNECT
        # tunnels through the same Smartproxy gateway credential
        # (Smartproxy issues independent rotating exit IPs per tunnel).
        self._max_in_use_per_endpoint = max(1, int(max_in_use_per_endpoint))
        self._states = [
            _ProxyState(endpoint=item, max_in_use=self._max_in_use_per_endpoint)
            for item in endpoints
        ]
        self._by_name = {state.endpoint.name: state for state in self._states}
        self._cursor = 0
        self._success_cooldown_seconds = max(0.0, float(default_success_cooldown_seconds))
        self._jitter_seconds = max(0.0, float(jitter_seconds))
        self._random = randomizer or random.Random()
        self._proxy_state_store = proxy_state_store
        self._proxy_health_cache_ttl_seconds = max(0.0, float(proxy_health_cache_ttl_seconds))
        self._proxy_health_cache: dict[str, tuple[bool, float]] = {}
        self._now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
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
            # Defensive against double-release: clamp at 0 so that an
            # accidental second ``release()`` call on the same lease
            # cannot drift in_use_count negative and silently allow an
            # extra concurrent acquire.
            state.in_use_count = max(0, state.in_use_count - 1)
            if success:
                self.record_success(state.endpoint.name)
            else:
                self.record_failure(state.endpoint.name)
                # Smartproxy session-expanded slot (Variant B, currently
                # NOT enabled in production after HTTP 612 from upstream
                # — see commit history): rotate session id on failure to
                # replace the bad exit IP. No-op for non-session-expanded
                # endpoints, which is the case for both the legacy
                # single-flight model and Variant A-lite (max_in_use>1
                # without session ids).
                state.endpoint.regenerate_session()
            self._condition.notify_all()

    def _try_acquire(self) -> ProxyLease | None:
        now = self._clock()
        for offset in range(len(self._states)):
            index = (self._cursor + offset) % len(self._states)
            state = self._states[index]
            # ``available`` already gates on (in_use_count < max_in_use)
            # AND (now >= cooldown_until), so the same loop body works
            # for both default (max_in_use=1, single-flight) and
            # Variant A-lite (max_in_use>1, several concurrent leases
            # per endpoint).
            if not state.available(now):
                continue
            health_available, _ = self._health_status(state.endpoint, now=now)
            if health_available:
                state.in_use_count += 1
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
            if state.in_use_count >= state.max_in_use:
                # Endpoint is saturated. We cannot estimate when a lease
                # will release (no scheduled time) — caller waits on the
                # condition's notify_all().
                has_in_use = True
                continue
            local_delay = max(0.0, state.cooldown_until - now)
            if local_delay > 0.0:
                delays.append(local_delay)
                continue
            health_available, health_delay = self._health_status(state.endpoint, now=now)
            delays.append(0.0 if health_available else health_delay)
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

    def _health_status(self, endpoint: ProxyEndpoint, *, now: float) -> tuple[bool, float]:
        if self._proxy_state_store is None:
            return True, 0.0
        proxy_address = _proxy_address_from_url(endpoint.url)
        if proxy_address is None:
            return True, 0.0
        cached = self._proxy_health_cache.get(proxy_address)
        if cached is not None:
            available, expires_at = cached
            if now < expires_at:
                return available, max(0.0, expires_at - now)
        try:
            available = bool(self._proxy_state_store.is_available(proxy_address, now_ms=int(self._now_ms_factory())))
        except Exception as exc:  # pragma: no cover - fail-open guard
            logger.warning("Proxy health state lookup failed for %s: %s", proxy_address, exc)
            available = True
        expires_at = now + self._proxy_health_cache_ttl_seconds
        self._proxy_health_cache[proxy_address] = (available, expires_at)
        return available, 0.0 if available else max(0.0, expires_at - now)


def _proxy_address_from_url(proxy_url: str | None) -> str | None:
    if not proxy_url:
        return None
    parsed = urlparse(proxy_url)
    host = parsed.hostname
    if not host:
        return None
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    return f"{host}:{parsed.port}" if parsed.port is not None else host
