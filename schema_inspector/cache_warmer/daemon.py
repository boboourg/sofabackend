"""CacheWarmerDaemon — tick loop that fetches hot URLs to populate cache.

Architecture mirrors :class:`MonitoringDaemon`:

* ``run_forever`` loops calling ``_tick``.
* ``_tick`` walks the target list, fetches each due target via HTTP,
  records timestamps so the next tick can decide when to refetch.

Single-process daemon. One systemd unit
(``sofascore-cache-warmer.service``) on prod is sufficient — its load
on the API server is ~3 req/sec, far below user traffic.

Resilience: HTTP / network errors are swallowed and logged. A failing
URL doesn't block other targets and doesn't crash the daemon.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from .config import CacheWarmerConfig
from .url_targets import (
    UrlTarget,
    build_url_targets,
    today_and_tomorrow_iso,
)


logger = logging.getLogger(__name__)


@dataclass
class TargetState:
    """Mutable per-target runtime state."""

    target: UrlTarget
    last_fetched_at_monotonic: float = -1.0
    last_status_code: int = 0
    last_duration_ms: float = 0.0
    total_fetches: int = 0
    total_failures: int = 0


@dataclass(frozen=True)
class TickReport:
    """Per-tick summary (returned by ``_tick``, used for testing)."""

    targets_due: int
    targets_fetched: int
    targets_failed: int


class CacheWarmerDaemon:
    """Periodic URL fetcher to populate the API response cache."""

    def __init__(
        self,
        *,
        config: CacheWarmerConfig,
        http_client: Any,
        targets: list[UrlTarget] | None = None,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        today_resolver: Callable[[], tuple[str, str]] = today_and_tomorrow_iso,
    ) -> None:
        self.config = config
        self.http_client = http_client
        self._clock = clock
        self._sleep = sleep
        self._today_resolver = today_resolver
        self._states: dict[str, TargetState] = {}
        if targets is None:
            today, tomorrow = today_resolver()
            targets = build_url_targets(
                sports=config.sports,
                today=today,
                tomorrow=tomorrow,
                live_interval_seconds=config.live_interval_seconds,
                scheduled_today_interval_seconds=config.scheduled_today_interval_seconds,
                scheduled_tomorrow_interval_seconds=config.scheduled_tomorrow_interval_seconds,
                scheduled_tournaments_interval_seconds=config.scheduled_tournaments_interval_seconds,
                warm_tomorrow_scheduled=config.warm_tomorrow_scheduled,
            )
        for target in targets:
            self._states[target.url] = TargetState(target=target)
        # Diagnostic counters.
        self.total_ticks = 0
        self.total_fetches = 0
        self.total_failures = 0

    async def run_forever(self) -> None:
        if not self.config.enabled:
            logger.info(
                "CacheWarmerDaemon: SOFASCORE_CACHE_WARMER_ENABLED=0, exiting"
            )
            return
        logger.info(
            "CacheWarmerDaemon: starting tick=%.1fs targets=%d sports=%d",
            self.config.tick_interval_seconds,
            len(self._states),
            len(self.config.sports),
        )
        try:
            while True:
                await self._tick()
                await self._sleep(self.config.tick_interval_seconds)
        except asyncio.CancelledError:
            logger.info("CacheWarmerDaemon: cancelled, exiting cleanly")
            raise

    async def _tick(self) -> TickReport:
        self.total_ticks += 1
        now = float(self._clock())
        due_targets = [
            state
            for state in self._states.values()
            if state.last_fetched_at_monotonic < 0
            or (now - state.last_fetched_at_monotonic) >= state.target.interval_seconds
        ]
        if not due_targets:
            return TickReport(targets_due=0, targets_fetched=0, targets_failed=0)
        fetched = 0
        failed = 0
        for state in due_targets:
            ok = await self._fetch_target(state)
            if ok:
                fetched += 1
                self.total_fetches += 1
            else:
                failed += 1
                self.total_failures += 1
            state.last_fetched_at_monotonic = float(self._clock())
        return TickReport(
            targets_due=len(due_targets),
            targets_fetched=fetched,
            targets_failed=failed,
        )

    async def _fetch_target(self, state: TargetState) -> bool:
        url = self.config.base_url.rstrip("/") + state.target.url
        start = self._clock()
        state.total_fetches += 1
        try:
            response = await self.http_client.get(
                url, timeout=self.config.request_timeout_seconds
            )
            state.last_status_code = int(getattr(response, "status_code", 0))
            state.last_duration_ms = (self._clock() - start) * 1000.0
            if state.last_status_code >= 500:
                state.total_failures += 1
                logger.warning(
                    "cache_warmer fetch %s returned status=%d",
                    state.target.url,
                    state.last_status_code,
                )
                return False
            return True
        except Exception as exc:  # noqa: BLE001 — never crash daemon
            state.total_failures += 1
            state.last_status_code = 0
            state.last_duration_ms = (self._clock() - start) * 1000.0
            logger.warning(
                "cache_warmer fetch %s failed: %r", state.target.url, exc
            )
            return False
