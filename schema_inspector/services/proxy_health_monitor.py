"""Traffic-based proxy health monitor."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Any, Mapping

from ..storage.proxy_health_repository import ProxyHealthRepository, ProxyTrafficAggregate

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProxyHealthMonitorConfig:
    enabled: bool = True
    interval_seconds: float = 60.0
    window_seconds: int = 3600
    min_requests: int = 20
    success_rate_threshold: float = 0.05
    unhealthy_ttl_seconds: int = 1800
    excluded_address_substrings: tuple[str, ...] = ("smartproxy", "proxy.smartproxy.net")

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "ProxyHealthMonitorConfig":
        return cls(
            enabled=_env_bool(env, "SOFASCORE_PROXY_HEALTH_ENABLED", True),
            interval_seconds=_env_float(env, "SOFASCORE_PROXY_HEALTH_INTERVAL_SECONDS", 60.0),
            window_seconds=_env_int(env, "SOFASCORE_PROXY_HEALTH_WINDOW_SECONDS", 3600),
            min_requests=_env_int(env, "SOFASCORE_PROXY_HEALTH_MIN_REQUESTS", 20),
            success_rate_threshold=_env_float(env, "SOFASCORE_PROXY_HEALTH_SUCCESS_RATE_THRESHOLD", 0.05),
            unhealthy_ttl_seconds=_env_int(env, "SOFASCORE_PROXY_HEALTH_UNHEALTHY_TTL_SECONDS", 1800),
            excluded_address_substrings=_env_csv(
                env,
                "SOFASCORE_PROXY_HEALTH_EXCLUDED_ADDRESS_SUBSTRINGS",
                ("smartproxy", "proxy.smartproxy.net"),
            ),
        )


@dataclass(frozen=True)
class ProxyHealthMonitorReport:
    observed: int
    below_sample: int
    excluded: int
    healthy: int
    marked_unhealthy: int


class ProxyHealthMonitor:
    def __init__(
        self,
        *,
        repository: ProxyHealthRepository,
        state_store: Any,
        config: ProxyHealthMonitorConfig,
        now_ms_factory=None,
        sleeper=None,
    ) -> None:
        self.repository = repository
        self.state_store = state_store
        self.config = config
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.sleeper = sleeper or asyncio.sleep

    async def run_once(self, connection_context) -> ProxyHealthMonitorReport:
        if not self.config.enabled:
            return ProxyHealthMonitorReport(observed=0, below_sample=0, excluded=0, healthy=0, marked_unhealthy=0)
        async with connection_context as connection:
            aggregates = await self.repository.fetch_proxy_traffic(
                connection,
                window_seconds=self.config.window_seconds,
            )
        return self._apply(aggregates)

    async def run_forever(self, connection_factory) -> None:
        while True:
            try:
                report = await self.run_once(connection_factory())
                logger.info(
                    "proxy-health tick observed=%s below_sample=%s excluded=%s healthy=%s marked_unhealthy=%s",
                    report.observed,
                    report.below_sample,
                    report.excluded,
                    report.healthy,
                    report.marked_unhealthy,
                )
            except Exception:
                logger.exception("proxy-health tick failed")
            await self.sleeper(max(1.0, float(self.config.interval_seconds)))

    def _apply(self, aggregates: tuple[ProxyTrafficAggregate, ...]) -> ProxyHealthMonitorReport:
        below_sample = 0
        excluded = 0
        healthy = 0
        marked_unhealthy = 0
        now_ms = int(self.now_ms_factory())
        cooldown_ms = int(self.config.unhealthy_ttl_seconds) * 1000
        for aggregate in aggregates:
            if self._is_excluded(aggregate.proxy_address):
                excluded += 1
                continue
            if aggregate.total_requests < self.config.min_requests:
                below_sample += 1
                continue
            if aggregate.success_rate < self.config.success_rate_threshold:
                self.state_store.record_failure(
                    aggregate.proxy_address,
                    status_code=_representative_status_code(aggregate),
                    challenge_reason="traffic_unhealthy",
                    observed_at_ms=now_ms,
                    cooldown_ms=cooldown_ms,
                )
                marked_unhealthy += 1
                continue
            healthy += 1
        return ProxyHealthMonitorReport(
            observed=len(aggregates),
            below_sample=below_sample,
            excluded=excluded,
            healthy=healthy,
            marked_unhealthy=marked_unhealthy,
        )

    def _is_excluded(self, proxy_address: str) -> bool:
        normalized = proxy_address.lower()
        return any(item and item.lower() in normalized for item in self.config.excluded_address_substrings)


def _representative_status_code(aggregate: ProxyTrafficAggregate) -> int | None:
    candidates = (
        (aggregate.status_403_count, 403),
        (aggregate.status_404_count, 404),
        (aggregate.status_5xx_count, 500),
    )
    count, status_code = max(candidates, key=lambda item: item[0])
    return status_code if count > 0 else None


def _env_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _env_int(env: Mapping[str, str], name: str, default: int) -> int:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_csv(env: Mapping[str, str], name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    raw = env.get(name)
    if raw is None or raw == "":
        return default
    return tuple(item.strip() for item in raw.split(",") if item.strip())
