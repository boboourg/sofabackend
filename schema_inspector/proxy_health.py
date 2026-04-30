"""Health probes for configured proxy pools."""

from __future__ import annotations

import time
from dataclasses import dataclass, replace
from typing import Callable, Literal

from .runtime import ProxyEndpoint, RuntimeConfig, load_runtime_config, load_structure_runtime_config
from .transport import InspectorTransport


ProxyPoolName = Literal["live", "historical", "structure"]
TransportFactory = Callable[[RuntimeConfig], InspectorTransport]

DEFAULT_HEALTH_URL = "https://www.sofascore.com/api/v1/sport/football/events/live"


@dataclass(frozen=True)
class ProxyHealthResult:
    pool: str
    proxy_name: str
    proxy_url: str
    verdict: str
    status_code: int | None
    challenge_reason: str | None
    elapsed_seconds: float
    attempts: int
    error: str | None = None


def load_pool_runtime_config(pool: ProxyPoolName, *, env: dict[str, str] | None = None) -> RuntimeConfig:
    normalized = str(pool).strip().lower()
    if normalized == "live":
        return load_runtime_config(env=env)
    if normalized == "historical":
        return load_runtime_config(env=env, proxy_env_key="SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS")
    if normalized == "structure":
        return load_structure_runtime_config(env=env)
    raise ValueError(f"Unsupported proxy pool: {pool}")


async def probe_proxy_endpoint(
    *,
    endpoint: ProxyEndpoint,
    base_config: RuntimeConfig,
    url: str = DEFAULT_HEALTH_URL,
    timeout: float = 15.0,
    pool: str = "live",
    transport_factory: TransportFactory = InspectorTransport,
) -> ProxyHealthResult:
    single_proxy_config = replace(
        base_config,
        proxy_endpoints=(endpoint,),
        proxy_request_cooldown_seconds=0.0,
        proxy_request_jitter_seconds=0.0,
        retry_policy=replace(
            base_config.retry_policy,
            max_attempts=1,
            challenge_max_attempts=1,
            network_error_max_attempts=1,
            backoff_seconds=0.0,
        ),
    )
    transport = transport_factory(single_proxy_config)
    started = time.monotonic()
    try:
        result = await transport.fetch(url, timeout=timeout)
    except Exception as exc:
        elapsed = time.monotonic() - started
        return ProxyHealthResult(
            pool=pool,
            proxy_name=endpoint.name,
            proxy_url=endpoint.url,
            verdict="dead",
            status_code=None,
            challenge_reason=None,
            elapsed_seconds=elapsed,
            attempts=0,
            error=str(exc),
        )
    finally:
        await transport.close()

    elapsed = time.monotonic() - started
    verdict = "healthy" if result.status_code == 200 and result.challenge_reason is None else "dead"
    return ProxyHealthResult(
        pool=pool,
        proxy_name=endpoint.name,
        proxy_url=endpoint.url,
        verdict=verdict,
        status_code=result.status_code,
        challenge_reason=result.challenge_reason,
        elapsed_seconds=elapsed,
        attempts=len(result.attempts),
        error=None,
    )
