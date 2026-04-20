"""Runtime configuration and transport metadata."""

from __future__ import annotations

import os
import ssl
from dataclasses import dataclass, field
from pathlib import Path
from typing import Mapping


@dataclass(frozen=True)
class ProxyEndpoint:
    """One proxy endpoint available to the inspector transport."""

    name: str
    url: str
    cooldown_seconds: float = 30.0


@dataclass(frozen=True)
class RetryPolicy:
    """Retry and backoff configuration."""

    max_attempts: int = 3
    backoff_seconds: float = 1.0
    retry_status_codes: tuple[int, ...] = (408, 429, 500, 502, 503, 504)


@dataclass(frozen=True)
class TlsPolicy:
    """Verified TLS policy for outbound HTTPS requests."""

    minimum_version: ssl.TLSVersion = ssl.TLSVersion.TLSv1_2
    maximum_version: ssl.TLSVersion | None = ssl.TLSVersion.TLSv1_3
    check_hostname: bool = True
    impersonate: str = "chrome110"


@dataclass(frozen=True)
class RuntimeConfig:
    """All network settings for schema inspector requests."""

    user_agent: str = "schema-inspector/1.0"
    require_proxy: bool = True
    default_headers: Mapping[str, str] = field(default_factory=dict)
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    tls_policy: TlsPolicy = field(default_factory=TlsPolicy)
    proxy_endpoints: tuple[ProxyEndpoint, ...] = ()
    challenge_markers: tuple[str, ...] = (
        "captcha",
        "g-recaptcha",
        "h-captcha",
        "cf-chl",
        "verify you are human",
        "attention required",
    )


@dataclass(frozen=True)
class TransportAttempt:
    """One fetch attempt."""

    attempt_number: int
    proxy_name: str | None
    status_code: int | None
    error: str | None
    challenge_reason: str | None


@dataclass(frozen=True)
class TransportResult:
    """Transport output with network metadata."""

    resolved_url: str
    status_code: int
    headers: Mapping[str, str]
    body_bytes: bytes
    attempts: tuple[TransportAttempt, ...]
    final_proxy_name: str | None
    challenge_reason: str | None


def load_runtime_config(
    *,
    env: Mapping[str, str] | None = None,
    proxy_urls: list[str] | None = None,
    proxy_env_key: str | None = None,
    user_agent: str | None = None,
    extra_headers: Mapping[str, str] | None = None,
    max_attempts: int | None = None,
) -> RuntimeConfig:
    """Build a runtime config from explicit arguments and environment.

    proxy_env_key: if set and proxy_urls is empty, reads proxy list from this
    env variable instead of the default SCHEMA_INSPECTOR_PROXY_URLS.
    Used to route historical workers to Proxyline without touching live proxies.
    """

    env = env or _load_project_env()
    configured_proxy_urls = list(proxy_urls or _read_proxy_urls(env, proxy_env_key=proxy_env_key))
    endpoints = tuple(
        ProxyEndpoint(name=f"proxy_{index + 1}", url=url.strip())
        for index, url in enumerate(configured_proxy_urls)
        if url.strip()
    )

    headers = {
        "Accept": "application/json, text/plain, */*",
        # Explicit compression request — ensures gzip/br regardless of curl_cffi
        # impersonation profile or library version.
        "Accept-Encoding": "gzip, deflate, br",
    }
    if extra_headers:
        headers.update(extra_headers)

    return RuntimeConfig(
        user_agent=user_agent or env.get("SCHEMA_INSPECTOR_USER_AGENT", "schema-inspector/1.0"),
        require_proxy=_env_bool(env, "SCHEMA_INSPECTOR_REQUIRE_PROXY", True),
        default_headers=headers,
        retry_policy=RetryPolicy(
            max_attempts=max_attempts or _env_int(env, "SCHEMA_INSPECTOR_MAX_ATTEMPTS", 3),
            backoff_seconds=_env_float(env, "SCHEMA_INSPECTOR_BACKOFF_SECONDS", 1.0),
        ),
        tls_policy=TlsPolicy(
            minimum_version=_env_tls(env.get("SCHEMA_INSPECTOR_TLS_MIN_VERSION"), ssl.TLSVersion.TLSv1_2),
            maximum_version=_env_tls(env.get("SCHEMA_INSPECTOR_TLS_MAX_VERSION"), ssl.TLSVersion.TLSv1_3),
            check_hostname=_env_bool(env, "SCHEMA_INSPECTOR_TLS_CHECK_HOSTNAME", True),
            impersonate=env.get("SCHEMA_INSPECTOR_TLS_IMPERSONATE", "chrome110").strip() or "chrome110",
        ),
        proxy_endpoints=endpoints,
    )


def _read_proxy_urls(env: Mapping[str, str], *, proxy_env_key: str | None = None) -> list[str]:
    # If a specific env key is requested (e.g. for historical workers), use it
    # exclusively so we never accidentally mix proxy pools.
    if proxy_env_key:
        joined = (env.get(proxy_env_key) or "").strip()
        return [item.strip() for item in joined.split(",") if item.strip()]

    # Default: read from the standard live/scheduled proxy variables.
    values = []
    single = (env.get("SCHEMA_INSPECTOR_PROXY_URL") or "").strip()
    if single:
        values.append(single)
    joined = (env.get("SCHEMA_INSPECTOR_PROXY_URLS") or "").strip()
    if joined:
        values.extend(item.strip() for item in joined.split(",") if item.strip())
    return values


def _load_project_env() -> dict[str, str]:
    merged = dict(os.environ)
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return merged

    for raw_line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        merged.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    return merged


def _env_bool(env: Mapping[str, str], name: str, default: bool) -> bool:
    value = env.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(env: Mapping[str, str], name: str, default: int) -> int:
    value = env.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(env: Mapping[str, str], name: str, default: float) -> float:
    value = env.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _env_tls(value: str | None, default: ssl.TLSVersion | None) -> ssl.TLSVersion | None:
    if value is None or not value.strip():
        return default

    normalized = value.strip().lower().replace("tls", "").replace("v", "").replace("_", ".")
    mapping = {
        "1.2": ssl.TLSVersion.TLSv1_2,
        "1.3": ssl.TLSVersion.TLSv1_3,
    }
    return mapping.get(normalized, default)
