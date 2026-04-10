"""Unified transport layer used by all schema inspector requests.
Now powered by curl_cffi for Stealth Mode (Cloudflare/Akamai bypass)."""

from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass
from typing import Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from curl_cffi.requests import AsyncSession, RequestsError

from .challenge import detect_challenge
from .proxy import ProxyPool
from .runtime import RuntimeConfig, TransportAttempt, TransportResult


@dataclass(frozen=True)
class _RawResponse:
    resolved_url: str
    status_code: int
    headers: Mapping[str, str]
    body_bytes: bytes


class InspectorTransport:
    """All outbound requests for the inspector pass through this class."""

    def __init__(self, runtime_config: RuntimeConfig, *, sleeper=None, clock=None) -> None:
        self.runtime_config = runtime_config
        self.sleeper = sleeper or asyncio.sleep
        self.clock = clock or time.monotonic
        self.proxy_pool = ProxyPool(runtime_config.proxy_endpoints, clock=self.clock)

    async def fetch(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        timeout: float = 20.0,
    ) -> TransportResult:
        request_headers = dict(self.runtime_config.default_headers)
        if self.runtime_config.user_agent:
            request_headers["User-Agent"] = self.runtime_config.user_agent

        if headers:
            request_headers.update(headers)

        attempts = []
        for attempt_number in range(1, self.runtime_config.retry_policy.max_attempts + 1):
            proxy = self.proxy_pool.acquire()
            proxy_name = proxy.name if proxy is not None else None
            proxy_url = proxy.url if proxy is not None else None

            try:
                raw = await self._execute_once(url, request_headers, timeout, proxy_url)
                challenge_reason = detect_challenge(
                    raw.status_code,
                    raw.headers,
                    raw.body_bytes,
                    self.runtime_config.challenge_markers,
                )
                attempts.append(
                    TransportAttempt(
                        attempt_number=attempt_number,
                        proxy_name=proxy_name,
                        status_code=raw.status_code,
                        error=None,
                        challenge_reason=challenge_reason,
                    )
                )

                should_retry = (
                    raw.status_code in self.runtime_config.retry_policy.retry_status_codes
                    and attempt_number < self.runtime_config.retry_policy.max_attempts
                )
                if should_retry:
                    if proxy_name is not None:
                        self.proxy_pool.record_failure(proxy_name)
                    await self._sleep(self.runtime_config.retry_policy.backoff_seconds * attempt_number)
                    continue

                if raw.status_code < 400 and proxy_name is not None:
                    self.proxy_pool.record_success(proxy_name)
                elif raw.status_code >= 400 and proxy_name is not None:
                    self.proxy_pool.record_failure(proxy_name)

                return TransportResult(
                    resolved_url=raw.resolved_url,
                    status_code=raw.status_code,
                    headers=raw.headers,
                    body_bytes=raw.body_bytes,
                    attempts=tuple(attempts),
                    final_proxy_name=proxy_name,
                    challenge_reason=challenge_reason,
                )

            except (URLError, RequestsError) as exc:
                error_msg = str(getattr(exc, "reason", exc))
                attempts.append(
                    TransportAttempt(
                        attempt_number=attempt_number,
                        proxy_name=proxy_name,
                        status_code=None,
                        error=error_msg,
                        challenge_reason=None,
                    )
                )
                if proxy_name is not None:
                    self.proxy_pool.record_failure(proxy_name)
                if attempt_number < self.runtime_config.retry_policy.max_attempts:
                    await self._sleep(self.runtime_config.retry_policy.backoff_seconds * attempt_number)
                    continue
                raise

        raise RuntimeError("Transport exhausted without a final response.")

    async def _execute_once(
        self,
        url: str,
        headers: Mapping[str, str],
        timeout: float,
        proxy_url: str | None,
    ) -> _RawResponse:
        parsed = urlparse(url)

        # 1. Fallback for local files (curl_cffi doesn't do file://)
        if parsed.scheme == "file":
            return await asyncio.to_thread(self._execute_local_file_once, url, headers, timeout)

        # 2. STEALTH MODE for HTTP/HTTPS
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None

        # Safely extract impersonate profile, fallback to "chrome120" if not set in runtime.py yet
        tls_policy = getattr(self.runtime_config, "tls_policy", None)
        impersonate_profile = getattr(tls_policy, "impersonate", "chrome120")

        async with AsyncSession() as session:
            response = await session.get(
                url,
                headers=dict(headers),
                proxies=proxies,
                timeout=timeout,
                impersonate=impersonate_profile,
            )

        return _RawResponse(
            resolved_url=response.url,
            status_code=response.status_code,
            headers=dict(response.headers),
            body_bytes=response.content,
        )

    async def _sleep(self, delay: float) -> None:
        maybe_awaitable = self.sleeper(delay)
        if inspect.isawaitable(maybe_awaitable):
            await maybe_awaitable

    @staticmethod
    def _execute_local_file_once(
        url: str,
        headers: Mapping[str, str],
        timeout: float,
    ) -> _RawResponse:
        request = Request(url=url, headers=dict(headers), method="GET")
        try:
            with urlopen(request, timeout=timeout) as response:
                return _RawResponse(
                    resolved_url=response.geturl(),
                    status_code=getattr(response, "status", 200) or 200,
                    headers=dict(response.headers.items()),
                    body_bytes=response.read(),
                )
        except HTTPError as exc:
            return _RawResponse(
                resolved_url=exc.geturl(),
                status_code=exc.code,
                headers=dict(exc.headers.items()),
                body_bytes=exc.read(),
            )
