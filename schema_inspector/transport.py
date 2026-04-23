"""Unified transport layer used by all schema inspector requests.
Now powered by curl_cffi for Stealth Mode (Cloudflare/Akamai bypass)."""

from __future__ import annotations

import asyncio
import inspect
import sys
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


if sys.platform == "win32":
    current_policy = asyncio.get_event_loop_policy()
    if not isinstance(current_policy, asyncio.WindowsSelectorEventLoopPolicy):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


@dataclass(frozen=True)
class _RawResponse:
    resolved_url: str
    status_code: int
    headers: Mapping[str, str]
    body_bytes: bytes


class ProxyRequiredError(RuntimeError):
    """Raised when proxy-only mode is enabled and no proxy can be used."""


class InspectorTransport:
    """All outbound requests for the inspector pass through this class."""

    def __init__(self, runtime_config: RuntimeConfig, *, sleeper=None, clock=None) -> None:
        self.runtime_config = runtime_config
        self.sleeper = sleeper or asyncio.sleep
        self.clock = clock or time.monotonic
        self.proxy_pool = ProxyPool(runtime_config.proxy_endpoints, clock=self.clock)
        self._session_cache: dict[str, AsyncSession] = {}
        self._session_lock = asyncio.Lock()
        # ETag cache: url -> (etag_value, last_body_bytes).
        # Populated on 200 responses that carry an ETag header.
        # Used to send If-None-Match on subsequent requests; on 304 we return
        # the cached body transparently — zero proxy bytes consumed.
        self._etag_cache: dict[str, tuple[str, bytes]] = {}

    async def fetch(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        timeout: float = 20.0,
    ) -> TransportResult:
        parsed = urlparse(url)
        proxy_required = parsed.scheme in {"http", "https"}
        request_headers = dict(self.runtime_config.default_headers)
        if self.runtime_config.user_agent:
            request_headers["User-Agent"] = self.runtime_config.user_agent

        if headers:
            request_headers.update(headers)

        # Inject If-None-Match if we have a cached ETag for this URL.
        cached_etag_entry = self._etag_cache.get(url)
        if cached_etag_entry is not None:
            request_headers["If-None-Match"] = cached_etag_entry[0]

        attempts = []
        max_attempt_budget = max(
            int(self.runtime_config.retry_policy.max_attempts),
            int(self.runtime_config.retry_policy.challenge_max_attempts),
            int(self.runtime_config.retry_policy.network_error_max_attempts),
        )
        for attempt_number in range(1, max_attempt_budget + 1):
            proxy = self.proxy_pool.acquire()
            while proxy_required and proxy is None and self.runtime_config.proxy_endpoints:
                wait_seconds = max(
                    self.proxy_pool.next_available_delay() or 0.0,
                    self.runtime_config.retry_policy.backoff_seconds,
                )
                await self._sleep(wait_seconds)
                proxy = self.proxy_pool.acquire()

            proxy_name = proxy.name if proxy is not None else None
            proxy_url = proxy.url if proxy is not None else None
            if proxy_required and proxy_url is None:
                error_msg = self._proxy_required_message()
                attempts.append(
                    TransportAttempt(
                        attempt_number=attempt_number,
                        proxy_name=None,
                        status_code=None,
                        error=error_msg,
                        challenge_reason=None,
                    )
                )
                raise ProxyRequiredError(error_msg)

            try:
                raw = await self._execute_once(url, request_headers, timeout, proxy_url)

                # 304 Not Modified — data hasn't changed, return cached body.
                # Proxy transferred only request headers, no response body.
                if raw.status_code == 304 and cached_etag_entry is not None:
                    if proxy_name is not None:
                        self.proxy_pool.record_success(proxy_name)
                    attempts.append(
                        TransportAttempt(
                            attempt_number=attempt_number,
                            proxy_name=proxy_name,
                            status_code=304,
                            error=None,
                            challenge_reason=None,
                        )
                    )
                    return TransportResult(
                        resolved_url=raw.resolved_url,
                        status_code=200,  # surface as 200 so upstream pipeline is unaffected
                        headers=raw.headers,
                        body_bytes=cached_etag_entry[1],
                        attempts=tuple(attempts),
                        final_proxy_name=proxy_name,
                        challenge_reason=None,
                    )

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
                    (
                        raw.status_code in self.runtime_config.retry_policy.retry_status_codes
                        or challenge_reason is not None
                    )
                    and attempt_number < self._response_attempt_budget(
                        status_code=raw.status_code,
                        challenge_reason=challenge_reason,
                    )
                )
                if should_retry:
                    if proxy_name is not None:
                        self.proxy_pool.record_failure(proxy_name)
                    await self._sleep(self.runtime_config.retry_policy.backoff_seconds * attempt_number)
                    continue

                if proxy_name is not None:
                    if self._should_cooldown_proxy(raw.status_code, challenge_reason):
                        self.proxy_pool.record_failure(proxy_name)
                    else:
                        self.proxy_pool.record_success(proxy_name)

                # Store ETag from a successful 200 response for future requests.
                if raw.status_code == 200 and raw.body_bytes:
                    etag = _extract_etag(raw.headers)
                    if etag:
                        self._etag_cache[url] = (etag, raw.body_bytes)

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
                await self._discard_session(proxy_url)
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
                if attempt_number < self._network_attempt_budget():
                    await self._sleep(self.runtime_config.retry_policy.backoff_seconds * attempt_number)
                    continue
                raise

        raise RuntimeError("Transport exhausted without a final response.")

    def _proxy_required_message(self) -> str:
        if not self.runtime_config.proxy_endpoints:
            return "Proxy-only mode is enabled, but no proxies are configured."
        return "Proxy-only mode is enabled, but no proxy is currently available."

    def _should_cooldown_proxy(self, status_code: int, challenge_reason: str | None) -> bool:
        if challenge_reason is not None:
            return True
        if status_code in self.runtime_config.retry_policy.retry_status_codes:
            return True
        return status_code >= 500

    def _response_attempt_budget(self, *, status_code: int, challenge_reason: str | None) -> int:
        budget = int(self.runtime_config.retry_policy.max_attempts)
        if challenge_reason is not None or status_code in {403, 429}:
            budget = max(budget, int(self.runtime_config.retry_policy.challenge_max_attempts))
        return max(1, budget)

    def _network_attempt_budget(self) -> int:
        return max(
            1,
            int(self.runtime_config.retry_policy.max_attempts),
            int(self.runtime_config.retry_policy.network_error_max_attempts),
        )

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
        session = await self._get_session(proxy_url)
        response = await session.get(
            url,
            headers=dict(headers),
            timeout=timeout,
        )

        return _RawResponse(
            resolved_url=response.url,
            status_code=response.status_code,
            headers=dict(response.headers),
            body_bytes=response.content,
        )

    async def close(self) -> None:
        async with self._session_lock:
            sessions = list(self._session_cache.values())
            self._session_cache.clear()
        for session in sessions:
            await self._close_session(session)

    async def _sleep(self, delay: float) -> None:
        maybe_awaitable = self.sleeper(delay)
        if inspect.isawaitable(maybe_awaitable):
            await maybe_awaitable

    async def _get_session(self, proxy_url: str | None) -> AsyncSession:
        session_key = self._session_key(proxy_url)
        cached = self._session_cache.get(session_key)
        if cached is not None:
            return cached

        async with self._session_lock:
            cached = self._session_cache.get(session_key)
            if cached is not None:
                return cached
            session = AsyncSession(**self._session_kwargs(proxy_url))
            self._session_cache[session_key] = session
            return session

    async def _discard_session(self, proxy_url: str | None) -> None:
        session_key = self._session_key(proxy_url)
        async with self._session_lock:
            session = self._session_cache.pop(session_key, None)
        if session is not None:
            await self._close_session(session)

    async def _close_session(self, session: AsyncSession) -> None:
        close = getattr(session, "close", None)
        if close is None:
            return
        maybe_awaitable = close()
        if inspect.isawaitable(maybe_awaitable):
            await maybe_awaitable

    def _session_key(self, proxy_url: str | None) -> str:
        return proxy_url or "__direct__"

    def _session_kwargs(self, proxy_url: str | None) -> dict[str, object]:
        tls_policy = getattr(self.runtime_config, "tls_policy", None)
        impersonate_profile = getattr(tls_policy, "impersonate", "chrome120")
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        kwargs: dict[str, object] = {
            "max_clients": max(20, len(self.runtime_config.proxy_endpoints) or 1),
            "impersonate": impersonate_profile,
        }
        if proxies is not None:
            kwargs["proxies"] = proxies
        return kwargs

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


def _extract_etag(headers: Mapping[str, str]) -> str | None:
    """Return the ETag value from response headers, case-insensitively."""
    for key, value in headers.items():
        if key.lower() == "etag":
            return value.strip()
    return None
