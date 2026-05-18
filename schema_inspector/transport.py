"""Unified transport layer used by all schema inspector requests.
Now powered by curl_cffi for Stealth Mode (Cloudflare/Akamai bypass)."""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import sys
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import certifi
from curl_cffi.requests import AsyncSession, RequestsError

from .challenge import detect_challenge
from .proxy import ProxyPool
from .queue.proxy_state import ProxyStateStore
from .runtime import FingerprintProfile, RuntimeConfig, TransportAttempt, TransportResult

logger = logging.getLogger(__name__)


_SESSION_TLS_FIELDS: tuple[str, ...] = (
    "verify",
    "http_version",
    "ja3",
    "akamai",
    "extra_fp",
)


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


@dataclass
class _SessionEntry:
    """One cached ``AsyncSession`` plus its refcount + tombstone flag.

    X' patch: introduced so that LRU eviction and discard-on-error can both
    avoid closing a session that is **currently being used** by another
    concurrent caller. Lifecycle:

    * Born with ``refcount=0`` immediately before being inserted into
      ``InspectorTransport._session_cache``; ``_acquire_session`` then bumps
      it to 1 and returns ``(session, entry)`` to the caller.
    * ``_release_session(entry)`` decrements ``refcount``. If the entry is
      already tombstoned and ``refcount`` reaches 0, the session is closed
      and the entry is removed from ``_pending_close``.
    * ``_discard_session(...)`` removes the entry from the active cache.
      If ``refcount > 0`` at that moment, the entry is appended to
      ``_pending_close`` (tombstone) and the close is deferred until
      every in-flight caller releases. Otherwise it is closed immediately.
    * LRU eviction in ``_acquire_session`` (when ``_session_cache_max > 0``)
      only considers entries with ``refcount == 0``; in-flight entries
      are skipped. If every entry is in-flight, the cache temporarily
      exceeds the cap (logged as a warning); the next idle entry on the
      next ``_acquire_session`` call will be evicted.
    """

    session: AsyncSession
    refcount: int = 0
    tombstoned: bool = False


class ProxyRequiredError(RuntimeError):
    """Raised when proxy-only mode is enabled and no proxy can be used."""


class TransportExhaustedError(RuntimeError):
    """Raised when the transport retry budget is consumed without a final response.

    Carries the per-attempt log so callers (FetchExecutor) can persist
    proxy/fingerprint/error/latency information into ``api_request_log``
    instead of recording an empty ``proxy=NULL`` row. This is purely a
    forensics improvement: the underlying transport behaviour, retry
    policy, timeouts, and proxy pool are unchanged.
    """

    def __init__(
        self,
        message: str,
        *,
        attempts: tuple[TransportAttempt, ...],
        final_proxy_name: str | None,
        final_proxy_address: str | None,
        original: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.attempts = tuple(attempts)
        self.final_proxy_name = final_proxy_name
        self.final_proxy_address = final_proxy_address
        self.original = original


class InspectorTransport:
    """All outbound requests for the inspector pass through this class."""

    def __init__(self, runtime_config: RuntimeConfig, *, sleeper=None, clock=None, proxy_state_store=None) -> None:
        self.runtime_config = runtime_config
        self.sleeper = sleeper or asyncio.sleep
        self.clock = clock or time.monotonic
        self.proxy_pool = ProxyPool(
            runtime_config.proxy_endpoints,
            clock=self.clock,
            default_success_cooldown_seconds=runtime_config.proxy_request_cooldown_seconds,
            jitter_seconds=runtime_config.proxy_request_jitter_seconds,
            max_in_use_per_endpoint=runtime_config.proxy_max_in_use_per_endpoint,
            proxy_state_store=proxy_state_store if proxy_state_store is not None else _load_proxy_state_store_from_env(),
        )
        # X' patch: ``OrderedDict`` preserves LRU semantics — ``move_to_end``
        # on cache hit, evict the oldest idle entry on insert when capped.
        # Wraps each session in ``_SessionEntry`` so the eviction algorithm
        # and ``_discard_session`` can both honour an in-flight refcount.
        # See ``_SessionEntry`` docstring for the lifecycle.
        self._session_cache: OrderedDict[str, _SessionEntry] = OrderedDict()
        # Tombstoned entries: removed from the active cache but still held
        # alive because at least one in-flight caller has them open. When
        # the final ``_release_session`` decrements refcount to 0, the
        # session is closed and the entry leaves this list.
        self._pending_close: list[_SessionEntry] = []
        # Cap on ``_session_cache`` size. 0 = unbounded (legacy no-op).
        # Read from ``runtime_config.session_cache_max_entries`` which itself
        # came from env via ``_resolve_session_cache_max_entries`` — see the
        # docstring on that resolver for the env priority order.
        self._session_cache_max: int = max(
            0, int(getattr(runtime_config, "session_cache_max_entries", 0) or 0)
        )
        self._session_lock = asyncio.Lock()
        self._fingerprint_lock = asyncio.Lock()
        self._fingerprint_cursor = 0
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
        method: str = "GET",
    ) -> TransportResult:
        parsed = urlparse(url)
        proxy_required = parsed.scheme in {"http", "https"}
        request_headers = dict(self.runtime_config.default_headers)

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
            lease = await self.proxy_pool.acquire()
            while proxy_required and lease is None and self.runtime_config.proxy_endpoints:
                wait_seconds = max(
                    self.proxy_pool.next_available_delay() or 0.0,
                    self.runtime_config.retry_policy.backoff_seconds,
                )
                await self._sleep(wait_seconds)
                lease = await self.proxy_pool.acquire()

            proxy_name = lease.endpoint.name if lease is not None else None
            proxy_url = lease.endpoint.url if lease is not None else None
            proxy_address = _proxy_address_from_url(proxy_url)
            if proxy_required and proxy_url is None:
                error_msg = self._proxy_required_message()
                attempts.append(
                    TransportAttempt(
                        attempt_number=attempt_number,
                        proxy_name=None,
                        status_code=None,
                        error=error_msg,
                        challenge_reason=None,
                        proxy_address=None,
                    )
                )
                raise ProxyRequiredError(error_msg)

            fingerprint_profile = await self._next_fingerprint_profile()
            attempt_started_monotonic = self.clock()
            try:
                attempt_headers = self._apply_fingerprint_headers(request_headers, fingerprint_profile)
                if lease is not None and lease.pre_request_delay > 0.0:
                    await self._sleep(lease.pre_request_delay)
                raw = await asyncio.wait_for(
                    self._execute_once(
                        url,
                        attempt_headers,
                        timeout,
                        proxy_url,
                        fingerprint_profile=fingerprint_profile,
                        method=method,
                    ),
                    timeout=timeout,
                )
                attempt_latency_ms = int((self.clock() - attempt_started_monotonic) * 1000)

                # 304 Not Modified — data hasn't changed, return cached body.
                # Proxy transferred only request headers, no response body.
                if raw.status_code == 304 and cached_etag_entry is not None:
                    if lease is not None:
                        await lease.release(success=True)
                    attempts.append(
                        TransportAttempt(
                            attempt_number=attempt_number,
                            proxy_name=proxy_name,
                            status_code=304,
                            error=None,
                            challenge_reason=None,
                            proxy_address=proxy_address,
                            latency_ms=attempt_latency_ms,
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
                        final_proxy_address=proxy_address,
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
                        proxy_address=proxy_address,
                        latency_ms=attempt_latency_ms,
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
                    if lease is not None:
                        await lease.release(success=False)
                    await self._sleep(self.runtime_config.retry_policy.backoff_seconds * attempt_number)
                    continue

                if lease is not None:
                    if self._should_cooldown_proxy(raw.status_code, challenge_reason):
                        await lease.release(success=False)
                    else:
                        await lease.release(success=True)

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
                    final_proxy_address=proxy_address,
                )

            except (URLError, RequestsError, asyncio.TimeoutError) as exc:
                attempt_latency_ms = int((self.clock() - attempt_started_monotonic) * 1000)
                error_msg = _transport_error_message(exc, timeout=timeout)
                await self._discard_session(proxy_url, fingerprint_profile=fingerprint_profile)
                attempts.append(
                    TransportAttempt(
                        attempt_number=attempt_number,
                        proxy_name=proxy_name,
                        status_code=None,
                        error=error_msg,
                        challenge_reason=None,
                        proxy_address=proxy_address,
                        latency_ms=attempt_latency_ms,
                    )
                )
                if lease is not None:
                    await lease.release(success=False)
                if attempt_number < self._network_attempt_budget():
                    await self._sleep(self.runtime_config.retry_policy.backoff_seconds * attempt_number)
                    continue
                # Transport budget exhausted on a network-level failure.
                # Wrap the original exception with the per-attempt log so
                # FetchExecutor can persist proxy/fingerprint/latency info
                # into api_request_log instead of recording an empty
                # proxy=NULL row.
                raise TransportExhaustedError(
                    error_msg,
                    attempts=tuple(attempts),
                    final_proxy_name=proxy_name,
                    final_proxy_address=proxy_address,
                    original=exc,
                ) from exc
            except Exception:
                if lease is not None:
                    await lease.release(success=False)
                raise

        raise TransportExhaustedError(
            "Transport exhausted without a final response.",
            attempts=tuple(attempts),
            final_proxy_name=None,
            final_proxy_address=None,
            original=None,
        )

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
        *,
        fingerprint_profile: FingerprintProfile | None = None,
        method: str = "GET",
    ) -> _RawResponse:
        parsed = urlparse(url)

        # 1. Fallback for local files (curl_cffi doesn't do file://)
        if parsed.scheme == "file":
            return await asyncio.to_thread(self._execute_local_file_once, url, headers, timeout)

        # 2. STEALTH MODE for HTTP/HTTPS — refcount-tracked acquire/release
        # (X' patch). The refcount holds the entry open for the duration of
        # this single HTTP call so that a concurrent ``_discard_session``
        # OR an LRU eviction triggered by another in-flight caller cannot
        # close the underlying session out from under us.
        session, entry = await self._acquire_session(
            proxy_url, fingerprint_profile=fingerprint_profile
        )
        try:
            if str(method or "GET").upper() == "HEAD":
                response = await session.head(
                    url,
                    headers=dict(headers),
                    timeout=timeout,
                )
            else:
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
        finally:
            await self._release_session(entry)

    async def close(self) -> None:
        """Close every cached session and drain the tombstone list.

        Used on shutdown — synchronous awaits are safe because no in-flight
        callers can race a shutdown path (the runtime stops dispatching new
        work before invoking ``close``).
        """
        async with self._session_lock:
            sessions = [entry.session for entry in self._session_cache.values()]
            sessions.extend(entry.session for entry in self._pending_close)
            self._session_cache.clear()
            self._pending_close.clear()
        for session in sessions:
            await self._close_session(session)

    async def _sleep(self, delay: float) -> None:
        maybe_awaitable = self.sleeper(delay)
        if inspect.isawaitable(maybe_awaitable):
            await maybe_awaitable

    async def _acquire_session(
        self,
        proxy_url: str | None,
        *,
        fingerprint_profile: FingerprintProfile | None = None,
    ) -> tuple[AsyncSession, _SessionEntry]:
        """Return ``(session, entry)`` for the (proxy_url, fingerprint) key.

        On cache hit: refresh LRU order and bump refcount on the existing
        entry. On cache miss: evict idle entries to make room for the cap
        (if set), then create a fresh ``AsyncSession`` and insert it with
        refcount=1.

        Callers MUST call :meth:`_release_session` on the returned entry
        exactly once when they are done using the session. ``_execute_once``
        wraps this in a try/finally — there is no other internal caller.
        """
        session_key = self._session_key(proxy_url, fingerprint_profile=fingerprint_profile)
        evicted_entries: list[_SessionEntry] = []
        async with self._session_lock:
            entry = self._session_cache.get(session_key)
            if entry is not None:
                # Cache hit: LRU-touch and bump refcount.
                self._session_cache.move_to_end(session_key)
                entry.refcount += 1
                return entry.session, entry
            # Cache miss: evict idle entries to make room when capped.
            if self._session_cache_max > 0:
                evicted_entries = self._collect_evictions_locked()
            new_session = AsyncSession(
                **self._session_kwargs(proxy_url, fingerprint_profile=fingerprint_profile)
            )
            new_entry = _SessionEntry(session=new_session, refcount=1)
            self._session_cache[session_key] = new_entry
        # Close evicted sessions outside the lock to avoid blocking other
        # acquirers on a slow curl close. Errors are swallowed to local logs.
        for evicted in evicted_entries:
            asyncio.create_task(self._close_session_swallow(evicted.session))
        return new_entry.session, new_entry

    def _collect_evictions_locked(self) -> list[_SessionEntry]:
        """Pop oldest IDLE entries (refcount==0) until size < cap.

        MUST be called with ``self._session_lock`` held. Skips in-flight
        entries; if every entry is in-flight, returns whatever was already
        evicted and allows the cache to temporarily exceed the cap (logged
        as a warning so the operator sees the pressure).
        """
        evicted: list[_SessionEntry] = []
        # +1 because we're about to insert a new entry; eviction must
        # produce room for that incoming entry, not just match the cap.
        while len(self._session_cache) >= self._session_cache_max:
            evict_key = None
            for candidate_key, candidate_entry in self._session_cache.items():
                if candidate_entry.refcount == 0:
                    evict_key = candidate_key
                    break
            if evict_key is None:
                logger.warning(
                    "Session cache cap exceeded — all entries in-flight; "
                    "over-cap allowed temporarily (cap=%d size=%d)",
                    self._session_cache_max,
                    len(self._session_cache),
                )
                return evicted
            evicted.append(self._session_cache.pop(evict_key))
        return evicted

    async def _release_session(self, entry: _SessionEntry) -> None:
        """Decrement refcount. If the entry is tombstoned and now idle, close it.

        Safe to call multiple times — refcount is clamped at 0. Errors from
        the underlying ``session.close()`` are swallowed (logged at DEBUG).
        """
        to_close: AsyncSession | None = None
        async with self._session_lock:
            entry.refcount = max(0, entry.refcount - 1)
            if entry.tombstoned and entry.refcount == 0:
                try:
                    self._pending_close.remove(entry)
                except ValueError:
                    pass
                to_close = entry.session
        if to_close is not None:
            await self._close_session_swallow(to_close)

    async def _discard_session(
        self,
        proxy_url: str | None,
        *,
        fingerprint_profile: FingerprintProfile | None = None,
    ) -> None:
        """Remove the cached session for this (proxy_url, fingerprint) key.

        If the entry is in-flight (refcount > 0), tombstone it: pop from the
        active cache, move to ``_pending_close``, and defer the actual close
        until the last in-flight caller releases. This prevents premature
        ``session.close()`` from racing concurrent ``session.get()`` calls
        that the caller had already started before the discard fired.
        """
        session_key = self._session_key(proxy_url, fingerprint_profile=fingerprint_profile)
        to_close: AsyncSession | None = None
        async with self._session_lock:
            entry = self._session_cache.pop(session_key, None)
            if entry is None:
                return
            if entry.refcount > 0:
                entry.tombstoned = True
                self._pending_close.append(entry)
            else:
                to_close = entry.session
        if to_close is not None:
            await self._close_session_swallow(to_close)

    async def _close_session(self, session: AsyncSession) -> None:
        close = getattr(session, "close", None)
        if close is None:
            return
        maybe_awaitable = close()
        if inspect.isawaitable(maybe_awaitable):
            await maybe_awaitable

    async def _close_session_swallow(self, session: AsyncSession) -> None:
        """Same as :meth:`_close_session` but swallows + logs any error.

        Used in fire-and-forget paths (eviction, tombstone close) where an
        unhandled exception would crash the asyncio loop or surface as an
        unrelated retry attempt's exception. The legacy synchronous
        ``_close_session`` is preserved for the shutdown ``close()`` path
        where errors should propagate to the supervising code.
        """
        try:
            await self._close_session(session)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug("Background session close failed: %s", exc, exc_info=True)

    async def _next_fingerprint_profile(self) -> FingerprintProfile | None:
        profiles = self.runtime_config.fingerprint_profiles
        if not profiles:
            return None
        async with self._fingerprint_lock:
            profile = profiles[self._fingerprint_cursor % len(profiles)]
            self._fingerprint_cursor += 1
            return profile

    def _apply_fingerprint_headers(
        self,
        headers: Mapping[str, str],
        fingerprint_profile: FingerprintProfile | None,
    ) -> dict[str, str]:
        request_headers = dict(headers)
        if fingerprint_profile is None:
            if self.runtime_config.user_agent:
                request_headers.setdefault("User-Agent", self.runtime_config.user_agent)
            return request_headers
        request_headers.setdefault("User-Agent", fingerprint_profile.user_agent)
        request_headers.setdefault("Accept-Language", fingerprint_profile.accept_language)
        request_headers.setdefault("Sec-Ch-Ua", fingerprint_profile.sec_ch_ua)
        request_headers.setdefault("Sec-Ch-Ua-Mobile", fingerprint_profile.sec_ch_ua_mobile)
        request_headers.setdefault("Sec-Ch-Ua-Platform", fingerprint_profile.sec_ch_ua_platform)
        request_headers.setdefault("Referer", fingerprint_profile.referer)
        return request_headers

    def _session_key(
        self,
        proxy_url: str | None,
        *,
        fingerprint_profile: FingerprintProfile | None = None,
    ) -> str:
        profile_key = fingerprint_profile.name if fingerprint_profile is not None else "__default__"
        return f"{proxy_url or '__direct__'}|{profile_key}"

    def _resolve_tls_field(
        self,
        name: str,
        fingerprint_profile: FingerprintProfile | None,
    ) -> Any:
        if fingerprint_profile is not None:
            value = getattr(fingerprint_profile, name, None)
            if value is not None:
                return value
        tls_policy = getattr(self.runtime_config, "tls_policy", None)
        if tls_policy is not None:
            return getattr(tls_policy, name, None)
        return None

    def _session_kwargs(
        self,
        proxy_url: str | None,
        *,
        fingerprint_profile: FingerprintProfile | None = None,
    ) -> dict[str, object]:
        tls_policy = getattr(self.runtime_config, "tls_policy", None)
        impersonate_profile = (
            fingerprint_profile.impersonate
            if fingerprint_profile is not None
            else getattr(tls_policy, "impersonate", "chrome120")
        )
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        kwargs: dict[str, object] = {
            "max_clients": max(20, len(self.runtime_config.proxy_endpoints) or 1),
            "impersonate": impersonate_profile,
        }
        if proxies is not None:
            kwargs["proxies"] = proxies
        for field_name in _SESSION_TLS_FIELDS:
            value = self._resolve_tls_field(field_name, fingerprint_profile)
            if value is not None:
                kwargs[field_name] = value
        # curl_cffi 0.7+ no longer ships its own cacert.pem; without an
        # explicit ``verify`` libcurl falls back to whatever CApath the
        # bundled libcurl was built with, which is missing inside our
        # venv. Effect (2026-05-18 incident): 47.6 % transport_err on
        # /api/v1/event/{event_id} with ErrCode 77 / CURLE_SSL_CACERT_BADFILE
        # right after the 0.5.10 → 0.15.0 upgrade. ``setdefault`` so an
        # explicit policy override (str path or False) still wins.
        kwargs.setdefault("verify", certifi.where())
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


def _proxy_address_from_url(proxy_url: str | None) -> str | None:
    if not proxy_url:
        return None
    parsed = urlparse(proxy_url)
    host = parsed.hostname
    if not host:
        netloc = parsed.netloc or parsed.path
        if "@" in netloc:
            netloc = netloc.rsplit("@", 1)[1]
        return netloc or None
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    if parsed.port is None:
        return host
    return f"{host}:{parsed.port}"


def _transport_error_message(exc: BaseException, *, timeout: float) -> str:
    if isinstance(exc, asyncio.TimeoutError):
        return f"request timed out after {timeout:g}s"
    return str(getattr(exc, "reason", exc))


def _load_proxy_state_store_from_env():
    enabled = os.environ.get("SOFASCORE_PROXY_HEALTH_POOL_FILTER_ENABLED", "true").strip().lower()
    if enabled in {"0", "false", "no", "off"}:
        return None
    redis_url = os.environ.get("REDIS_URL") or os.environ.get("SOFASCORE_REDIS_URL")
    if not redis_url:
        return None
    try:
        import redis  # type: ignore
    except ImportError:  # pragma: no cover - optional production dependency
        logger.warning("Proxy health pool filter disabled because python package `redis` is unavailable.")
        return None
    try:
        backend = redis.Redis.from_url(redis_url, decode_responses=True)
    except Exception as exc:  # pragma: no cover - defensive fail-open
        logger.warning("Proxy health pool filter disabled because Redis backend could not be created: %s", exc)
        return None
    return ProxyStateStore(backend)
