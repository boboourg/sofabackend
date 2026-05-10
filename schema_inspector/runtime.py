"""Runtime configuration and transport metadata."""

from __future__ import annotations

import os
import ssl
import uuid
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, urlparse, urlunparse


_SESSION_ID_USERNAME_MARKER = "-session-"
_SESSION_ID_LENGTH_CHARS = 8


@dataclass
class ProxyEndpoint:
    """One proxy endpoint available to the inspector transport.

    For Smartproxy gateway-style providers, ``is_session_expanded=True``
    marks this endpoint as a virtual slot synthesised from a base
    credential by appending ``-session-<id>`` to the username. On
    request failure, ``regenerate_session()`` swaps the session id (and
    therefore the upstream Smartproxy exit IP) while keeping the same
    logical name so worker statistics stay coherent.

    For non-session-expanded endpoints (legacy/static proxy URLs and the
    default no-op multiplier=1 path), ``regenerate_session()`` is a no-op
    so behaviour matches the historical single-flight model exactly.

    Mutability: ``url`` is the only field that can change after init,
    and only via ``regenerate_session()``. ``name`` and structural fields
    stay stable. Equality / hashing are not used anywhere — the pool
    indexes by ``name`` via a dict — so a non-frozen dataclass is safe.
    """

    name: str
    url: str
    cooldown_seconds: float = 30.0
    is_session_expanded: bool = False

    def regenerate_session(self) -> None:
        """Replace this endpoint's session id with a fresh random one.

        No-op for non-session-expanded endpoints — preserves the legacy
        behaviour for static proxy URLs and for the default-no-op
        multiplier=1 path.
        """
        if not self.is_session_expanded:
            return
        new_id = _generate_session_id()
        self.url = _set_session_id_in_url(self.url, new_id)


def _generate_session_id() -> str:
    return uuid.uuid4().hex[:_SESSION_ID_LENGTH_CHARS]


def _set_session_id_in_url(url: str, session_id: str) -> str:
    """Return ``url`` with the username's session-id segment set to
    ``session_id``.

    * If the username already contains ``-session-<old_id>``, the old id
      is replaced with ``session_id`` (preserving any modifiers that
      come AFTER the session segment, e.g. ``-sessionduration-30``).
    * If the username has no session segment, ``-session-<session_id>``
      is appended.
    * If the URL has no userinfo (no ``@`` in netloc), returns the URL
      unchanged. Defensive: the expansion path only generates URLs that
      already had auth, so this branch is only reached for legacy
      direct-egress callers.
    """
    parsed = urlparse(url)
    if "@" not in parsed.netloc:
        return url
    userinfo, _, hostpart = parsed.netloc.partition("@")
    if ":" in userinfo:
        user, _, password = userinfo.partition(":")
    else:
        user, password = userinfo, ""
    new_user = _replace_session_in_username(user, session_id)
    if password:
        new_userinfo = f"{quote(new_user, safe='-_.')}:{password}"
    else:
        new_userinfo = quote(new_user, safe="-_.")
    new_netloc = f"{new_userinfo}@{hostpart}"
    return urlunparse(parsed._replace(netloc=new_netloc))


def _replace_session_in_username(username: str, session_id: str) -> str:
    """Set ``-session-<id>`` in a Smartproxy-style username.

    Pattern: a Smartproxy modifier looks like
    ``baseuser-modifier-value`` (e.g. ``-country-us``,
    ``-session-abc``, ``-sessionduration-30``). Multiple modifiers
    chain. We replace ONLY the session-id value; all other modifiers
    (whatever appears after the next ``-`` past the session id) are
    preserved.
    """
    marker = _SESSION_ID_USERNAME_MARKER
    if marker not in username:
        return f"{username}{marker}{session_id}"
    prefix, _, after_marker = username.partition(marker)
    # ``after_marker`` is "<old_id>" or "<old_id>-<rest>"; preserve <rest>.
    if "-" in after_marker:
        _, dash, tail = after_marker.partition("-")
        return f"{prefix}{marker}{session_id}-{tail}"
    return f"{prefix}{marker}{session_id}"


def _expand_proxy_urls_with_sessions(
    urls: list[str],
    multiplier: int,
) -> list[tuple[str, str, bool]]:
    """Expand a list of base URLs into (name, url, is_session_expanded) tuples.

    * ``multiplier <= 1`` → one endpoint per URL with ``is_session_expanded=False``.
      Identical to the historical pre-expansion behaviour. This is the default
      when no env knob is set, so deploying this change with empty env is a no-op.
    * ``multiplier > 1`` and URL has userinfo → ``multiplier`` virtual slots per URL.
      Each slot's username gets a unique ``-session-<random>`` segment, so
      Smartproxy issues an independent rotating exit IP per slot. Slot names
      are ``proxy_<i>_s<NN>`` for forensics.
    * ``multiplier > 1`` and URL has NO userinfo → ``multiplier`` slots with the
      same URL but unique names. ``is_session_expanded=False`` because session
      regeneration only makes sense for credentialed gateway URLs. Concurrency
      still grows per slot (different ``in_use`` flags), but a failure won't
      try to mutate a URL that has nothing to mutate.
    """
    cleaned = [url.strip() for url in urls if url.strip()]
    out: list[tuple[str, str, bool]] = []
    if multiplier <= 1:
        for index, url in enumerate(cleaned):
            out.append((f"proxy_{index + 1}", url, False))
        return out
    for index, base_url in enumerate(cleaned):
        has_auth = "@" in (urlparse(base_url).netloc or "")
        for slot in range(multiplier):
            slot_name = f"proxy_{index + 1}_s{slot:02d}"
            if has_auth:
                slot_url = _set_session_id_in_url(base_url, _generate_session_id())
                out.append((slot_name, slot_url, True))
            else:
                out.append((slot_name, base_url, False))
    return out


def _resolve_proxy_session_multiplier(
    env: Mapping[str, str],
    *,
    env_keys: tuple[str, ...] = (),
) -> int:
    """Resolve the session multiplier from ``env`` using priority order.

    ``env_keys`` is a caller-supplied list of preferred env vars in
    descending priority (e.g. a per-lane scoped key first). The global
    ``SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER`` is always appended as
    the last fallback. If none are set or set to invalid values, returns 1
    (no-op default).

    Cap to ``[1, 100]`` so a typo cannot accidentally generate
    thousands of slots.
    """
    keys_to_check = list(env_keys) + ["SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER"]
    for key in keys_to_check:
        raw = (env.get(key) or "").strip()
        if not raw:
            continue
        try:
            value = int(raw)
        except ValueError:
            continue
        if value < 1:
            return 1
        return min(value, 100)
    return 1


def _resolve_proxy_max_in_use_per_endpoint(
    env: Mapping[str, str],
    *,
    env_keys: tuple[str, ...] = (),
) -> int:
    """Resolve the per-endpoint concurrent-lease cap from ``env``.

    Mirrors ``_resolve_proxy_session_multiplier`` priority semantics:
    ``env_keys`` first (per-lane scoped keys), then the global fallback
    ``SCHEMA_INSPECTOR_PROXY_MAX_IN_USE_PER_ENDPOINT``. If nothing is set
    or set to invalid values, returns 1 — strict no-op default that
    preserves the historical single-flight ProxyPool semantics.

    Cap to ``[1, 100]`` so a typo cannot accidentally generate hundreds
    of concurrent CONNECT tunnels per endpoint.
    """
    keys_to_check = list(env_keys) + ["SCHEMA_INSPECTOR_PROXY_MAX_IN_USE_PER_ENDPOINT"]
    for key in keys_to_check:
        raw = (env.get(key) or "").strip()
        if not raw:
            continue
        try:
            value = int(raw)
        except ValueError:
            continue
        if value < 1:
            return 1
        return min(value, 100)
    return 1


@dataclass(frozen=True)
class FingerprintProfile:
    """A lightweight browser fingerprint template for one outbound request.

    The ``name`` field is the stable identifier used by the transport layer
    in its session-cache key — it must be unique across all profiles in a
    given :class:`RuntimeConfig`.

    The optional TLS / HTTP-fingerprint fields override the matching values
    from :class:`TlsPolicy` on a per-profile basis. ``None`` means "not set,
    fall through to the next layer".
    """

    name: str
    impersonate: str
    user_agent: str
    accept_language: str
    sec_ch_ua: str
    sec_ch_ua_mobile: str
    sec_ch_ua_platform: str
    referer: str
    verify: bool | None = None
    http_version: Any | None = None
    ja3: str | None = None
    akamai: str | None = None
    extra_fp: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class RetryPolicy:
    """Retry and backoff configuration."""

    max_attempts: int = 3
    challenge_max_attempts: int = 5
    network_error_max_attempts: int = 4
    backoff_seconds: float = 1.0
    retry_status_codes: tuple[int, ...] = (408, 429, 500, 502, 503, 504)


@dataclass(frozen=True)
class TlsPolicy:
    """Verified TLS policy for outbound HTTPS requests.

    Optional curl_cffi overrides are passed through to ``AsyncSession`` only
    when explicitly set. ``None`` preserves the defaults associated with the
    chosen ``impersonate`` profile.
    """

    minimum_version: ssl.TLSVersion = ssl.TLSVersion.TLSv1_2
    maximum_version: ssl.TLSVersion | None = ssl.TLSVersion.TLSv1_3
    check_hostname: bool = True
    impersonate: str = "chrome110"
    verify: bool | None = None
    http_version: Any | None = None
    ja3: str | None = None
    akamai: str | None = None
    extra_fp: Mapping[str, Any] | None = None


@dataclass(frozen=True)
class RuntimeConfig:
    """All network settings for schema inspector requests."""

    source_slug: str = "sofascore"
    user_agent: str = "schema-inspector/1.0"
    require_proxy: bool = True
    default_headers: Mapping[str, str] = field(default_factory=dict)
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    tls_policy: TlsPolicy = field(default_factory=TlsPolicy)
    proxy_endpoints: tuple[ProxyEndpoint, ...] = ()
    proxy_request_cooldown_seconds: float = 1.5
    proxy_request_jitter_seconds: float = 1.0
    # Variant A-lite: cap on concurrent leases per single ProxyEndpoint
    # in ``ProxyPool``. Default 1 = legacy single-flight behaviour
    # (one lease per endpoint at a time). >1 unlocks several concurrent
    # CONNECT-tunnels through the same Smartproxy gateway credential
    # (Smartproxy issues independent rotating exit IPs per tunnel —
    # no username/session-id mutation involved, unlike Variant B which
    # was rejected by Smartproxy with HTTP 612).
    proxy_max_in_use_per_endpoint: int = 1
    fingerprint_profiles: tuple[FingerprintProfile, ...] = ()
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
    proxy_address: str | None = None
    # Per-attempt wall-clock latency in milliseconds. One attempt =
    # one HTTP request through curl_cffi (proxy lease + jitter +
    # request + response or timeout). Excludes inter-attempt backoff
    # and proxy-pool wait. Unset for synthesised attempts where
    # timing is not available (e.g. the proxy-required short-circuit
    # that never executes a request).
    latency_ms: int | None = None


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
    final_proxy_address: str | None = None


def load_runtime_config(
    *,
    env: Mapping[str, str] | None = None,
    proxy_urls: list[str] | None = None,
    proxy_env_key: str | None = None,
    user_agent: str | None = None,
    extra_headers: Mapping[str, str] | None = None,
    max_attempts: int | None = None,
    proxy_session_multiplier_env_keys: tuple[str, ...] = (),
    proxy_max_in_use_per_endpoint_env_keys: tuple[str, ...] = (),
) -> RuntimeConfig:
    """Build a runtime config from explicit arguments and environment.

    proxy_env_key: if set and proxy_urls is empty, reads proxy list from this
    env variable instead of the default SCHEMA_INSPECTOR_PROXY_URLS.
    Used to route historical workers to Proxyline without touching live proxies.

    proxy_session_multiplier_env_keys: ordered list of env vars whose value
    (if set and ≥1) is used as the Smartproxy session multiplier. The global
    ``SCHEMA_INSPECTOR_PROXY_SESSION_MULTIPLIER`` is always consulted as the
    last fallback. CLI workers may pass scoped keys (e.g.
    ``SCHEMA_INSPECTOR_LIVE_TIER_1_PROXY_SESSION_MULTIPLIER``) to opt this
    process into a different multiplier without affecting other lanes.
    Default empty tuple → only the global key is consulted → if absent,
    multiplier=1 (no-op, behaviour identical to pre-expansion code).
    """

    env = _load_project_env() if env is None else env
    configured_proxy_urls = list(proxy_urls or _read_proxy_urls(env, proxy_env_key=proxy_env_key))
    multiplier = _resolve_proxy_session_multiplier(
        env, env_keys=proxy_session_multiplier_env_keys
    )
    max_in_use_per_endpoint = _resolve_proxy_max_in_use_per_endpoint(
        env, env_keys=proxy_max_in_use_per_endpoint_env_keys
    )
    expanded_endpoints = _expand_proxy_urls_with_sessions(
        configured_proxy_urls, multiplier=multiplier
    )
    endpoints = tuple(
        ProxyEndpoint(name=name, url=url, is_session_expanded=is_expanded)
        for name, url, is_expanded in expanded_endpoints
    )
    impersonate = env.get("SCHEMA_INSPECTOR_TLS_IMPERSONATE", "chrome110").strip() or "chrome110"
    resolved_user_agent = user_agent or env.get("SCHEMA_INSPECTOR_USER_AGENT", "schema-inspector/1.0")
    fingerprint_profiles = _build_fingerprint_profiles(
        default_impersonate=impersonate,
        default_user_agent=resolved_user_agent,
    )

    headers = {
        "Accept": "application/json, text/plain, */*",
        # Explicit compression request — ensures gzip/br regardless of curl_cffi
        # impersonation profile or library version.
        "Accept-Encoding": "gzip, deflate, br",
    }
    if extra_headers:
        headers.update(extra_headers)

    source_slug = env.get("SCHEMA_INSPECTOR_SOURCE_SLUG", "sofascore").strip() or "sofascore"

    return RuntimeConfig(
        source_slug=source_slug,
        user_agent=resolved_user_agent,
        require_proxy=_env_bool(env, "SCHEMA_INSPECTOR_REQUIRE_PROXY", True),
        default_headers=headers,
        retry_policy=RetryPolicy(
            max_attempts=max_attempts or _env_int(env, "SCHEMA_INSPECTOR_MAX_ATTEMPTS", 3),
            challenge_max_attempts=max(
                1,
                _env_int(env, "SCHEMA_INSPECTOR_CHALLENGE_MAX_ATTEMPTS", 5),
            ),
            network_error_max_attempts=_env_int(env, "SCHEMA_INSPECTOR_NETWORK_ERROR_MAX_ATTEMPTS", 4),
            backoff_seconds=_env_float(env, "SCHEMA_INSPECTOR_BACKOFF_SECONDS", 1.0),
        ),
        tls_policy=TlsPolicy(
            minimum_version=_env_tls(env.get("SCHEMA_INSPECTOR_TLS_MIN_VERSION"), ssl.TLSVersion.TLSv1_2),
            maximum_version=_env_tls(env.get("SCHEMA_INSPECTOR_TLS_MAX_VERSION"), ssl.TLSVersion.TLSv1_3),
            check_hostname=_env_bool(env, "SCHEMA_INSPECTOR_TLS_CHECK_HOSTNAME", True),
            impersonate=impersonate,
        ),
        proxy_endpoints=endpoints,
        proxy_max_in_use_per_endpoint=max_in_use_per_endpoint,
        proxy_request_cooldown_seconds=_env_float(env, "SCHEMA_INSPECTOR_PROXY_REQUEST_COOLDOWN_SECONDS", 1.5),
        proxy_request_jitter_seconds=_env_float(env, "SCHEMA_INSPECTOR_PROXY_REQUEST_JITTER_SECONDS", 1.0),
        fingerprint_profiles=fingerprint_profiles,
    )


def _read_proxy_urls(env: Mapping[str, str], *, proxy_env_key: str | None = None) -> list[str]:
    # If a specific env key is requested (e.g. for historical workers), use it
    # exclusively so we never accidentally mix proxy pools.
    if proxy_env_key:
        joined = (env.get(proxy_env_key) or "").strip()
        values = [item.strip() for item in joined.split(",") if item.strip()]
        # Also honour a companion singular variant when a "...PROXY_URLS" key is
        # provided, so callers can set either plural or singular interchangeably
        # (mirrors the default-mode behaviour below).
        if proxy_env_key.endswith("_PROXY_URLS"):
            singular_key = proxy_env_key[: -len("_PROXY_URLS")] + "_PROXY_URL"
            singular = (env.get(singular_key) or "").strip()
            if singular and singular not in values:
                values.insert(0, singular)
        return values

    # Default: read from the standard live/scheduled proxy variables.
    values = []
    single = (env.get("SCHEMA_INSPECTOR_PROXY_URL") or "").strip()
    if single:
        values.append(single)
    joined = (env.get("SCHEMA_INSPECTOR_PROXY_URLS") or "").strip()
    if joined:
        values.extend(item.strip() for item in joined.split(",") if item.strip())
    return values


STRUCTURE_PROXY_ENV_KEY = "SCHEMA_INSPECTOR_STRUCTURE_PROXY_URLS"
HISTORICAL_PROXY_ENV_KEY = "SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS"


def load_structure_runtime_config(
    *,
    env: Mapping[str, str] | None = None,
    user_agent: str | None = None,
    extra_headers: Mapping[str, str] | None = None,
    max_attempts: int | None = None,
    require_non_residential: bool = True,
) -> RuntimeConfig:
    """Build a RuntimeConfig that routes exclusively through the non-residential
    (datacenter / static) proxy pool reserved for the structural-sync contour.

    The contract: structural sync must NEVER borrow the residential / live pool.
    We read only ``SCHEMA_INSPECTOR_STRUCTURE_PROXY_URLS`` (+ singular variant)
    via ``proxy_env_key`` so the two pools are mechanically isolated, even when
    a config refresh races with live workers.

    When ``require_non_residential=True`` and no URLs are configured, this
    function raises. This is intentional: structure-sync workers must fail-fast
    rather than silently fall back to direct egress or to the residential pool.
    """

    resolved_env = _load_project_env() if env is None else env
    config = load_runtime_config(
        env=resolved_env,
        proxy_env_key=STRUCTURE_PROXY_ENV_KEY,
        user_agent=user_agent,
        extra_headers=extra_headers,
        max_attempts=max_attempts,
    )
    config = replace(
        config,
        proxy_request_cooldown_seconds=_env_float(
            resolved_env,
            "SCHEMA_INSPECTOR_STRUCTURE_PROXY_REQUEST_COOLDOWN_SECONDS",
            0.5,
        ),
        proxy_request_jitter_seconds=_env_float(
            resolved_env,
            "SCHEMA_INSPECTOR_STRUCTURE_PROXY_REQUEST_JITTER_SECONDS",
            1.5,
        ),
        retry_policy=replace(
            config.retry_policy,
            max_attempts=_env_int(resolved_env, "SCHEMA_INSPECTOR_STRUCTURE_MAX_ATTEMPTS", 2),
            challenge_max_attempts=_env_int(
                resolved_env,
                "SCHEMA_INSPECTOR_STRUCTURE_CHALLENGE_MAX_ATTEMPTS",
                2,
            ),
            network_error_max_attempts=_env_int(
                resolved_env,
                "SCHEMA_INSPECTOR_STRUCTURE_NETWORK_ERROR_MAX_ATTEMPTS",
                2,
            ),
            backoff_seconds=_env_float(
                resolved_env,
                "SCHEMA_INSPECTOR_STRUCTURE_BACKOFF_SECONDS",
                0.25,
            ),
        ),
    )
    if require_non_residential and not config.proxy_endpoints:
        raise RuntimeError(
            "structure-sync contour requires a non-residential proxy pool; "
            "set SCHEMA_INSPECTOR_STRUCTURE_PROXY_URLS (or SCHEMA_INSPECTOR_STRUCTURE_PROXY_URL)"
        )
    return config


def load_historical_runtime_config(
    *,
    env: Mapping[str, str] | None = None,
    user_agent: str | None = None,
    extra_headers: Mapping[str, str] | None = None,
    max_attempts: int | None = None,
    require_dedicated_pool: bool = True,
) -> RuntimeConfig:
    """Build a RuntimeConfig that routes exclusively through the historical
    proxy pool, with rate-limit settings calibrated for archival/backfill
    workloads.

    The historical contour scans deep dated surfaces (event/{id},
    sport/*/scheduled-events/{date}) which Sofascore's edge treats as
    higher-suspicion traffic than scaffold queries. To avoid burning out
    the pool we apply a slower cooldown, larger jitter, and minimal retry
    policy compared to live.

    The contract: historical workers must NEVER fall back to the live or
    structure pool. We read only ``SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS``
    (+ singular variant) via ``proxy_env_key`` so the three pools are
    mechanically isolated.

    When ``require_dedicated_pool=True`` and no URLs are configured, this
    function raises. Historical workers must fail-fast rather than silently
    fall back to direct egress or to another pool.
    """

    resolved_env = _load_project_env() if env is None else env
    config = load_runtime_config(
        env=resolved_env,
        proxy_env_key=HISTORICAL_PROXY_ENV_KEY,
        user_agent=user_agent,
        extra_headers=extra_headers,
        max_attempts=max_attempts,
    )
    config = replace(
        config,
        proxy_request_cooldown_seconds=_env_float(
            resolved_env,
            "SCHEMA_INSPECTOR_HISTORICAL_PROXY_REQUEST_COOLDOWN_SECONDS",
            3.0,
        ),
        proxy_request_jitter_seconds=_env_float(
            resolved_env,
            "SCHEMA_INSPECTOR_HISTORICAL_PROXY_REQUEST_JITTER_SECONDS",
            2.0,
        ),
        retry_policy=replace(
            config.retry_policy,
            max_attempts=_env_int(
                resolved_env,
                "SCHEMA_INSPECTOR_HISTORICAL_MAX_ATTEMPTS",
                1,
            ),
            challenge_max_attempts=_env_int(
                resolved_env,
                "SCHEMA_INSPECTOR_HISTORICAL_CHALLENGE_MAX_ATTEMPTS",
                1,
            ),
            network_error_max_attempts=_env_int(
                resolved_env,
                "SCHEMA_INSPECTOR_HISTORICAL_NETWORK_ERROR_MAX_ATTEMPTS",
                2,
            ),
            backoff_seconds=_env_float(
                resolved_env,
                "SCHEMA_INSPECTOR_HISTORICAL_BACKOFF_SECONDS",
                2.0,
            ),
        ),
    )
    if require_dedicated_pool and not config.proxy_endpoints:
        raise RuntimeError(
            "historical contour requires a dedicated proxy pool; "
            "set SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS "
            "(or SCHEMA_INSPECTOR_HISTORICAL_PROXY_URL)"
        )
    return config


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


def _build_fingerprint_profiles(
    *,
    default_impersonate: str,
    default_user_agent: str,
) -> tuple[FingerprintProfile, ...]:
    profiles = (
        FingerprintProfile(
            name="chrome-win-110",
            impersonate="chrome110",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
            ),
            accept_language="en-US,en;q=0.9,uk;q=0.8",
            sec_ch_ua='"Chromium";v="110", "Google Chrome";v="110", "Not A(Brand";v="24"',
            sec_ch_ua_mobile="?0",
            sec_ch_ua_platform='"Windows"',
            referer="https://www.sofascore.com/",
        ),
        FingerprintProfile(
            name="chrome-win-107",
            impersonate="chrome107",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36"
            ),
            accept_language="en-US,en;q=0.9",
            sec_ch_ua='"Chromium";v="107", "Google Chrome";v="107", "Not=A?Brand";v="24"',
            sec_ch_ua_mobile="?0",
            sec_ch_ua_platform='"Windows"',
            referer="https://www.sofascore.com/football",
        ),
        FingerprintProfile(
            name="edge-win-101",
            impersonate="edge101",
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.67 "
                "Safari/537.36 Edg/101.0.1210.53"
            ),
            accept_language="en-GB,en;q=0.9",
            sec_ch_ua='"Microsoft Edge";v="101", "Chromium";v="101", "Not:A-Brand";v="99"',
            sec_ch_ua_mobile="?0",
            sec_ch_ua_platform='"Windows"',
            referer="https://www.sofascore.com/basketball",
        ),
        FingerprintProfile(
            name="chrome-linux-104",
            impersonate="chrome104",
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36"
            ),
            accept_language="en-US,en;q=0.9,fr;q=0.7",
            sec_ch_ua='"Chromium";v="104", "Google Chrome";v="104", "Not A(Brand";v="99"',
            sec_ch_ua_mobile="?0",
            sec_ch_ua_platform='"Linux"',
            referer="https://www.sofascore.com/tennis",
        ),
    )
    if default_user_agent and default_user_agent != "schema-inspector/1.0":
        return (
            FingerprintProfile(
                name="configured-primary",
                impersonate=default_impersonate,
                user_agent=default_user_agent,
                accept_language="en-US,en;q=0.9",
                sec_ch_ua='"Chromium";v="110", "Google Chrome";v="110", "Not A(Brand";v="24"',
                sec_ch_ua_mobile="?0",
                sec_ch_ua_platform='"Windows"',
                referer="https://www.sofascore.com/",
            ),
            *profiles,
        )
    return profiles
