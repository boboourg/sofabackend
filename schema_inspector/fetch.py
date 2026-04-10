"""HTTP and file fetching helpers."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping

from .runtime import RuntimeConfig, TransportAttempt, load_runtime_config
from .transport import InspectorTransport


@dataclass(frozen=True)
class FetchResult:
    """Fetched response details."""

    source_url: str
    resolved_url: str
    fetched_at: str
    status_code: int
    headers: Mapping[str, str]
    body_bytes: bytes
    payload: object
    attempts: tuple[TransportAttempt, ...]
    final_proxy_name: str | None
    challenge_reason: str | None


class FetchJsonError(RuntimeError):
    """Raised when a response cannot be safely parsed as JSON."""


def fetch_json(
    url: str,
    headers: Mapping[str, str] | None = None,
    timeout: float = 20.0,
    runtime_config: RuntimeConfig | None = None,
) -> FetchResult:
    """Fetch JSON through the inspector transport architecture."""

    runtime_config = runtime_config or load_runtime_config(extra_headers=headers)
    transport = InspectorTransport(runtime_config)
    transport_result = transport.fetch(url, headers=headers, timeout=timeout)
    if transport_result.challenge_reason:
        raise FetchJsonError(
            f"Request ended with challenge reason '{transport_result.challenge_reason}' "
            f"and status {transport_result.status_code}"
        )
    if transport_result.status_code >= 400:
        raise FetchJsonError(f"Request failed with HTTP {transport_result.status_code}")

    try:
        payload = json.loads(transport_result.body_bytes.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise FetchJsonError(f"Response is not valid JSON: {exc}") from exc

    return FetchResult(
        source_url=url,
        resolved_url=transport_result.resolved_url,
        fetched_at=_utc_now(),
        status_code=transport_result.status_code,
        headers=transport_result.headers,
        body_bytes=transport_result.body_bytes,
        payload=payload,
        attempts=transport_result.attempts,
        final_proxy_name=transport_result.final_proxy_name,
        challenge_reason=transport_result.challenge_reason,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
