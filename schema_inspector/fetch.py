"""HTTP and file fetching helpers."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Mapping

from .runtime import RuntimeConfig, TransportAttempt, load_runtime_config
from .sofascore_client import SofascoreClientError
from .sources import SourceAdapterError, SourceFetchRequest, build_source_adapter


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


async def fetch_json(
    url: str,
    headers: Mapping[str, str] | None = None,
    timeout: float = 20.0,
    runtime_config: RuntimeConfig | None = None,
) -> FetchResult:
    """Fetch JSON through the inspector transport architecture."""

    runtime_config = runtime_config or load_runtime_config(extra_headers=headers)
    try:
        adapter = build_source_adapter(runtime_config.source_slug, runtime_config=runtime_config)
        result = await adapter.get_json(SourceFetchRequest(url=url, headers=headers, timeout=timeout))
    except (SofascoreClientError, SourceAdapterError) as exc:
        raise FetchJsonError(str(exc)) from exc

    return FetchResult(
        source_url=result.source_url,
        resolved_url=result.resolved_url,
        fetched_at=result.fetched_at,
        status_code=result.status_code,
        headers=result.headers,
        body_bytes=result.body_bytes,
        payload=result.payload,
        attempts=result.attempts,
        final_proxy_name=result.final_proxy_name,
        challenge_reason=result.challenge_reason,
    )


def fetch_json_sync(
    url: str,
    headers: Mapping[str, str] | None = None,
    timeout: float = 20.0,
    runtime_config: RuntimeConfig | None = None,
) -> FetchResult:
    """Synchronous bridge for entrypoints that still need a blocking API."""

    return asyncio.run(fetch_json(url, headers=headers, timeout=timeout, runtime_config=runtime_config))
