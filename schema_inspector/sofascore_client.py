"""Async Sofascore API client built on top of InspectorTransport."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Mapping

from .runtime import RuntimeConfig, TransportAttempt, TransportResult
from .transport import InspectorTransport


@dataclass(frozen=True)
class SofascoreResponse:
    """Successful JSON response returned by the Sofascore client."""

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


class SofascoreClientError(RuntimeError):
    """Base client error with optional transport context."""

    def __init__(self, message: str, *, transport_result: TransportResult | None = None) -> None:
        super().__init__(message)
        self.transport_result = transport_result


class SofascoreHttpError(SofascoreClientError):
    """Raised for non-success HTTP responses that are not mapped to a more specific error."""


class SofascoreRateLimitError(SofascoreHttpError):
    """Raised when upstream returns HTTP 429 / rate-limited challenge."""


class SofascoreAccessDeniedError(SofascoreHttpError):
    """Raised when upstream returns HTTP 403 / guarded response."""


class SofascoreJsonDecodeError(SofascoreClientError):
    """Raised when upstream response is not valid JSON."""


class SofascoreClient:
    """Thin async wrapper above InspectorTransport for parser and ETL code."""

    def __init__(
        self,
        runtime_config: RuntimeConfig,
        *,
        transport: InspectorTransport | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.runtime_config = runtime_config
        self.transport = transport or InspectorTransport(runtime_config)
        self.logger = logger or logging.getLogger(__name__)

    async def get_json(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        timeout: float = 20.0,
    ) -> SofascoreResponse:
        """Fetch and decode a JSON response through the stealth transport."""

        self.logger.debug("SofascoreClient GET %s", url)
        transport_result = await self.transport.fetch(url, headers=headers, timeout=timeout)
        self._log_transport_result(url, transport_result)

        if transport_result.status_code == 429 or transport_result.challenge_reason == "rate_limited":
            message = self._format_http_error("Rate limited by upstream", transport_result)
            self.logger.warning(message)
            raise SofascoreRateLimitError(message, transport_result=transport_result)

        if transport_result.status_code == 403 or transport_result.challenge_reason in {"access_denied", "bot_challenge"}:
            message = self._format_http_error("Access denied by upstream", transport_result)
            self.logger.warning(message)
            raise SofascoreAccessDeniedError(message, transport_result=transport_result)

        if transport_result.status_code >= 400:
            message = self._format_http_error("HTTP request failed", transport_result)
            self.logger.error(message)
            raise SofascoreHttpError(message, transport_result=transport_result)

        try:
            payload = json.loads(transport_result.body_bytes.decode("utf-8"))
        except json.JSONDecodeError as exc:
            message = f"Response is not valid JSON for {url}: {exc}"
            self.logger.error(message)
            raise SofascoreJsonDecodeError(message, transport_result=transport_result) from exc

        response = SofascoreResponse(
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
        self.logger.info(
            "SofascoreClient OK status=%s attempts=%s proxy=%s url=%s",
            response.status_code,
            len(response.attempts),
            response.final_proxy_name or "direct",
            url,
        )
        return response

    def _log_transport_result(self, url: str, transport_result: TransportResult) -> None:
        for attempt in transport_result.attempts:
            self.logger.debug(
                "SofascoreClient attempt=%s proxy=%s status=%s challenge=%s error=%s url=%s",
                attempt.attempt_number,
                attempt.proxy_name or "direct",
                attempt.status_code,
                attempt.challenge_reason,
                attempt.error,
                url,
            )

    @staticmethod
    def _format_http_error(prefix: str, transport_result: TransportResult) -> str:
        proxy_name = transport_result.final_proxy_name or "direct"
        return (
            f"{prefix}: status={transport_result.status_code}, "
            f"proxy={proxy_name}, challenge={transport_result.challenge_reason or '-'}"
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
