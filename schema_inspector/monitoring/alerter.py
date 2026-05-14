"""Alert sinks — destinations where formatted alert messages get sent.

The protocol is intentionally trivial: ``send(message: str) -> bool``
returning ``True`` on success. Failures are logged but never raised — the
daemon must keep running even when Telegram is degraded.

Two implementations:

* :class:`TelegramAlertSink` — production sink, HTTP POST to
  ``https://api.telegram.org``.
* :class:`NullAlertSink` — no-op for development / tests / when the
  Telegram credentials are missing.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

import httpx


logger = logging.getLogger(__name__)


class AlertSink(Protocol):
    """Anything that can deliver a formatted alert message."""

    async def send(self, message: str) -> bool:
        """Deliver ``message``. Return True on success, False otherwise.

        Must never raise.
        """


class NullAlertSink:
    """Drops all messages on the floor. Used when monitoring is disabled."""

    def __init__(self) -> None:
        self.delivered: list[str] = []

    async def send(self, message: str) -> bool:
        self.delivered.append(message)
        logger.debug("NullAlertSink: dropped message len=%d", len(message))
        return True


class TelegramAlertSink:
    """Posts messages to the Telegram Bot API.

    Uses ``parse_mode=None`` so accidental Markdown characters in the
    payload (especially in dynamic ``note`` fields) do not break delivery.
    """

    API_URL_TEMPLATE = "https://api.telegram.org/bot{token}/sendMessage"

    def __init__(
        self,
        *,
        bot_token: str,
        chat_id: str,
        timeout_seconds: float = 10.0,
        http_client: Any = None,
    ) -> None:
        if not bot_token:
            raise ValueError("bot_token must not be empty")
        if not chat_id:
            raise ValueError("chat_id must not be empty")
        self.bot_token = str(bot_token)
        self.chat_id = str(chat_id)
        self.timeout_seconds = float(timeout_seconds)
        # ``http_client`` may be injected for tests. When None we lazy-build
        # an httpx.AsyncClient on first send and reuse it across calls.
        self._http_client = http_client

    def _api_url(self) -> str:
        return self.API_URL_TEMPLATE.format(token=self.bot_token)

    async def _ensure_client(self) -> Any:
        if self._http_client is not None:
            return self._http_client
        self._http_client = httpx.AsyncClient(timeout=self.timeout_seconds)
        return self._http_client

    async def send(self, message: str) -> bool:
        try:
            client = await self._ensure_client()
            response = await client.post(
                self._api_url(),
                json={
                    "chat_id": self.chat_id,
                    "text": message,
                    "disable_web_page_preview": True,
                },
                timeout=self.timeout_seconds,
            )
            if response.status_code == 200:
                return True
            logger.warning(
                "TelegramAlertSink: non-200 response status=%d body=%r",
                response.status_code,
                response.text[:200],
            )
            return False
        except Exception as exc:  # noqa: BLE001 — sink must never crash daemon
            logger.warning("TelegramAlertSink: send failed: %r", exc)
            return False

    async def aclose(self) -> None:
        client = self._http_client
        if client is None:
            return
        close = getattr(client, "aclose", None)
        if callable(close):
            try:
                await close()
            except Exception:  # noqa: BLE001
                pass
        self._http_client = None
