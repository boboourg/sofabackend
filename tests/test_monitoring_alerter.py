"""Tests for monitoring/alerter.py — TelegramAlertSink + NullAlertSink.

Pins the contract:
- TelegramAlertSink POSTs to api.telegram.org with bot token + chat_id
- Non-200 response → returns False, does NOT raise
- Network exception → returns False, does NOT raise
- bot_token / chat_id missing → constructor raises ValueError
- Message body uses ``chat_id`` and ``text`` fields per Telegram API
- aclose closes the underlying http client when one was created
- NullAlertSink always returns True and records delivery for inspection
"""

from __future__ import annotations

import unittest

from schema_inspector.monitoring.alerter import (
    NullAlertSink,
    TelegramAlertSink,
)


class _StubResponse:
    def __init__(self, status_code: int = 200, body: str = "ok") -> None:
        self.status_code = status_code
        self.text = body


class _StubHttpClient:
    """Tiny shim that captures the last POST call."""

    def __init__(self, *, status_code: int = 200, raise_exc: Exception | None = None) -> None:
        self.status_code = status_code
        self.raise_exc = raise_exc
        self.last_url: str | None = None
        self.last_json: dict | None = None
        self.last_timeout: float | None = None
        self.post_calls: list[dict] = []
        self.aclose_called = False

    async def post(self, url: str, *, json: dict, timeout: float):
        self.last_url = url
        self.last_json = json
        self.last_timeout = timeout
        self.post_calls.append({"url": url, "json": json, "timeout": timeout})
        if self.raise_exc:
            raise self.raise_exc
        return _StubResponse(status_code=self.status_code)

    async def aclose(self) -> None:
        self.aclose_called = True


class TelegramAlertSinkTests(unittest.IsolatedAsyncioTestCase):
    async def test_post_includes_bot_token_in_url(self) -> None:
        client = _StubHttpClient()
        sink = TelegramAlertSink(bot_token="1234:abc", chat_id="5678", http_client=client)
        await sink.send("hello")
        self.assertIn("1234:abc", client.last_url or "")
        self.assertTrue((client.last_url or "").endswith("/sendMessage"))

    async def test_post_includes_chat_id_and_text(self) -> None:
        client = _StubHttpClient()
        sink = TelegramAlertSink(bot_token="x", chat_id="42", http_client=client)
        await sink.send("payload")
        self.assertEqual(client.last_json["chat_id"], "42")
        self.assertEqual(client.last_json["text"], "payload")
        # We always disable web preview to avoid Telegram unfurling URLs
        # accidentally embedded in notes.
        self.assertTrue(client.last_json["disable_web_page_preview"])

    async def test_returns_true_on_200(self) -> None:
        client = _StubHttpClient(status_code=200)
        sink = TelegramAlertSink(bot_token="x", chat_id="42", http_client=client)
        self.assertTrue(await sink.send("ok"))

    async def test_returns_false_on_non_200(self) -> None:
        client = _StubHttpClient(status_code=400)
        sink = TelegramAlertSink(bot_token="x", chat_id="42", http_client=client)
        self.assertFalse(await sink.send("bad"))

    async def test_returns_false_on_network_exception(self) -> None:
        client = _StubHttpClient(raise_exc=ConnectionError("DNS broken"))
        sink = TelegramAlertSink(bot_token="x", chat_id="42", http_client=client)
        self.assertFalse(await sink.send("net"))

    async def test_empty_token_rejected_at_construction(self) -> None:
        with self.assertRaises(ValueError):
            TelegramAlertSink(bot_token="", chat_id="42")

    async def test_empty_chat_id_rejected_at_construction(self) -> None:
        with self.assertRaises(ValueError):
            TelegramAlertSink(bot_token="x", chat_id="")

    async def test_aclose_closes_injected_client(self) -> None:
        client = _StubHttpClient()
        sink = TelegramAlertSink(bot_token="x", chat_id="42", http_client=client)
        await sink.send("trigger lazy init")
        await sink.aclose()
        self.assertTrue(client.aclose_called)

    async def test_aclose_no_op_when_client_not_built(self) -> None:
        sink = TelegramAlertSink(bot_token="x", chat_id="42")
        # No send call → no client. aclose must not raise.
        await sink.aclose()

    async def test_timeout_passed_through(self) -> None:
        client = _StubHttpClient()
        sink = TelegramAlertSink(
            bot_token="x", chat_id="42", timeout_seconds=2.5, http_client=client
        )
        await sink.send("hi")
        self.assertEqual(client.last_timeout, 2.5)


class NullAlertSinkTests(unittest.IsolatedAsyncioTestCase):
    async def test_always_returns_true(self) -> None:
        sink = NullAlertSink()
        self.assertTrue(await sink.send("anything"))

    async def test_records_delivered_messages(self) -> None:
        sink = NullAlertSink()
        await sink.send("a")
        await sink.send("b")
        self.assertEqual(sink.delivered, ["a", "b"])


if __name__ == "__main__":
    unittest.main()
