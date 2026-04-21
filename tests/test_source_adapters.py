from __future__ import annotations

import unittest

from schema_inspector.runtime import TransportAttempt


class SourceAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_source_adapter_exposes_source_slug(self) -> None:
        from schema_inspector.runtime import RuntimeConfig
        from schema_inspector.sources import SofascoreSourceAdapter

        adapter = SofascoreSourceAdapter(runtime_config=RuntimeConfig(require_proxy=False), client=_FakeSofascoreClient())

        self.assertEqual(adapter.source_slug, "sofascore")
        self.assertTrue(adapter.is_enabled)

    async def test_sofascore_adapter_maps_response_fields_from_client(self) -> None:
        from schema_inspector.runtime import RuntimeConfig
        from schema_inspector.sources import SofascoreSourceAdapter, SourceFetchRequest

        fake_client = _FakeSofascoreClient(
            response=_FakeSofascoreResponse(
                source_url="https://www.sofascore.com/api/v1/test",
                resolved_url="https://api.sofascore.com/v1/test",
                fetched_at="2026-04-21T10:00:00+00:00",
                status_code=200,
                headers={"content-type": "application/json"},
                body_bytes=b'{"ok": true}',
                payload={"ok": True},
                attempts=(TransportAttempt(1, "proxy_1", 200, None, None),),
                final_proxy_name="proxy_1",
                challenge_reason=None,
            )
        )
        adapter = SofascoreSourceAdapter(runtime_config=RuntimeConfig(require_proxy=False), client=fake_client)

        response = await adapter.get_json(
            SourceFetchRequest(
                url="https://www.sofascore.com/api/v1/test",
                timeout=15.0,
                headers={"x-test": "1"},
            )
        )

        self.assertEqual(fake_client.calls, [("https://www.sofascore.com/api/v1/test", {"x-test": "1"}, 15.0)])
        self.assertEqual(response.source_slug, "sofascore")
        self.assertEqual(response.source_url, "https://www.sofascore.com/api/v1/test")
        self.assertEqual(response.resolved_url, "https://api.sofascore.com/v1/test")
        self.assertEqual(response.fetched_at, "2026-04-21T10:00:00+00:00")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "application/json")
        self.assertEqual(response.body_bytes, b'{"ok": true}')
        self.assertEqual(response.payload, {"ok": True})
        self.assertEqual(response.attempts[0].proxy_name, "proxy_1")
        self.assertEqual(response.final_proxy_name, "proxy_1")
        self.assertEqual(response.challenge_reason, None)

    async def test_secondary_stub_is_disabled_and_raises_actionable_error(self) -> None:
        from schema_inspector.sources import (
            DisabledSourceAdapterError,
            SecondaryStubSourceAdapter,
            SourceFetchRequest,
        )

        adapter = SecondaryStubSourceAdapter()

        self.assertEqual(adapter.source_slug, "secondary_source")
        self.assertFalse(adapter.is_enabled)
        with self.assertRaisesRegex(DisabledSourceAdapterError, "secondary_source adapter is disabled"):
            await adapter.get_json(SourceFetchRequest(url="https://secondary.example/api/test"))

    def test_factory_returns_sofascore_by_default_and_stub_for_secondary_source(self) -> None:
        from schema_inspector.runtime import RuntimeConfig
        from schema_inspector.sources import (
            SecondaryStubSourceAdapter,
            SofascoreSourceAdapter,
            build_source_adapter,
        )

        runtime_config = RuntimeConfig(require_proxy=False)

        default_adapter = build_source_adapter(runtime_config=runtime_config)
        secondary_adapter = build_source_adapter("secondary_source", runtime_config=runtime_config)

        self.assertIsInstance(default_adapter, SofascoreSourceAdapter)
        self.assertIsInstance(secondary_adapter, SecondaryStubSourceAdapter)

    def test_factory_raises_clean_error_for_unknown_source(self) -> None:
        from schema_inspector.runtime import RuntimeConfig
        from schema_inspector.sources import UnknownSourceAdapterError, build_source_adapter

        with self.assertRaisesRegex(UnknownSourceAdapterError, "Unknown source adapter: mirror_x"):
            build_source_adapter("mirror_x", runtime_config=RuntimeConfig(require_proxy=False))


class _FakeSofascoreResponse:
    def __init__(
        self,
        *,
        source_url: str,
        resolved_url: str,
        fetched_at: str,
        status_code: int,
        headers: dict[str, str],
        body_bytes: bytes,
        payload: object,
        attempts=None,
        final_proxy_name: str | None = None,
        challenge_reason: str | None = None,
    ) -> None:
        self.source_url = source_url
        self.resolved_url = resolved_url
        self.fetched_at = fetched_at
        self.status_code = status_code
        self.headers = headers
        self.body_bytes = body_bytes
        self.payload = payload
        self.attempts = attempts or ()
        self.final_proxy_name = final_proxy_name
        self.challenge_reason = challenge_reason


class _FakeSofascoreClient:
    def __init__(self, response: _FakeSofascoreResponse | None = None) -> None:
        self.response = response or _FakeSofascoreResponse(
            source_url="https://www.sofascore.com/api/v1/default",
            resolved_url="https://www.sofascore.com/api/v1/default",
            fetched_at="2026-04-21T00:00:00+00:00",
            status_code=200,
            headers={"content-type": "application/json"},
            body_bytes=b"{}",
            payload={},
            attempts=(),
            final_proxy_name=None,
            challenge_reason=None,
        )
        self.calls: list[tuple[str, dict[str, str] | None, float]] = []

    async def get_json(self, url: str, *, headers=None, timeout: float = 20.0):
        normalized_headers = None if headers is None else dict(headers)
        self.calls.append((url, normalized_headers, timeout))
        return self.response


if __name__ == "__main__":
    unittest.main()
