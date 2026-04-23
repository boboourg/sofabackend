from __future__ import annotations

import asyncio
import time
import unittest

from schema_inspector.runtime import RuntimeConfig, RetryPolicy, TlsPolicy, load_runtime_config
from schema_inspector.transport import InspectorTransport


class TransportRetryPolicyTests(unittest.IsolatedAsyncioTestCase):
    def test_proxy_cooldown_decisions_follow_retry_policy_and_challenge_reason(self) -> None:
        transport = InspectorTransport(load_runtime_config(env={}, proxy_urls=["http://proxy-1.local:8080"]))

        self.assertTrue(transport._should_cooldown_proxy(429, None))
        self.assertTrue(transport._should_cooldown_proxy(403, "access_denied"))
        self.assertTrue(transport._should_cooldown_proxy(503, None))
        self.assertFalse(transport._should_cooldown_proxy(404, None))
        self.assertFalse(transport._should_cooldown_proxy(200, None))

    async def test_proxy_pool_respects_cooldown_even_when_only_one_proxy_exists(self) -> None:
        from schema_inspector.proxy import ProxyPool
        from schema_inspector.runtime import ProxyEndpoint

        now = 100.0
        pool = ProxyPool(
            (ProxyEndpoint(name="proxy-1", url="http://proxy-1.local:8080", cooldown_seconds=30.0),),
            clock=lambda: now,
        )

        lease = await pool.acquire()
        self.assertEqual(lease.endpoint.name, "proxy-1")
        await lease.release(success=False)
        self.assertIsNone(pool.try_acquire_nowait())

    async def test_proxy_pool_serializes_requests_per_proxy_and_applies_post_use_cooldown(self) -> None:
        from schema_inspector.proxy import ProxyPool
        from schema_inspector.runtime import ProxyEndpoint

        pool = ProxyPool(
            (
                ProxyEndpoint(
                    name="proxy-1",
                    url="http://proxy-1.local:8080",
                    cooldown_seconds=0.0,
                ),
            ),
            default_success_cooldown_seconds=0.05,
            jitter_seconds=0.0,
        )

        first = await pool.acquire()
        second_started = asyncio.Event()
        second_acquired_at: float | None = None

        async def _acquire_second() -> None:
            nonlocal second_acquired_at
            second_started.set()
            lease = await pool.acquire()
            second_acquired_at = time.monotonic()
            await lease.release(success=True)

        started_at = time.monotonic()
        second_task = asyncio.create_task(_acquire_second())
        await second_started.wait()
        await asyncio.sleep(0.01)
        self.assertIsNone(second_acquired_at)

        await first.release(success=True)
        await second_task

        assert second_acquired_at is not None
        self.assertGreaterEqual(second_acquired_at - started_at, 0.05)

    async def test_transport_rotates_fingerprints_and_headers(self) -> None:
        transport = _RecordingTransport(
            load_runtime_config(
                env={
                    "SCHEMA_INSPECTOR_PROXY_URLS": "http://proxy-1.local:8080,http://proxy-2.local:8080",
                    "SCHEMA_INSPECTOR_PROXY_REQUEST_COOLDOWN_SECONDS": "0",
                    "SCHEMA_INSPECTOR_PROXY_REQUEST_JITTER_SECONDS": "0",
                }
            ),
            sleeper=lambda delay: None,
        )

        await transport.fetch("https://www.sofascore.com/api/v1/unique-tournament/17", timeout=5.0)
        await transport.fetch("https://www.sofascore.com/api/v1/unique-tournament/18", timeout=5.0)

        self.assertEqual(len(transport.seen_requests), 2)
        first, second = transport.seen_requests
        self.assertNotEqual(first["impersonate"], second["impersonate"])
        self.assertNotEqual(first["headers"]["User-Agent"], second["headers"]["User-Agent"])
        self.assertIn("Accept-Language", first["headers"])
        self.assertIn("Sec-Ch-Ua", first["headers"])
        self.assertIn("Referer", first["headers"])

    async def test_transport_retries_access_denied_across_extended_proxy_budget(self) -> None:
        transport = _FakeTransport(
            RuntimeConfig(
                user_agent="test-agent",
                retry_policy=RetryPolicy(
                    max_attempts=2,
                    challenge_max_attempts=5,
                    network_error_max_attempts=4,
                    backoff_seconds=0.0,
                ),
                tls_policy=TlsPolicy(impersonate="chrome110"),
                proxy_endpoints=load_runtime_config(
                    env={},
                    proxy_urls=[
                        "http://proxy-1.local:8080",
                        "http://proxy-2.local:8080",
                        "http://proxy-3.local:8080",
                        "http://proxy-4.local:8080",
                        "http://proxy-5.local:8080",
                    ],
                ).proxy_endpoints,
            )
        )

        result = await transport.fetch("https://www.sofascore.com/api/v1/unique-tournament/17", timeout=5.0)

        self.assertEqual(len(result.attempts), 5)
        self.assertEqual(
            [attempt.proxy_name for attempt in result.attempts],
            ["proxy_1", "proxy_2", "proxy_3", "proxy_4", "proxy_5"],
        )
        self.assertEqual(result.status_code, 403)


class _FakeTransport(InspectorTransport):
    def __init__(self, runtime_config: RuntimeConfig, *, sleeper=None) -> None:
        super().__init__(runtime_config, sleeper=sleeper or (lambda delay: None))

    async def _execute_once(self, url: str, headers, timeout: float, proxy_url: str | None, fingerprint_profile=None):
        del url, headers, timeout, proxy_url, fingerprint_profile
        return type(
            "_RawResponse",
            (),
            {
                "resolved_url": "https://www.sofascore.com/api/v1/unique-tournament/17",
                "status_code": 403,
                "headers": {"content-type": "application/json"},
                "body_bytes": b'{"error":"forbidden"}',
            },
        )()


class _RecordingTransport(InspectorTransport):
    def __init__(self, runtime_config: RuntimeConfig, *, sleeper=None) -> None:
        super().__init__(runtime_config, sleeper=sleeper or (lambda delay: None))
        self.seen_requests: list[dict[str, object]] = []

    async def _execute_once(self, url: str, headers, timeout: float, proxy_url: str | None, fingerprint_profile=None):
        del timeout
        self.seen_requests.append(
            {
                "url": url,
                "proxy_url": proxy_url,
                "headers": dict(headers),
                "impersonate": None if fingerprint_profile is None else fingerprint_profile.impersonate,
            }
        )
        return type(
            "_RawResponse",
            (),
            {
                "resolved_url": url,
                "status_code": 200,
                "headers": {"content-type": "application/json"},
                "body_bytes": b"{}",
            },
        )()


if __name__ == "__main__":
    unittest.main()
