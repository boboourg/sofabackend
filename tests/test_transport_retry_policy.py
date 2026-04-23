from __future__ import annotations

import asyncio
import unittest

from schema_inspector.runtime import RuntimeConfig, RetryPolicy, TlsPolicy, load_runtime_config
from schema_inspector.transport import InspectorTransport


class TransportRetryPolicyTests(unittest.TestCase):
    def test_proxy_cooldown_decisions_follow_retry_policy_and_challenge_reason(self) -> None:
        transport = InspectorTransport(load_runtime_config(env={}, proxy_urls=["http://proxy-1.local:8080"]))

        self.assertTrue(transport._should_cooldown_proxy(429, None))
        self.assertTrue(transport._should_cooldown_proxy(403, "access_denied"))
        self.assertTrue(transport._should_cooldown_proxy(503, None))
        self.assertFalse(transport._should_cooldown_proxy(404, None))
        self.assertFalse(transport._should_cooldown_proxy(200, None))

    def test_proxy_pool_respects_cooldown_even_when_only_one_proxy_exists(self) -> None:
        from schema_inspector.proxy import ProxyPool
        from schema_inspector.runtime import ProxyEndpoint

        now = 100.0
        pool = ProxyPool(
            (ProxyEndpoint(name="proxy-1", url="http://proxy-1.local:8080", cooldown_seconds=30.0),),
            clock=lambda: now,
        )

        self.assertEqual(pool.acquire().name, "proxy-1")
        pool.record_failure("proxy-1")
        self.assertIsNone(pool.acquire())

    def test_transport_retries_access_denied_across_extended_proxy_budget(self) -> None:
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

        result = asyncio.run(transport.fetch("https://www.sofascore.com/api/v1/unique-tournament/17", timeout=5.0))

        self.assertEqual(len(result.attempts), 5)
        self.assertEqual(
            [attempt.proxy_name for attempt in result.attempts],
            ["proxy_1", "proxy_2", "proxy_3", "proxy_4", "proxy_5"],
        )
        self.assertEqual(result.status_code, 403)


class _FakeTransport(InspectorTransport):
    def __init__(self, runtime_config: RuntimeConfig) -> None:
        super().__init__(runtime_config, sleeper=lambda delay: None)

    async def _execute_once(self, url: str, headers, timeout: float, proxy_url: str | None):
        del url, headers, timeout, proxy_url
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


if __name__ == "__main__":
    unittest.main()
