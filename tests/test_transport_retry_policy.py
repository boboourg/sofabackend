from __future__ import annotations

import asyncio
import time
import unittest

from schema_inspector.runtime import FingerprintProfile, RuntimeConfig, RetryPolicy, TlsPolicy, load_runtime_config
from schema_inspector.transport import InspectorTransport


class HistoricalRuntimeConfigTests(unittest.TestCase):
    def _load_loader(self):
        import schema_inspector.runtime as runtime_module

        loader = getattr(runtime_module, "load_historical_runtime_config", None)
        self.assertIsNotNone(loader, "schema_inspector.runtime.load_historical_runtime_config is missing")
        return runtime_module, loader

    def test_load_historical_runtime_config_uses_historical_proxy_urls(self) -> None:
        runtime_module, loader = self._load_loader()

        self.assertEqual(
            getattr(runtime_module, "HISTORICAL_PROXY_ENV_KEY", None),
            "SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS",
        )

        config = loader(
            env={
                "SCHEMA_INSPECTOR_PROXY_URLS": "http://live-1.local:8080,http://live-2.local:8080",
                "SCHEMA_INSPECTOR_HISTORICAL_PROXY_URL": "http://hist-1.local:8080",
                "SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS": "http://hist-2.local:8080,http://hist-3.local:8080",
            }
        )

        self.assertEqual(
            [endpoint.url for endpoint in config.proxy_endpoints],
            [
                "http://hist-1.local:8080",
                "http://hist-2.local:8080",
                "http://hist-3.local:8080",
            ],
        )

    def test_load_historical_runtime_config_default_overrides(self) -> None:
        _, loader = self._load_loader()

        config = loader(
            env={
                "SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS": "http://hist-1.local:8080,http://hist-2.local:8080",
            }
        )

        self.assertEqual(config.proxy_request_cooldown_seconds, 3.0)
        self.assertEqual(config.proxy_request_jitter_seconds, 2.0)
        self.assertEqual(config.retry_policy.max_attempts, 1)
        self.assertEqual(config.retry_policy.challenge_max_attempts, 1)
        self.assertEqual(config.retry_policy.network_error_max_attempts, 2)
        self.assertEqual(config.retry_policy.backoff_seconds, 2.0)

    def test_load_historical_runtime_config_env_overrides(self) -> None:
        _, loader = self._load_loader()

        config = loader(
            env={
                "SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS": "http://hist-1.local:8080",
                "SCHEMA_INSPECTOR_HISTORICAL_PROXY_REQUEST_COOLDOWN_SECONDS": "4.5",
                "SCHEMA_INSPECTOR_HISTORICAL_PROXY_REQUEST_JITTER_SECONDS": "2.5",
                "SCHEMA_INSPECTOR_HISTORICAL_MAX_ATTEMPTS": "2",
                "SCHEMA_INSPECTOR_HISTORICAL_CHALLENGE_MAX_ATTEMPTS": "3",
                "SCHEMA_INSPECTOR_HISTORICAL_NETWORK_ERROR_MAX_ATTEMPTS": "5",
                "SCHEMA_INSPECTOR_HISTORICAL_BACKOFF_SECONDS": "6.0",
            }
        )

        self.assertEqual(config.proxy_request_cooldown_seconds, 4.5)
        self.assertEqual(config.proxy_request_jitter_seconds, 2.5)
        self.assertEqual(config.retry_policy.max_attempts, 2)
        self.assertEqual(config.retry_policy.challenge_max_attempts, 3)
        self.assertEqual(config.retry_policy.network_error_max_attempts, 5)
        self.assertEqual(config.retry_policy.backoff_seconds, 6.0)

    def test_load_historical_runtime_config_requires_dedicated_pool(self) -> None:
        _, loader = self._load_loader()

        with self.assertRaisesRegex(RuntimeError, "SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS"):
            loader(env={})

    def test_load_historical_runtime_config_singular_fallback(self) -> None:
        _, loader = self._load_loader()

        config = loader(
            env={
                "SCHEMA_INSPECTOR_HISTORICAL_PROXY_URL": "http://hist-1.local:8080",
            }
        )

        self.assertEqual([endpoint.url for endpoint in config.proxy_endpoints], ["http://hist-1.local:8080"])

    def test_load_historical_runtime_config_isolation_from_live_and_structure(self) -> None:
        _, loader = self._load_loader()

        config = loader(
            env={
                "SCHEMA_INSPECTOR_PROXY_URLS": "http://live-1.local:8080",
                "SCHEMA_INSPECTOR_STRUCTURE_PROXY_URLS": "http://structure-1.local:8080",
                "SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS": "http://historical-1.local:8080,http://historical-2.local:8080",
            }
        )

        self.assertEqual(
            [endpoint.url for endpoint in config.proxy_endpoints],
            [
                "http://historical-1.local:8080",
                "http://historical-2.local:8080",
            ],
        )


class TransportRetryPolicyTests(unittest.IsolatedAsyncioTestCase):
    def test_runtime_fingerprint_profiles_use_supported_impersonation_values(self) -> None:
        config = load_runtime_config(env={})
        supported = {"chrome104", "chrome107", "chrome110", "edge101", "safari15_5", "safari15_3"}
        self.assertTrue(config.fingerprint_profiles)
        self.assertTrue({profile.impersonate for profile in config.fingerprint_profiles}.issubset(supported))

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

    async def test_transport_result_records_proxy_address_without_credentials(self) -> None:
        transport = _RecordingTransport(
            load_runtime_config(
                env={
                    "SCHEMA_INSPECTOR_PROXY_URLS": "http://user:secret@10.10.10.10:12345",
                    "SCHEMA_INSPECTOR_PROXY_REQUEST_COOLDOWN_SECONDS": "0",
                    "SCHEMA_INSPECTOR_PROXY_REQUEST_JITTER_SECONDS": "0",
                }
            ),
            sleeper=lambda delay: None,
        )

        result = await transport.fetch("https://www.sofascore.com/api/v1/event/1", timeout=5.0)

        self.assertEqual(result.final_proxy_name, "proxy_1")
        self.assertEqual(result.final_proxy_address, "10.10.10.10:12345")
        self.assertEqual(result.attempts[0].proxy_address, "10.10.10.10:12345")
        self.assertNotIn("secret", result.final_proxy_address)

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

    def test_session_kwargs_include_only_explicit_tls_overrides(self) -> None:
        transport = InspectorTransport(
            RuntimeConfig(
                tls_policy=TlsPolicy(
                    impersonate="chrome110",
                    verify=True,
                    http_version="2",
                ),
            )
        )

        kwargs = transport._session_kwargs(None)

        self.assertEqual(kwargs["impersonate"], "chrome110")
        self.assertEqual(kwargs["verify"], True)
        self.assertEqual(kwargs["http_version"], "2")
        self.assertNotIn("ja3", kwargs)
        self.assertNotIn("akamai", kwargs)
        self.assertNotIn("extra_fp", kwargs)

    def test_session_kwargs_prefer_profile_tls_overrides_and_preserve_false(self) -> None:
        transport = InspectorTransport(
            RuntimeConfig(
                tls_policy=TlsPolicy(
                    impersonate="chrome110",
                    verify=True,
                    http_version="2",
                    ja3="policy-ja3",
                ),
            )
        )
        profile = FingerprintProfile(
            name="profile-a",
            impersonate="chrome107",
            user_agent="ua",
            accept_language="en-US",
            sec_ch_ua='"Chromium";v="107"',
            sec_ch_ua_mobile="?0",
            sec_ch_ua_platform='"Windows"',
            referer="https://www.sofascore.com/",
            verify=False,
            akamai="profile-akamai",
        )

        kwargs = transport._session_kwargs("http://proxy-1.local:8080", fingerprint_profile=profile)

        self.assertEqual(kwargs["impersonate"], "chrome107")
        self.assertEqual(kwargs["verify"], False)
        self.assertEqual(kwargs["http_version"], "2")
        self.assertEqual(kwargs["ja3"], "policy-ja3")
        self.assertEqual(kwargs["akamai"], "profile-akamai")
        self.assertEqual(
            kwargs["proxies"],
            {"http": "http://proxy-1.local:8080", "https": "http://proxy-1.local:8080"},
        )

    def test_session_key_distinguishes_profiles_with_same_impersonate(self) -> None:
        transport = InspectorTransport(RuntimeConfig())
        first = FingerprintProfile(
            name="profile-a",
            impersonate="chrome110",
            user_agent="ua-a",
            accept_language="en-US",
            sec_ch_ua='"Chromium";v="110"',
            sec_ch_ua_mobile="?0",
            sec_ch_ua_platform='"Windows"',
            referer="https://www.sofascore.com/",
        )
        second = FingerprintProfile(
            name="profile-b",
            impersonate="chrome110",
            user_agent="ua-b",
            accept_language="en-GB",
            sec_ch_ua='"Chromium";v="110"',
            sec_ch_ua_mobile="?0",
            sec_ch_ua_platform='"Windows"',
            referer="https://www.sofascore.com/football",
        )

        self.assertNotEqual(
            transport._session_key("http://proxy-1.local:8080", fingerprint_profile=first),
            transport._session_key("http://proxy-1.local:8080", fingerprint_profile=second),
        )


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
