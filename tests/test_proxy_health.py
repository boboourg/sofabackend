from __future__ import annotations

import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

from schema_inspector.proxy_health import (
    ProxyHealthResult,
    load_pool_runtime_config,
    probe_proxy_endpoint,
)
from schema_inspector.runtime import ProxyEndpoint, RuntimeConfig, TransportAttempt, TransportResult


class ProxyHealthTests(unittest.IsolatedAsyncioTestCase):
    def test_proxy_health_script_bootstraps_repo_imports(self) -> None:
        script_path = Path(__file__).resolve().parent.parent / "scripts" / "ops" / "proxy_health_check.py"

        completed = subprocess.run(
            [sys.executable, str(script_path), "--help"],
            cwd=script_path.parents[2],
            capture_output=True,
            text=True,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("--pool", completed.stdout)

    def test_load_pool_runtime_config_reads_live_pool_by_default(self) -> None:
        config = load_pool_runtime_config(
            "live",
            env={
                "SCHEMA_INSPECTOR_PROXY_URLS": "http://live-1.local:8080,http://live-2.local:8080",
            },
        )

        self.assertEqual([item.url for item in config.proxy_endpoints], ["http://live-1.local:8080", "http://live-2.local:8080"])

    def test_load_pool_runtime_config_reads_historical_pool(self) -> None:
        config = load_pool_runtime_config(
            "historical",
            env={
                "SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS": "http://hist-1.local:8080,http://hist-2.local:8080",
            },
        )

        self.assertEqual([item.url for item in config.proxy_endpoints], ["http://hist-1.local:8080", "http://hist-2.local:8080"])

    def test_load_pool_runtime_config_uses_historical_factory(self) -> None:
        with patch("schema_inspector.proxy_health.load_historical_runtime_config") as loader:
            loader.return_value = RuntimeConfig()

            config = load_pool_runtime_config(
                "historical",
                env={"SCHEMA_INSPECTOR_HISTORICAL_PROXY_URLS": "http://hist.local:8080"},
            )

        self.assertIs(config, loader.return_value)
        loader.assert_called_once()

    async def test_probe_proxy_endpoint_marks_healthy_for_clean_200(self) -> None:
        endpoint = ProxyEndpoint(name="proxy_1", url="http://proxy-1.local:8080")
        base_config = RuntimeConfig(proxy_endpoints=(endpoint,))

        result = await probe_proxy_endpoint(
            endpoint=endpoint,
            base_config=base_config,
            url="https://www.sofascore.com/api/v1/sport/football/events/live",
            timeout=5.0,
            transport_factory=_transport_factory(
                "http://proxy-1.local:8080",
                status_code=200,
                challenge_reason=None,
            ),
        )

        self.assertEqual(result.verdict, "healthy")
        self.assertEqual(result.status_code, 200)
        self.assertEqual(result.proxy_name, "proxy_1")

    async def test_probe_proxy_endpoint_marks_dead_for_403(self) -> None:
        endpoint = ProxyEndpoint(name="proxy_2", url="http://proxy-2.local:8080")
        base_config = RuntimeConfig(proxy_endpoints=(endpoint,))

        result = await probe_proxy_endpoint(
            endpoint=endpoint,
            base_config=base_config,
            url="https://www.sofascore.com/api/v1/sport/football/events/live",
            timeout=5.0,
            transport_factory=_transport_factory(
                "http://proxy-2.local:8080",
                status_code=403,
                challenge_reason="access_denied",
            ),
        )

        self.assertEqual(result.verdict, "dead")
        self.assertEqual(result.status_code, 403)
        self.assertEqual(result.challenge_reason, "access_denied")

    async def test_probe_proxy_endpoint_marks_dead_for_transport_error(self) -> None:
        endpoint = ProxyEndpoint(name="proxy_3", url="http://proxy-3.local:8080")
        base_config = RuntimeConfig(proxy_endpoints=(endpoint,))

        result = await probe_proxy_endpoint(
            endpoint=endpoint,
            base_config=base_config,
            url="https://www.sofascore.com/api/v1/sport/football/events/live",
            timeout=5.0,
            transport_factory=_error_transport_factory("boom"),
        )

        self.assertEqual(result.verdict, "dead")
        self.assertIsNone(result.status_code)
        self.assertEqual(result.error, "boom")

    async def test_probe_proxy_endpoint_uses_single_attempt_probe_budget(self) -> None:
        endpoint = ProxyEndpoint(name="proxy_4", url="http://proxy-4.local:8080")
        base_config = RuntimeConfig(proxy_endpoints=(endpoint,))

        observed = {}

        def _factory(runtime_config: RuntimeConfig) -> _FakeTransport:
            observed["max_attempts"] = runtime_config.retry_policy.max_attempts
            observed["challenge_max_attempts"] = runtime_config.retry_policy.challenge_max_attempts
            observed["network_error_max_attempts"] = runtime_config.retry_policy.network_error_max_attempts
            observed["backoff_seconds"] = runtime_config.retry_policy.backoff_seconds
            return _FakeTransport(runtime_config, status_code=200, challenge_reason=None)

        await probe_proxy_endpoint(
            endpoint=endpoint,
            base_config=base_config,
            url="https://www.sofascore.com/api/v1/sport/football/events/live",
            timeout=5.0,
            transport_factory=_factory,
        )

        self.assertEqual(observed, {
            "max_attempts": 1,
            "challenge_max_attempts": 1,
            "network_error_max_attempts": 1,
            "backoff_seconds": 0.0,
        })


class _FakeTransport:
    def __init__(self, runtime_config: RuntimeConfig, *, status_code: int, challenge_reason: str | None, error: str | None = None) -> None:
        self.runtime_config = runtime_config
        self._status_code = status_code
        self._challenge_reason = challenge_reason
        self._error = error

    async def fetch(self, url: str, *, headers=None, timeout: float = 20.0) -> TransportResult:
        del headers, timeout
        if self._error is not None:
            raise RuntimeError(self._error)
        endpoint = self.runtime_config.proxy_endpoints[0]
        return TransportResult(
            resolved_url=url,
            status_code=self._status_code,
            headers={"content-type": "application/json"},
            body_bytes=b"{}",
            attempts=(TransportAttempt(1, endpoint.name, self._status_code, None, self._challenge_reason),),
            final_proxy_name=endpoint.name,
            challenge_reason=self._challenge_reason,
        )

    async def close(self) -> None:
        return None


def _transport_factory(expected_proxy_url: str, *, status_code: int, challenge_reason: str | None):
    def _factory(runtime_config: RuntimeConfig) -> _FakeTransport:
        endpoint = runtime_config.proxy_endpoints[0]
        assert endpoint.url == expected_proxy_url
        return _FakeTransport(runtime_config, status_code=status_code, challenge_reason=challenge_reason)

    return _factory


def _error_transport_factory(error: str):
    def _factory(runtime_config: RuntimeConfig) -> _FakeTransport:
        return _FakeTransport(runtime_config, status_code=0, challenge_reason=None, error=error)

    return _factory


if __name__ == "__main__":
    unittest.main()
