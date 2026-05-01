from __future__ import annotations

import unittest

from schema_inspector.services.proxy_health_monitor import ProxyHealthMonitor, ProxyHealthMonitorConfig
from schema_inspector.storage.proxy_health_repository import ProxyHealthRepository, ProxyTrafficAggregate


class ProxyHealthRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_fetch_proxy_traffic_aggregates_api_request_log_by_proxy_address(self) -> None:
        connection = _FakeConnection(
            rows=[
                {
                    "proxy_address": "10.0.0.1:8080",
                    "total_requests": 20,
                    "success_count": 1,
                    "failure_count": 19,
                    "status_403_count": 19,
                    "status_404_count": 0,
                    "status_5xx_count": 0,
                    "latest_started_at": "2026-05-01T12:00:00+00:00",
                }
            ]
        )

        rows = await ProxyHealthRepository().fetch_proxy_traffic(connection, window_seconds=3600)

        self.assertEqual(connection.args, (3600,))
        self.assertIn("FROM api_request_log", connection.query)
        self.assertIn("proxy_address", connection.query)
        self.assertIn("started_at >= NOW()", connection.query)
        self.assertEqual(rows[0].proxy_address, "10.0.0.1:8080")
        self.assertEqual(rows[0].total_requests, 20)
        self.assertEqual(rows[0].success_count, 1)


class ProxyHealthMonitorTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_once_marks_low_success_proxy_unhealthy(self) -> None:
        store = _FakeProxyStateStore()
        monitor = ProxyHealthMonitor(
            repository=_FakeProxyHealthRepository(
                ProxyTrafficAggregate(
                    proxy_address="10.0.0.1:8080",
                    total_requests=20,
                    success_count=0,
                    failure_count=20,
                    status_403_count=20,
                    status_404_count=0,
                    status_5xx_count=0,
                    latest_started_at=None,
                )
            ),
            state_store=store,
            config=ProxyHealthMonitorConfig(
                window_seconds=3600,
                min_requests=20,
                success_rate_threshold=0.05,
                unhealthy_ttl_seconds=1800,
                excluded_address_substrings=(),
            ),
            now_ms_factory=lambda: 1_000,
        )

        report = await monitor.run_once(_FakeConnectionContext())

        self.assertEqual(report.observed, 1)
        self.assertEqual(report.marked_unhealthy, 1)
        self.assertEqual(store.failures, [("10.0.0.1:8080", 403, "traffic_unhealthy", 1_000, 1_800_000)])

    async def test_run_once_never_marks_excluded_rotating_proxy(self) -> None:
        store = _FakeProxyStateStore()
        monitor = ProxyHealthMonitor(
            repository=_FakeProxyHealthRepository(
                ProxyTrafficAggregate(
                    proxy_address="proxy.smartproxy.net:3120",
                    total_requests=200,
                    success_count=0,
                    failure_count=200,
                    status_403_count=200,
                    status_404_count=0,
                    status_5xx_count=0,
                    latest_started_at=None,
                )
            ),
            state_store=store,
            config=ProxyHealthMonitorConfig(
                window_seconds=3600,
                min_requests=20,
                success_rate_threshold=0.05,
                unhealthy_ttl_seconds=1800,
                excluded_address_substrings=("smartproxy",),
            ),
            now_ms_factory=lambda: 1_000,
        )

        report = await monitor.run_once(_FakeConnectionContext())

        self.assertEqual(report.observed, 1)
        self.assertEqual(report.excluded, 1)
        self.assertEqual(report.marked_unhealthy, 0)
        self.assertEqual(store.failures, [])


class _FakeConnection:
    def __init__(self, *, rows: list[dict[str, object]]) -> None:
        self.rows = rows
        self.query = ""
        self.args: tuple[object, ...] = ()

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        self.query = query
        self.args = args
        return self.rows


class _FakeConnectionContext:
    async def __aenter__(self) -> _FakeConnection:
        return _FakeConnection(rows=[])

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb


class _FakeProxyHealthRepository:
    def __init__(self, *rows: ProxyTrafficAggregate) -> None:
        self.rows = rows
        self.window_seconds: int | None = None

    async def fetch_proxy_traffic(self, connection, *, window_seconds: int) -> tuple[ProxyTrafficAggregate, ...]:
        del connection
        self.window_seconds = window_seconds
        return tuple(self.rows)


class _FakeProxyStateStore:
    def __init__(self) -> None:
        self.failures: list[tuple[str, int | None, str | None, int, int]] = []

    def record_failure(
        self,
        proxy_id: str,
        *,
        status_code: int | None,
        challenge_reason: str | None,
        observed_at_ms: int,
        cooldown_ms: int,
    ):
        self.failures.append((proxy_id, status_code, challenge_reason, observed_at_ms, cooldown_ms))


if __name__ == "__main__":
    unittest.main()
