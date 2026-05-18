"""Tests for the /ws/health monitoring endpoint exposed by the
mirror WS server.

The endpoint is consumed by:
  * external uptime checks (nginx → return JSON status code)
  * internal /ops/health rollup (will pull this URL and join into
    the existing health report)

It must be cheap (no DB hit), bounded (no unbounded reads), and
return JSON with stable schema.
"""
from __future__ import annotations
import json
import unittest
from typing import Any


class FakeAsyncRedis:
    """Minimal async-redis-shaped fake for the health probe."""

    def __init__(self, *, ping_ok: bool = True) -> None:
        self._ping_ok = ping_ok
        self.pubsub_calls = 0

    async def ping(self) -> bool:
        if not self._ping_ok:
            raise ConnectionError("redis down")
        return True

    def pubsub(self) -> "FakePubsub":
        self.pubsub_calls += 1
        return FakePubsub()


class FakePubsub:
    async def subscribe(self, *_a: Any) -> None:
        pass

    async def unsubscribe(self, *_a: Any) -> None:
        pass

    async def get_message(self, **_kw: Any) -> None:
        return None

    async def close(self) -> None:
        pass


class HealthEndpointTests(unittest.TestCase):
    """Plain sync TestClient tests — we do NOT use `with client:`
    because that would trigger FastAPI lifespan (which boots the
    Mirror server's redis pubsub listener) and hang on the test
    fakes. The /ws/health route is intentionally lifecycle-free."""

    def _client(self, *, ping_ok: bool = True) -> Any:
        from fastapi.testclient import TestClient
        from schema_inspector.services.ws_server_service import build_app

        return TestClient(build_app(FakeAsyncRedis(ping_ok=ping_ok)))

    def test_health_returns_200_with_status_ok(self) -> None:
        client = self._client()
        r = client.get("/ws/health")
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["status"], "ok")

    def test_health_includes_redis_ping_result(self) -> None:
        client = self._client()
        r = client.get("/ws/health")
        body = r.json()
        self.assertEqual(body["redis"], "ok")

    def test_health_reports_redis_down(self) -> None:
        client = self._client(ping_ok=False)
        r = client.get("/ws/health")
        # 200 with degraded indicator (uptime checks don't alert on
        # transient redis hiccups; redis field has the real signal).
        self.assertEqual(r.status_code, 200)
        body = r.json()
        self.assertEqual(body["redis"], "down")
        self.assertEqual(body["status"], "degraded")

    def test_health_has_stable_schema(self) -> None:
        client = self._client()
        r = client.get("/ws/health")
        body = r.json()
        for key in (
            "status",
            "redis",
            "connections",
            "subscriptions",
            "fanout_channels",
            "uptime_seconds",
            "version",
        ):
            self.assertIn(key, body, f"missing required field {key!r}")
        self.assertIsInstance(body["connections"], int)
        self.assertIsInstance(body["subscriptions"], int)
        self.assertIsInstance(body["fanout_channels"], list)
        self.assertIsInstance(body["uptime_seconds"], (int, float))

    def test_root_returns_404_or_index(self) -> None:
        """We mount /ws/health, not / — health probes must hit the
        explicit path."""
        client = self._client()
        r = client.get("/")
        self.assertIn(r.status_code, (404, 200))


if __name__ == "__main__":
    unittest.main()
