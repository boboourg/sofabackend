"""Tests for FetchExecutor host-fallback (N4 Fix #2, 2026-05-15).

Pins the contract for _fetch_with_host_fallback:
- when SCHEMA_INSPECTOR_FALLBACK_HOSTS is unset → transparent passthrough
- when set and primary host raises → second host attempted
- when all hosts raise → last exception propagates
- non-www.sofascore.com URLs not touched (e.g. internal endpoints)
"""

from __future__ import annotations

import os
import unittest
from typing import Any
from unittest.mock import patch

from schema_inspector.fetch_executor import _fetch_with_host_fallback
from schema_inspector.fetch_models import FetchTask


def _make_task(url: str = "https://www.sofascore.com/api/v1/event/1") -> FetchTask:
    return FetchTask(
        trace_id="trace",
        job_id="job",
        sport_slug="football",
        endpoint_pattern="/api/v1/event/{event_id}",
        source_url=url,
        timeout_profile="pilot",
        context_entity_type="event",
        context_entity_id=1,
        fetch_reason="hydrate_event_root",
        method="GET",
        request_headers={},
        timeout_seconds=10.0,
    )


class _StubTransport:
    def __init__(self, behaviour: list[Any]) -> None:
        """``behaviour`` is a list of return values or Exceptions to use in order."""

        self.behaviour = list(behaviour)
        self.calls: list[str] = []

    async def fetch(self, url: str, *, headers, timeout):  # noqa: ARG002
        self.calls.append(url)
        result = self.behaviour.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class HostFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_passthrough_when_disabled(self) -> None:
        # Empty env → fallback disabled
        with patch.dict(os.environ, {"SCHEMA_INSPECTOR_FALLBACK_HOSTS": ""}):
            transport = _StubTransport(["ok"])
            result = await _fetch_with_host_fallback(
                transport=transport,
                task=_make_task(),
            )
        self.assertEqual(result, "ok")
        self.assertEqual(len(transport.calls), 1)
        self.assertIn("www.sofascore.com", transport.calls[0])

    async def test_primary_success_no_fallback_invoked(self) -> None:
        with patch.dict(
            os.environ,
            {"SCHEMA_INSPECTOR_FALLBACK_HOSTS": "api.sofascore.com,mobile.sofascore.com"},
        ):
            transport = _StubTransport(["primary"])
            result = await _fetch_with_host_fallback(
                transport=transport,
                task=_make_task(),
            )
        self.assertEqual(result, "primary")
        self.assertEqual(len(transport.calls), 1)

    async def test_primary_fails_falls_back_to_second_host(self) -> None:
        with patch.dict(
            os.environ,
            {"SCHEMA_INSPECTOR_FALLBACK_HOSTS": "api.sofascore.com,mobile.sofascore.com"},
        ):
            transport = _StubTransport([ConnectionError("primary down"), "api-success"])
            result = await _fetch_with_host_fallback(
                transport=transport,
                task=_make_task(),
            )
        self.assertEqual(result, "api-success")
        self.assertEqual(len(transport.calls), 2)
        # First call uses primary host, second uses api host
        self.assertIn("www.sofascore.com", transport.calls[0])
        self.assertIn("api.sofascore.com", transport.calls[1])

    async def test_first_two_fail_falls_back_to_third_host(self) -> None:
        with patch.dict(
            os.environ,
            {"SCHEMA_INSPECTOR_FALLBACK_HOSTS": "api.sofascore.com,mobile.sofascore.com"},
        ):
            transport = _StubTransport(
                [
                    ConnectionError("primary down"),
                    TimeoutError("api timeout"),
                    "mobile-success",
                ]
            )
            result = await _fetch_with_host_fallback(
                transport=transport,
                task=_make_task(),
            )
        self.assertEqual(result, "mobile-success")
        self.assertEqual(len(transport.calls), 3)
        self.assertIn("mobile.sofascore.com", transport.calls[2])

    async def test_all_hosts_fail_propagates_last_exception(self) -> None:
        with patch.dict(
            os.environ,
            {"SCHEMA_INSPECTOR_FALLBACK_HOSTS": "api.sofascore.com,mobile.sofascore.com"},
        ):
            transport = _StubTransport(
                [
                    ConnectionError("primary"),
                    ConnectionError("api"),
                    ConnectionError("mobile"),
                ]
            )
            with self.assertRaises(ConnectionError) as ctx:
                await _fetch_with_host_fallback(
                    transport=transport,
                    task=_make_task(),
                )
        self.assertIn("mobile", str(ctx.exception))
        self.assertEqual(len(transport.calls), 3)

    async def test_url_without_primary_host_not_touched(self) -> None:
        """Internal endpoints (no www.sofascore.com) skip fallback entirely."""

        with patch.dict(
            os.environ,
            {"SCHEMA_INSPECTOR_FALLBACK_HOSTS": "api.sofascore.com"},
        ):
            transport = _StubTransport([ConnectionError("dead")])
            with self.assertRaises(ConnectionError):
                await _fetch_with_host_fallback(
                    transport=transport,
                    task=_make_task("http://localhost:8000/api/v1/event/1"),
                )
        # Only one attempt because the URL doesn't contain www.sofascore.com
        self.assertEqual(len(transport.calls), 1)

    async def test_duplicate_fallback_host_ignored(self) -> None:
        """If the env lists the primary host or duplicates, dedupe transparently."""

        with patch.dict(
            os.environ,
            {"SCHEMA_INSPECTOR_FALLBACK_HOSTS": "www.sofascore.com,api.sofascore.com,api.sofascore.com"},
        ):
            transport = _StubTransport([ConnectionError("primary"), "api-success"])
            result = await _fetch_with_host_fallback(
                transport=transport,
                task=_make_task(),
            )
        self.assertEqual(result, "api-success")
        # Only 2 attempts (primary + api), duplicates filtered, primary host
        # in env discarded so we don't loop back.
        self.assertEqual(len(transport.calls), 2)


if __name__ == "__main__":
    unittest.main()
