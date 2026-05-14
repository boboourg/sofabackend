"""Tests for monitoring queue signals (Phase 2).

Pins the contract:
- fetch_queue_signals_from_api emits 5 snapshots — one per known stream
- Streams not in our watchlist are ignored
- length=0 for missing stream → snapshot value=None → severity=OK
- Overrides apply to thresholds, keyed by SignalDefinition.name
- fetch_all_signals_from_api returns SLO + queue snapshots concatenated
- Either branch failing yields the other still working
"""

from __future__ import annotations

import unittest

from schema_inspector.monitoring.signal_source import (
    fetch_all_signals_from_api,
    fetch_queue_signals_from_api,
)


class _StubResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class _StubHttpClient:
    """Routes /ops/* GETs to canned responses keyed by suffix path."""

    def __init__(self, responses: dict[str, _StubResponse] | None = None) -> None:
        self.responses = responses or {}
        self.last_urls: list[str] = []

    async def get(self, url: str, *, timeout: float):
        del timeout
        self.last_urls.append(url)
        for suffix, response in self.responses.items():
            if url.endswith(suffix):
                return response
        return _StubResponse(404, {})


class FetchQueueSignalsTests(unittest.IsolatedAsyncioTestCase):
    async def test_emits_five_queue_snapshots(self) -> None:
        payload = {
            "streams": [
                {"stream": "stream:etl:hydrate", "length": 800},
                {"stream": "stream:etl:live_hot", "length": 100},
                {"stream": "stream:etl:live_warm", "length": 50},
                {"stream": "stream:etl:live_discovery", "length": 10},
                {"stream": "stream:etl:discovery", "length": 0},
            ]
        }
        client = _StubHttpClient(
            {"/ops/queues/summary": _StubResponse(200, payload)}
        )
        snapshots = await fetch_queue_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        names = {s.name for s in snapshots}
        self.assertEqual(
            names,
            {
                "hydrate_xlen",
                "live_hot_xlen",
                "live_warm_xlen",
                "live_discovery_xlen",
                "discovery_xlen",
            },
        )
        hydrate = next(s for s in snapshots if s.name == "hydrate_xlen")
        self.assertEqual(hydrate.value, 800)
        self.assertEqual(hydrate.severity, "OK")  # 800 < warn=1000

    async def test_warn_threshold_for_hydrate(self) -> None:
        payload = {
            "streams": [{"stream": "stream:etl:hydrate", "length": 2500}],
        }
        client = _StubHttpClient(
            {"/ops/queues/summary": _StubResponse(200, payload)}
        )
        snapshots = await fetch_queue_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        hydrate = next(s for s in snapshots if s.name == "hydrate_xlen")
        self.assertEqual(hydrate.value, 2500)
        # default crit=5000, warn=1000 → WARN.
        self.assertEqual(hydrate.severity, "WARN")

    async def test_crit_threshold_for_live_hot(self) -> None:
        payload = {
            "streams": [{"stream": "stream:etl:live_hot", "length": 3000}],
        }
        client = _StubHttpClient(
            {"/ops/queues/summary": _StubResponse(200, payload)}
        )
        snapshots = await fetch_queue_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        hot = next(s for s in snapshots if s.name == "live_hot_xlen")
        self.assertEqual(hot.value, 3000)
        self.assertEqual(hot.severity, "CRIT")  # > crit=2000

    async def test_missing_stream_yields_none_value_ok(self) -> None:
        payload = {"streams": []}
        client = _StubHttpClient(
            {"/ops/queues/summary": _StubResponse(200, payload)}
        )
        snapshots = await fetch_queue_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        # All 5 snapshots present, all OK with value=None.
        self.assertEqual(len(snapshots), 5)
        self.assertTrue(all(s.value is None for s in snapshots))
        self.assertTrue(all(s.severity == "OK" for s in snapshots))

    async def test_unknown_streams_ignored(self) -> None:
        payload = {
            "streams": [
                {"stream": "stream:etl:hydrate", "length": 10},
                {"stream": "stream:etl:never_heard_of", "length": 9999},
            ]
        }
        client = _StubHttpClient(
            {"/ops/queues/summary": _StubResponse(200, payload)}
        )
        snapshots = await fetch_queue_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        names = {s.name for s in snapshots}
        self.assertNotIn("never_heard_of", names)
        self.assertNotIn("never_heard_of_xlen", names)

    async def test_returns_empty_on_non_200(self) -> None:
        client = _StubHttpClient(
            {"/ops/queues/summary": _StubResponse(500)}
        )
        result = await fetch_queue_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        self.assertEqual(result, [])

    async def test_overrides_apply_to_queue_thresholds(self) -> None:
        payload = {
            "streams": [{"stream": "stream:etl:hydrate", "length": 2500}],
        }
        client = _StubHttpClient(
            {"/ops/queues/summary": _StubResponse(200, payload)}
        )
        snapshots = await fetch_queue_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
            overrides={"hydrate_xlen": {"warn": 5000, "crit": 10000}},
        )
        hydrate = next(s for s in snapshots if s.name == "hydrate_xlen")
        # 2500 < 5000 (overridden warn) → OK.
        self.assertEqual(hydrate.severity, "OK")
        self.assertEqual(hydrate.threshold_warn, 5000)

    async def test_extra_includes_stream_name(self) -> None:
        payload = {
            "streams": [{"stream": "stream:etl:hydrate", "length": 0}],
        }
        client = _StubHttpClient(
            {"/ops/queues/summary": _StubResponse(200, payload)}
        )
        snapshots = await fetch_queue_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        hydrate = next(s for s in snapshots if s.name == "hydrate_xlen")
        self.assertEqual(hydrate.extra.get("stream"), "stream:etl:hydrate")


class FetchAllSignalsTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_slo_plus_queue(self) -> None:
        live_freshness = {
            "oldest_hot_score_age_seconds": 50,
            "tier_1_blocked_rate_cumulative": 0.0,
            "tier_1_active_events": 0,
            "refresh_live_event_success_rate_5min": None,
            "slos": [],
        }
        queue_summary = {
            "streams": [
                {"stream": "stream:etl:hydrate", "length": 100},
            ]
        }
        client = _StubHttpClient(
            {
                "/ops/live-freshness": _StubResponse(200, live_freshness),
                "/ops/queues/summary": _StubResponse(200, queue_summary),
            }
        )
        snapshots = await fetch_all_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        names = {s.name for s in snapshots}
        # 3 SLO + 5 queue = 8 snapshots.
        self.assertEqual(len(snapshots), 8)
        self.assertIn("oldest_hot_score_age_seconds", names)
        self.assertIn("hydrate_xlen", names)

    async def test_slo_fail_yields_queue_only(self) -> None:
        queue_summary = {
            "streams": [{"stream": "stream:etl:hydrate", "length": 100}]
        }
        client = _StubHttpClient(
            {
                "/ops/live-freshness": _StubResponse(503),
                "/ops/queues/summary": _StubResponse(200, queue_summary),
            }
        )
        snapshots = await fetch_all_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        # SLO fetch failed → only the 5 queue snapshots returned.
        self.assertEqual(len(snapshots), 5)
        self.assertTrue(all(s.name.endswith("_xlen") for s in snapshots))


if __name__ == "__main__":
    unittest.main()
