"""Tests for monitoring/signal_source.py — pull SLOs from /ops/live-freshness.

Pins the contract:
- All three Phase 1 signals are emitted on a successful 200 response
- Empty list returned on non-200, network error, or invalid JSON
- ``tier_1_blocked_rate_cumulative`` is None when ``tier_1_active_events == 0``
  (cumulative ratio is meaningless without active polling)
- Overrides apply to thresholds, not to definition names
- Notes from /ops/live-freshness slos[] propagate to snapshots
"""

from __future__ import annotations

import unittest
from datetime import datetime, timezone

from schema_inspector.monitoring.signal_source import fetch_slo_signals_from_api
from schema_inspector.monitoring.signals import (
    SIGNAL_OLDEST_HOT_AGE,
    SIGNAL_REFRESH_SUCCESS,
    SIGNAL_TIER_1_BLOCKED,
    SIGNAL_TIER_1_QUARANTINED,
)


class _StubResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class _StubHttpClient:
    def __init__(self, response: _StubResponse | None = None, raise_exc: Exception | None = None) -> None:
        self.response = response
        self.raise_exc = raise_exc
        self.last_url: str | None = None
        self.last_timeout: float | None = None

    async def get(self, url: str, *, timeout: float):
        self.last_url = url
        self.last_timeout = timeout
        if self.raise_exc:
            raise self.raise_exc
        return self.response


class FetchSloSignalsTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_four_snapshots_on_success(self) -> None:
        payload = {
            "oldest_hot_score_age_seconds": 96,
            "tier_1_blocked_rate_cumulative": 0.81,
            "tier_1_active_events": 5,
            "refresh_live_event_success_rate_5min": None,
            "tier_1_quarantined_events": 2,
            "slos": [],
        }
        client = _StubHttpClient(response=_StubResponse(200, payload))
        snapshots = await fetch_slo_signals_from_api(
            base_url="http://example/api",
            http_client=client,
            timeout_seconds=2.0,
        )
        names = [s.name for s in snapshots]
        self.assertEqual(
            set(names),
            {
                SIGNAL_OLDEST_HOT_AGE.name,
                SIGNAL_TIER_1_BLOCKED.name,
                SIGNAL_REFRESH_SUCCESS.name,
                SIGNAL_TIER_1_QUARANTINED.name,
            },
        )
        self.assertEqual(client.last_url, "http://example/api/ops/live-freshness")

    async def test_returns_empty_on_non_200(self) -> None:
        client = _StubHttpClient(response=_StubResponse(503))
        result = await fetch_slo_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        self.assertEqual(result, [])

    async def test_returns_empty_on_network_exception(self) -> None:
        client = _StubHttpClient(raise_exc=ConnectionError("dead"))
        result = await fetch_slo_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        self.assertEqual(result, [])

    async def test_tier_1_blocked_is_none_when_no_active_events(self) -> None:
        payload = {
            "oldest_hot_score_age_seconds": 50,
            "tier_1_blocked_rate_cumulative": 0.95,  # still 95% in cumulative
            "tier_1_active_events": 0,
            "refresh_live_event_success_rate_5min": 0.99,
            "slos": [],
        }
        client = _StubHttpClient(response=_StubResponse(200, payload))
        snapshots = await fetch_slo_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        tier_1 = next(s for s in snapshots if s.name == SIGNAL_TIER_1_BLOCKED.name)
        # No active events → cumulative ratio reported as None → severity OK
        self.assertIsNone(tier_1.value)
        self.assertEqual(tier_1.severity, "OK")

    async def test_overrides_apply_thresholds(self) -> None:
        payload = {
            "oldest_hot_score_age_seconds": 250,  # in WARN with defaults
            "tier_1_blocked_rate_cumulative": 0.0,
            "tier_1_active_events": 1,
            "refresh_live_event_success_rate_5min": 1.0,
            "slos": [],
        }
        client = _StubHttpClient(response=_StubResponse(200, payload))
        snapshots = await fetch_slo_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
            overrides={
                SIGNAL_OLDEST_HOT_AGE.name: {"warn": 300, "crit": 600},
            },
        )
        oldest = next(s for s in snapshots if s.name == SIGNAL_OLDEST_HOT_AGE.name)
        # 250 < 300 (overridden warn) → OK
        self.assertEqual(oldest.severity, "OK")
        self.assertEqual(oldest.threshold_warn, 300)
        self.assertEqual(oldest.threshold_crit, 600)

    async def test_note_from_slos_propagates(self) -> None:
        payload = {
            "oldest_hot_score_age_seconds": 500,
            "tier_1_blocked_rate_cumulative": 0.0,
            "tier_1_active_events": 0,
            "refresh_live_event_success_rate_5min": None,
            "slos": [
                {
                    "name": SIGNAL_OLDEST_HOT_AGE.name,
                    "breached": True,
                    "note": "stale entries in zset:live:hot",
                },
            ],
        }
        client = _StubHttpClient(response=_StubResponse(200, payload))
        snapshots = await fetch_slo_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        oldest = next(s for s in snapshots if s.name == SIGNAL_OLDEST_HOT_AGE.name)
        self.assertIn("zset:live:hot", oldest.note or "")

    async def test_url_strips_trailing_slash(self) -> None:
        client = _StubHttpClient(response=_StubResponse(200, {}))
        await fetch_slo_signals_from_api(
            base_url="http://example/",
            http_client=client,
            timeout_seconds=2.0,
        )
        self.assertEqual(client.last_url, "http://example/ops/live-freshness")

    async def test_timestamp_uses_injected_now(self) -> None:
        now = datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
        client = _StubHttpClient(
            response=_StubResponse(
                200,
                {
                    "oldest_hot_score_age_seconds": 10,
                    "tier_1_blocked_rate_cumulative": 0.0,
                    "tier_1_active_events": 0,
                    "refresh_live_event_success_rate_5min": None,
                    "slos": [],
                },
            )
        )
        snapshots = await fetch_slo_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
            now=now,
        )
        self.assertTrue(all(s.timestamp == now for s in snapshots))


if __name__ == "__main__":
    unittest.main()
