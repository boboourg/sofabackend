"""Tests for monitoring job signals (Phase 3).

Pins the contract:
- fetch_job_signals_from_api emits 3 snapshots from /ops/jobs/runs
- ``failed_jobs_15min`` counts rows with status='failed' inside window
- ``retry_rate_15min`` = retry_scheduled / total in window
- ``no_recent_jobs_age_seconds`` = age of max(started_at)
- Empty payload → snapshots with value=None or 0 + severity OK
- HTTP error → empty list
- fetch_all_signals_from_api respects ``include_job_signals``
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from schema_inspector.monitoring.signal_source import (
    fetch_all_signals_from_api,
    fetch_job_signals_from_api,
)


class _StubResponse:
    def __init__(self, status_code: int, payload: dict | None = None) -> None:
        self.status_code = status_code
        self._payload = payload or {}

    def json(self) -> dict:
        return self._payload


class _StubHttpClient:
    def __init__(self, responses: dict[str, _StubResponse] | None = None) -> None:
        self.responses = responses or {}
        self.last_urls: list[str] = []

    async def get(self, url: str, *, timeout: float):
        del timeout
        self.last_urls.append(url)
        for suffix, response in self.responses.items():
            if url.endswith(suffix) or suffix in url:
                return response
        return _StubResponse(404, {})


def _job_run(*, status: str, started_at: datetime, **extra) -> dict:
    base = {
        "status": status,
        "started_at": started_at.isoformat(),
        "job_type": "live_dispatch",
        "sport_slug": "football",
        "entity_type": "event",
        "entity_id": "1",
    }
    base.update(extra)
    return base


class FetchJobSignalsTests(unittest.IsolatedAsyncioTestCase):
    async def test_emits_three_signals(self) -> None:
        now = datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
        payload = {
            "jobRuns": [
                _job_run(status="succeeded", started_at=now - timedelta(minutes=1)),
                _job_run(status="failed", started_at=now - timedelta(minutes=2)),
                _job_run(status="retry_scheduled", started_at=now - timedelta(minutes=3)),
            ]
        }
        client = _StubHttpClient(
            {"/ops/jobs/runs": _StubResponse(200, payload)}
        )
        snapshots = await fetch_job_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
            now=now,
        )
        names = {s.name for s in snapshots}
        self.assertEqual(
            names,
            {
                "failed_jobs_15min",
                "retry_rate_15min",
                "no_recent_jobs_age_seconds",
            },
        )

    async def test_failed_count_only_within_window(self) -> None:
        now = datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
        payload = {
            "jobRuns": [
                _job_run(status="failed", started_at=now - timedelta(minutes=5)),
                _job_run(status="failed", started_at=now - timedelta(minutes=10)),
                # Outside 15-min window — must NOT count:
                _job_run(status="failed", started_at=now - timedelta(minutes=20)),
            ]
        }
        client = _StubHttpClient(
            {"/ops/jobs/runs": _StubResponse(200, payload)}
        )
        snapshots = await fetch_job_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
            now=now,
        )
        failed = next(s for s in snapshots if s.name == "failed_jobs_15min")
        self.assertEqual(failed.value, 2)

    async def test_retry_rate_is_share_of_in_window_total(self) -> None:
        now = datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
        payload = {
            "jobRuns": [
                _job_run(status="succeeded", started_at=now - timedelta(minutes=1)),
                _job_run(status="succeeded", started_at=now - timedelta(minutes=2)),
                _job_run(status="retry_scheduled", started_at=now - timedelta(minutes=3)),
                _job_run(status="retry_scheduled", started_at=now - timedelta(minutes=4)),
            ]
        }
        client = _StubHttpClient(
            {"/ops/jobs/runs": _StubResponse(200, payload)}
        )
        snapshots = await fetch_job_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
            now=now,
        )
        rate = next(s for s in snapshots if s.name == "retry_rate_15min")
        self.assertAlmostEqual(rate.value, 0.5, places=3)
        # default warn=0.02, crit=0.05 — 0.5 → CRIT
        self.assertEqual(rate.severity, "CRIT")

    async def test_no_recent_jobs_age_from_max_started_at(self) -> None:
        now = datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
        payload = {
            "jobRuns": [
                _job_run(status="succeeded", started_at=now - timedelta(seconds=45)),
                _job_run(status="succeeded", started_at=now - timedelta(minutes=10)),
            ]
        }
        client = _StubHttpClient(
            {"/ops/jobs/runs": _StubResponse(200, payload)}
        )
        snapshots = await fetch_job_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
            now=now,
        )
        age = next(s for s in snapshots if s.name == "no_recent_jobs_age_seconds")
        self.assertEqual(age.value, 45)
        self.assertEqual(age.severity, "OK")  # 45 < warn=300

    async def test_no_recent_jobs_crit_when_old(self) -> None:
        now = datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
        payload = {
            "jobRuns": [
                _job_run(status="succeeded", started_at=now - timedelta(minutes=15)),
            ]
        }
        client = _StubHttpClient(
            {"/ops/jobs/runs": _StubResponse(200, payload)}
        )
        snapshots = await fetch_job_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
            now=now,
        )
        age = next(s for s in snapshots if s.name == "no_recent_jobs_age_seconds")
        self.assertGreaterEqual(age.value, 900)  # 15 min in seconds
        # default crit=600 → CRIT.
        self.assertEqual(age.severity, "CRIT")

    async def test_empty_payload_yields_safe_signals(self) -> None:
        now = datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
        client = _StubHttpClient(
            {"/ops/jobs/runs": _StubResponse(200, {"jobRuns": []})}
        )
        snapshots = await fetch_job_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
            now=now,
        )
        failed = next(s for s in snapshots if s.name == "failed_jobs_15min")
        rate = next(s for s in snapshots if s.name == "retry_rate_15min")
        age = next(s for s in snapshots if s.name == "no_recent_jobs_age_seconds")
        self.assertEqual(failed.value, 0)
        # No rows → rate undefined → None → severity OK (no alert on missing data)
        self.assertIsNone(rate.value)
        self.assertEqual(rate.severity, "OK")
        self.assertIsNone(age.value)

    async def test_http_error_returns_empty(self) -> None:
        client = _StubHttpClient(
            {"/ops/jobs/runs": _StubResponse(503)}
        )
        result = await fetch_job_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        self.assertEqual(result, [])

    async def test_z_suffix_iso_timestamp_parsed(self) -> None:
        now = datetime(2026, 5, 14, 12, 0, 0, tzinfo=timezone.utc)
        payload = {
            "jobRuns": [
                {
                    "status": "failed",
                    "started_at": "2026-05-14T11:55:00Z",
                    "job_type": "x",
                    "sport_slug": "y",
                    "entity_type": "event",
                    "entity_id": "1",
                },
            ]
        }
        client = _StubHttpClient(
            {"/ops/jobs/runs": _StubResponse(200, payload)}
        )
        snapshots = await fetch_job_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
            now=now,
        )
        failed = next(s for s in snapshots if s.name == "failed_jobs_15min")
        self.assertEqual(failed.value, 1)


class FetchAllJobInclusionTests(unittest.IsolatedAsyncioTestCase):
    async def test_job_signals_opt_in_off_by_default(self) -> None:
        client = _StubHttpClient(
            {
                "/ops/live-freshness": _StubResponse(
                    200,
                    {
                        "oldest_hot_score_age_seconds": 50,
                        "tier_1_blocked_rate_cumulative": 0.0,
                        "tier_1_active_events": 0,
                        "refresh_live_event_success_rate_5min": None,
                        "slos": [],
                    },
                ),
                "/ops/queues/summary": _StubResponse(200, {"streams": []}),
                "/ops/jobs/runs": _StubResponse(200, {"jobRuns": []}),
            }
        )
        snapshots = await fetch_all_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
        )
        names = {s.name for s in snapshots}
        # Job signals not included by default.
        self.assertNotIn("failed_jobs_15min", names)

    async def test_job_signals_included_when_opted_in(self) -> None:
        client = _StubHttpClient(
            {
                "/ops/live-freshness": _StubResponse(
                    200,
                    {
                        "oldest_hot_score_age_seconds": 50,
                        "tier_1_blocked_rate_cumulative": 0.0,
                        "tier_1_active_events": 0,
                        "refresh_live_event_success_rate_5min": None,
                        "slos": [],
                    },
                ),
                "/ops/queues/summary": _StubResponse(200, {"streams": []}),
                "/ops/jobs/runs": _StubResponse(200, {"jobRuns": []}),
            }
        )
        snapshots = await fetch_all_signals_from_api(
            base_url="http://example",
            http_client=client,
            timeout_seconds=2.0,
            include_job_signals=True,
        )
        names = {s.name for s in snapshots}
        self.assertIn("failed_jobs_15min", names)
        self.assertIn("retry_rate_15min", names)
        self.assertIn("no_recent_jobs_age_seconds", names)


if __name__ == "__main__":
    unittest.main()
