"""Tests for monitoring disk-free signal (Phase 0, 2026-05-26 incident).

Pins the contract:
- free space above both thresholds => OK
- between WARN and CRIT => WARN (direction 'min')
- below CRIT => CRIT
- custom thresholds are honoured
- disk_usage raising => empty list (daemon must not crash; no fabricated OK)
- value is a GB-scaled float; extra carries mount_path + free_bytes
- injected ``now`` propagates to the snapshot timestamp
- fetch_all_signals_from_api includes the disk snapshot only when a mount
  path is provided

Uses a fake disk_usage callable so the test never touches the real FS.
"""

from __future__ import annotations

import unittest
from collections import namedtuple
from datetime import datetime, timezone

from schema_inspector.monitoring.signal_source import (
    fetch_all_signals_from_api,
    fetch_disk_signals,
)
from schema_inspector.monitoring.signals import SIGNAL_POSTGRES_DISK_FREE

_GiB = 1024 ** 3
_Usage = namedtuple("_Usage", ["total", "used", "free"])


def _fake_disk_usage(free_gb: float):
    free_bytes = int(free_gb * _GiB)
    total_bytes = 500 * _GiB

    def _probe(path: str) -> _Usage:
        del path
        return _Usage(total=total_bytes, used=total_bytes - free_bytes, free=free_bytes)

    return _probe


def _raising_disk_usage(path: str):
    del path
    raise OSError("mount not found")


class FetchDiskSignalsTests(unittest.IsolatedAsyncioTestCase):
    async def test_ok_above_both_thresholds(self) -> None:
        snaps = await fetch_disk_signals(
            mount_path="/var/lib/postgresql",
            warn_free_gb=20.0,
            crit_free_gb=10.0,
            disk_usage=_fake_disk_usage(30.0),
        )
        self.assertEqual(len(snaps), 1)
        snap = snaps[0]
        self.assertEqual(snap.name, SIGNAL_POSTGRES_DISK_FREE.name)
        self.assertEqual(snap.severity, "OK")
        self.assertEqual(snap.value, 30.0)
        self.assertEqual(snap.extra["mount_path"], "/var/lib/postgresql")
        self.assertEqual(snap.extra["free_bytes"], 30 * _GiB)

    async def test_warn_between_thresholds(self) -> None:
        snaps = await fetch_disk_signals(
            mount_path="/data",
            warn_free_gb=20.0,
            crit_free_gb=10.0,
            disk_usage=_fake_disk_usage(15.0),
        )
        self.assertEqual(snaps[0].severity, "WARN")
        self.assertEqual(snaps[0].threshold_warn, 20.0)

    async def test_crit_below_crit_threshold(self) -> None:
        snaps = await fetch_disk_signals(
            mount_path="/data",
            warn_free_gb=20.0,
            crit_free_gb=10.0,
            disk_usage=_fake_disk_usage(5.0),
        )
        self.assertEqual(snaps[0].severity, "CRIT")
        self.assertEqual(snaps[0].threshold_crit, 10.0)

    async def test_custom_thresholds_applied(self) -> None:
        snaps = await fetch_disk_signals(
            mount_path="/data",
            warn_free_gb=15.0,
            crit_free_gb=8.0,
            disk_usage=_fake_disk_usage(12.0),
        )
        # 12 < 15 (warn) but >= 8 (crit) => WARN
        self.assertEqual(snaps[0].severity, "WARN")
        self.assertEqual(snaps[0].threshold_warn, 15.0)
        self.assertEqual(snaps[0].threshold_crit, 8.0)

    async def test_disk_usage_error_returns_empty(self) -> None:
        snaps = await fetch_disk_signals(
            mount_path="/missing",
            warn_free_gb=20.0,
            crit_free_gb=10.0,
            disk_usage=_raising_disk_usage,
        )
        self.assertEqual(snaps, [])

    async def test_injected_now_propagates(self) -> None:
        now = datetime(2026, 5, 26, 3, 0, 0, tzinfo=timezone.utc)
        snaps = await fetch_disk_signals(
            mount_path="/data",
            warn_free_gb=20.0,
            crit_free_gb=10.0,
            now=now,
            disk_usage=_fake_disk_usage(50.0),
        )
        self.assertEqual(snaps[0].timestamp, now)


class FetchAllDiskInclusionTests(unittest.IsolatedAsyncioTestCase):
    class _StubResponse:
        def __init__(self, status_code: int, payload: dict) -> None:
            self.status_code = status_code
            self._payload = payload

        def json(self) -> dict:
            return self._payload

    class _StubHttpClient:
        def __init__(self, routes: dict) -> None:
            self.routes = routes

        async def get(self, url: str, *, timeout: float):
            del timeout
            for suffix, resp in self.routes.items():
                if url.endswith(suffix):
                    return resp
            return FetchAllDiskInclusionTests._StubResponse(404, {})

    def _client(self):
        return self._StubHttpClient(
            {
                "/ops/live-freshness": self._StubResponse(
                    200,
                    {
                        "oldest_hot_score_age_seconds": 10,
                        "tier_1_blocked_rate_cumulative": 0.0,
                        "tier_1_active_events": 0,
                        "refresh_live_event_success_rate_5min": None,
                        "slos": [],
                    },
                ),
                "/ops/queues/summary": self._StubResponse(200, {"streams": []}),
            }
        )

    async def test_disk_omitted_when_no_mount_path(self) -> None:
        snaps = await fetch_all_signals_from_api(
            base_url="http://example",
            http_client=self._client(),
            timeout_seconds=2.0,
        )
        names = {s.name for s in snaps}
        self.assertNotIn(SIGNAL_POSTGRES_DISK_FREE.name, names)

    async def test_disk_included_when_mount_path_set(self) -> None:
        import schema_inspector.monitoring.signal_source as ss

        original = ss.shutil.disk_usage
        ss.shutil.disk_usage = _fake_disk_usage(7.0)  # CRIT range
        try:
            snaps = await fetch_all_signals_from_api(
                base_url="http://example",
                http_client=self._client(),
                timeout_seconds=2.0,
                disk_mount_path="/var/lib/postgresql",
                disk_warn_free_gb=20.0,
                disk_crit_free_gb=10.0,
            )
        finally:
            ss.shutil.disk_usage = original
        disk = next(s for s in snaps if s.name == SIGNAL_POSTGRES_DISK_FREE.name)
        self.assertEqual(disk.severity, "CRIT")
        self.assertEqual(disk.value, 7.0)


if __name__ == "__main__":
    unittest.main()
