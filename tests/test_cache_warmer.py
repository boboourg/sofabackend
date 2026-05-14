"""Tests for cache_warmer module (N4 Layer C)."""

from __future__ import annotations

import asyncio
import unittest

from schema_inspector.cache_warmer import (
    CacheWarmerConfig,
    CacheWarmerDaemon,
    UrlTarget,
    build_url_targets,
)


class _StubResponse:
    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code


class _StubHttpClient:
    def __init__(self, *, status_code: int = 200, raise_exc: Exception | None = None) -> None:
        self.status_code = status_code
        self.raise_exc = raise_exc
        self.calls: list[str] = []

    async def get(self, url: str, *, timeout: float):
        del timeout
        self.calls.append(url)
        if self.raise_exc:
            raise self.raise_exc
        return _StubResponse(status_code=self.status_code)


class BuildUrlTargetsTests(unittest.TestCase):
    def test_three_targets_per_sport_by_default(self) -> None:
        targets = build_url_targets(
            sports=("football", "tennis"),
            today="2026-05-14",
        )
        self.assertEqual(len(targets), 6)  # 3 per sport
        urls = [t.url for t in targets]
        self.assertIn("/api/v1/sport/football/events/live", urls)
        self.assertIn("/api/v1/sport/football/scheduled-events/2026-05-14", urls)
        self.assertIn(
            "/api/v1/sport/football/scheduled-tournaments/2026-05-14/page/0", urls
        )

    def test_warm_tomorrow_adds_fourth_target(self) -> None:
        targets = build_url_targets(
            sports=("football",),
            today="2026-05-14",
            tomorrow="2026-05-15",
            warm_tomorrow_scheduled=True,
        )
        self.assertEqual(len(targets), 4)
        urls = [t.url for t in targets]
        self.assertIn("/api/v1/sport/football/scheduled-events/2026-05-15", urls)

    def test_empty_sport_filtered_out(self) -> None:
        targets = build_url_targets(
            sports=("football", "", "tennis"),
            today="2026-05-14",
        )
        self.assertEqual(len(targets), 6)  # 2 sports × 3, "" skipped

    def test_intervals_propagate(self) -> None:
        targets = build_url_targets(
            sports=("football",),
            today="2026-05-14",
            live_interval_seconds=10,
            scheduled_today_interval_seconds=120,
        )
        live = next(t for t in targets if "live" in t.url)
        sched = next(t for t in targets if "scheduled-events" in t.url)
        self.assertEqual(live.interval_seconds, 10)
        self.assertEqual(sched.interval_seconds, 120)


class CacheWarmerDaemonTests(unittest.IsolatedAsyncioTestCase):
    def _make_daemon(
        self,
        *,
        targets: list[UrlTarget] | None = None,
        http_client: _StubHttpClient | None = None,
        clock_value: list[float] | None = None,
    ) -> tuple[CacheWarmerDaemon, _StubHttpClient, list[float]]:
        clock_value = clock_value or [0.0]
        config = CacheWarmerConfig(
            enabled=True,
            base_url="http://example",
            tick_interval_seconds=1.0,
            request_timeout_seconds=10.0,
            sports=("football",),
            live_interval_seconds=5,
            scheduled_today_interval_seconds=60,
        )
        http_client = http_client or _StubHttpClient()
        targets = targets or [
            UrlTarget(url="/api/v1/sport/football/events/live", interval_seconds=5),
        ]
        daemon = CacheWarmerDaemon(
            config=config,
            http_client=http_client,
            targets=targets,
            clock=lambda: clock_value[0],
            sleep=asyncio.sleep,
        )
        return daemon, http_client, clock_value

    async def test_first_tick_fetches_all_targets(self) -> None:
        daemon, http_client, _ = self._make_daemon()
        report = await daemon._tick()
        self.assertEqual(report.targets_due, 1)
        self.assertEqual(report.targets_fetched, 1)
        self.assertEqual(report.targets_failed, 0)
        self.assertEqual(
            http_client.calls,
            ["http://example/api/v1/sport/football/events/live"],
        )

    async def test_second_tick_within_interval_does_nothing(self) -> None:
        clock = [0.0]
        daemon, http_client, _ = self._make_daemon(clock_value=clock)
        await daemon._tick()  # fetch at t=0
        clock[0] = 2.0  # 2s later, less than 5s interval
        report = await daemon._tick()
        self.assertEqual(report.targets_due, 0)
        self.assertEqual(len(http_client.calls), 1)  # unchanged

    async def test_second_tick_after_interval_fetches_again(self) -> None:
        clock = [0.0]
        daemon, http_client, _ = self._make_daemon(clock_value=clock)
        await daemon._tick()
        clock[0] = 6.0  # 6s later, past 5s interval
        await daemon._tick()
        self.assertEqual(len(http_client.calls), 2)

    async def test_500_status_counted_as_failure(self) -> None:
        http_client = _StubHttpClient(status_code=503)
        daemon, _, _ = self._make_daemon(http_client=http_client)
        report = await daemon._tick()
        self.assertEqual(report.targets_failed, 1)
        self.assertEqual(report.targets_fetched, 0)
        self.assertEqual(daemon.total_failures, 1)

    async def test_exception_counted_as_failure(self) -> None:
        http_client = _StubHttpClient(raise_exc=ConnectionError("boom"))
        daemon, _, _ = self._make_daemon(http_client=http_client)
        report = await daemon._tick()
        self.assertEqual(report.targets_failed, 1)

    async def test_disabled_config_exits_run_forever(self) -> None:
        config = CacheWarmerConfig(enabled=False)
        http_client = _StubHttpClient()
        daemon = CacheWarmerDaemon(
            config=config, http_client=http_client, targets=[]
        )
        # Must return quickly, not block.
        await asyncio.wait_for(daemon.run_forever(), timeout=2.0)

    async def test_multiple_targets_with_different_intervals(self) -> None:
        clock = [0.0]
        targets = [
            UrlTarget(url="/live", interval_seconds=5),
            UrlTarget(url="/scheduled", interval_seconds=60),
        ]
        daemon, http_client, _ = self._make_daemon(targets=targets, clock_value=clock)
        await daemon._tick()  # both fetched at t=0
        self.assertEqual(len(http_client.calls), 2)
        clock[0] = 10.0  # 10s — live due, scheduled not
        await daemon._tick()
        self.assertEqual(len(http_client.calls), 3)  # only /live re-fetched

    async def test_state_tracks_status_and_duration(self) -> None:
        daemon, _, _ = self._make_daemon()
        await daemon._tick()
        url = "/api/v1/sport/football/events/live"
        state = daemon._states[url]
        self.assertEqual(state.last_status_code, 200)
        self.assertEqual(state.total_fetches, 1)
        self.assertEqual(state.total_failures, 0)


class CacheWarmerConfigTests(unittest.TestCase):
    def test_defaults(self) -> None:
        config = CacheWarmerConfig.from_env(env={})
        self.assertTrue(config.enabled)
        self.assertEqual(config.tick_interval_seconds, 1.0)
        self.assertEqual(config.live_interval_seconds, 5)
        self.assertIn("football", config.sports)

    def test_disabled_via_env(self) -> None:
        config = CacheWarmerConfig.from_env(env={"SOFASCORE_CACHE_WARMER_ENABLED": "0"})
        self.assertFalse(config.enabled)

    def test_sports_env_override(self) -> None:
        config = CacheWarmerConfig.from_env(
            env={"SOFASCORE_CACHE_WARMER_SPORTS": "football,basketball,tennis"}
        )
        self.assertEqual(config.sports, ("football", "basketball", "tennis"))

    def test_intervals_env_override(self) -> None:
        config = CacheWarmerConfig.from_env(
            env={
                "SOFASCORE_CACHE_WARMER_LIVE_INTERVAL_SECONDS": "3",
                "SOFASCORE_CACHE_WARMER_SCHEDULED_TODAY_INTERVAL_SECONDS": "120",
            }
        )
        self.assertEqual(config.live_interval_seconds, 3)
        self.assertEqual(config.scheduled_today_interval_seconds, 120)


if __name__ == "__main__":
    unittest.main()
