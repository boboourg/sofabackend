from __future__ import annotations

import unittest

from schema_inspector.queue.live_state import LiveEventStateStore
from schema_inspector.queue.streams import RedisStreamQueue

from tests.test_pilot_live_paths import (
    _FakeLiveBackend,
    _FakeStreamBackend,
    _FakeTransport,
    _build_orchestrator,
    _tennis_responses,
)


class PilotLiveBootstrapTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_delta_runs_full_once_when_bootstrap_missing(self) -> None:
        bootstrap = _FakeBootstrapCoordinator(is_done=False, lock=True)
        transport = _FakeTransport(_tennis_responses(event_id=42, status_type="inprogress", start_timestamp=1_800_000_000))
        orchestrator = _build_orchestrator(
            transport=transport,
            live_state_store=LiveEventStateStore(_FakeLiveBackend()),
            stream_queue=RedisStreamQueue(_FakeStreamBackend()),
            now_ms_factory=lambda: 1_800_000_300_000,
            live_bootstrap_coordinator=bootstrap,
        )

        await orchestrator.run_event(event_id=42, sport_slug="tennis", hydration_mode="live_delta")

        self.assertEqual(bootstrap.marked, [42])
        self.assertIn("https://www.sofascore.com/api/v1/event/42/team-streaks", transport.seen_urls)
        self.assertNotIn("https://www.sofascore.com/api/v1/event/42/lineups", transport.seen_urls)

    async def test_live_delta_skips_when_single_flight_lock_is_held(self) -> None:
        bootstrap = _FakeBootstrapCoordinator(is_done=False, lock=False)
        transport = _FakeTransport(_tennis_responses(event_id=42, status_type="inprogress", start_timestamp=1_800_000_000))
        orchestrator = _build_orchestrator(
            transport=transport,
            live_state_store=LiveEventStateStore(_FakeLiveBackend()),
            stream_queue=RedisStreamQueue(_FakeStreamBackend()),
            now_ms_factory=lambda: 1_800_000_300_000,
            live_bootstrap_coordinator=bootstrap,
        )

        report = await orchestrator.run_event(event_id=42, sport_slug="tennis", hydration_mode="live_delta")

        self.assertEqual(report.fetch_outcomes, ())
        self.assertFalse(report.finalized)
        self.assertEqual(transport.seen_urls, [])

    async def test_finished_live_delta_resets_bootstrap_after_final_sweep(self) -> None:
        bootstrap = _FakeBootstrapCoordinator(is_done=True, lock=True)
        transport = _FakeTransport(_tennis_responses(event_id=42, status_type="finished", start_timestamp=1_800_000_000))
        orchestrator = _build_orchestrator(
            transport=transport,
            live_state_store=LiveEventStateStore(_FakeLiveBackend()),
            stream_queue=RedisStreamQueue(_FakeStreamBackend()),
            now_ms_factory=lambda: 1_800_000_300_000,
            live_bootstrap_coordinator=bootstrap,
        )

        report = await orchestrator.run_event(event_id=42, sport_slug="tennis", hydration_mode="live_delta")

        self.assertTrue(report.finalized)
        self.assertEqual(bootstrap.reset, [42])


class _FakeBootstrapCoordinator:
    def __init__(self, *, is_done: bool, lock: bool) -> None:
        self.is_done = bool(is_done)
        self.lock = bool(lock)
        self.marked: list[int] = []
        self.reset: list[int] = []

    async def is_bootstrapped(self, executor, *, event_id: int) -> bool:
        del executor, event_id
        return self.is_done

    def acquire_hydrate_lock(self, *, event_id: int) -> bool:
        del event_id
        return self.lock

    async def mark_bootstrapped(self, executor, *, event_id: int) -> None:
        del executor
        self.marked.append(int(event_id))
        self.is_done = True

    async def reset_bootstrap(self, executor, *, event_id: int) -> None:
        del executor
        self.reset.append(int(event_id))
        self.is_done = False


if __name__ == "__main__":
    unittest.main()
