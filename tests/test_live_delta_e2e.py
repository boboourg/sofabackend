from __future__ import annotations

import unittest

from schema_inspector.queue.live_state import LiveEventStateStore
from schema_inspector.queue.streams import RedisStreamQueue

from tests.test_pilot_live_bootstrap import _FakeBootstrapCoordinator
from tests.test_pilot_live_paths import (
    _FakeLiveBackend,
    _FakeStreamBackend,
    _FakeTransport,
    _build_orchestrator,
    _tennis_responses,
)


class LiveDeltaEndToEndTests(unittest.IsolatedAsyncioTestCase):
    async def test_bootstrap_three_delta_polls_and_final_sweep(self) -> None:
        event_id = 42
        now_ms = 1_800_000_300_000
        bootstrap = _FakeBootstrapCoordinator(is_done=False, lock=True)
        modes: list[str] = []
        delta_seen_urls: list[str] = []

        first_transport = _FakeTransport(
            _tennis_responses(event_id=event_id, status_type="inprogress", start_timestamp=1_800_000_000)
        )
        await _build_orchestrator(
            transport=first_transport,
            live_state_store=LiveEventStateStore(_FakeLiveBackend()),
            stream_queue=RedisStreamQueue(_FakeStreamBackend()),
            now_ms_factory=lambda: now_ms,
            live_bootstrap_coordinator=bootstrap,
        ).run_event(event_id=event_id, sport_slug="tennis", hydration_mode="live_delta")
        modes.append("full" if _url(event_id, "lineups") in first_transport.seen_urls else "live_delta")

        for _ in range(3):
            delta_transport = _FakeTransport(
                _tennis_responses(event_id=event_id, status_type="inprogress", start_timestamp=1_800_000_000)
            )
            await _build_orchestrator(
                transport=delta_transport,
                live_state_store=LiveEventStateStore(_FakeLiveBackend()),
                stream_queue=RedisStreamQueue(_FakeStreamBackend()),
                now_ms_factory=lambda: now_ms,
                live_bootstrap_coordinator=bootstrap,
            ).run_event(event_id=event_id, sport_slug="tennis", hydration_mode="live_delta")
            delta_seen_urls.extend(delta_transport.seen_urls)
            modes.append("live_delta" if _url(event_id, "lineups") not in delta_transport.seen_urls else "full")

        final_transport = _FakeTransport(
            _tennis_responses(event_id=event_id, status_type="finished", start_timestamp=1_800_000_000)
        )
        final_report = await _build_orchestrator(
            transport=final_transport,
            live_state_store=LiveEventStateStore(_FakeLiveBackend()),
            stream_queue=RedisStreamQueue(_FakeStreamBackend()),
            now_ms_factory=lambda: now_ms,
            live_bootstrap_coordinator=bootstrap,
        ).run_event(event_id=event_id, sport_slug="tennis", hydration_mode="live_delta")
        modes.append("final_sweep" if final_report.finalized else "not_final")

        self.assertEqual(modes, ["full", "live_delta", "live_delta", "live_delta", "final_sweep"])
        self.assertEqual(bootstrap.marked, [event_id])
        self.assertEqual(bootstrap.reset, [event_id])
        for suffix in ("lineups", "incidents", "managers", "h2h", "pregame-form", "comments"):
            self.assertNotIn(_url(event_id, suffix), delta_seen_urls)


def _url(event_id: int, suffix: str) -> str:
    return f"https://www.sofascore.com/api/v1/event/{event_id}/{suffix}"


if __name__ == "__main__":
    unittest.main()
