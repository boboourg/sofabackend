from __future__ import annotations

import unittest

from schema_inspector.queue.live_state import LiveEventStateStore
from schema_inspector.queue.streams import STREAM_LIVE_TIER_3, RedisStreamQueue, StreamEntry
from schema_inspector.workers.discovery_worker import DiscoveryWorker
from schema_inspector.workers.live_worker_service import LiveWorkerService

from tests.test_pilot_live_bootstrap import _FakeBootstrapCoordinator
from tests.test_pilot_live_paths import (
    _FakeLiveBackend,
    _FakeStreamBackend,
    _FakeTransport,
    _build_orchestrator,
    _json_result,
    _tennis_responses,
)


class LiveDeltaEndToEndTests(unittest.IsolatedAsyncioTestCase):
    async def test_football_discovery_bootstrap_three_delta_polls_and_final_sweep(self) -> None:
        event_id = 14000001
        now_ms = 1_800_000_300_000
        bootstrap = _FakeBootstrapCoordinator(is_done=False, lock=True)
        live_backend = _FakeLiveBackend()
        stream_backend = _FakeStreamBackend()
        queue = RedisStreamQueue(stream_backend)
        telemetry: list[dict[str, object]] = []

        discovery_worker = DiscoveryWorker(
            orchestrator=_FakeDiscoveryOrchestrator(event_id=event_id),
            queue=queue,
            consumer="worker-discovery-smoke",
        )
        discovery_result = await discovery_worker.handle(
            StreamEntry(
                stream="stream:etl:discovery",
                message_id="1-discovery",
                values={
                    "job_id": "job-discovery",
                    "job_type": "discover_sport_surface",
                    "sport_slug": "football",
                    "scope": "live",
                    "attempt": "1",
                },
            )
        )
        self.assertEqual(discovery_result, "published:1")
        hydrate_values = stream_backend.streams[STREAM_LIVE_TIER_3][0][1]

        full_transport = _FakeTransport(_football_responses(event_id=event_id, status_type="inprogress", start_timestamp=1_800_000_000))
        full_orchestrator = _build_orchestrator(
            transport=full_transport,
            live_state_store=LiveEventStateStore(live_backend),
            stream_queue=queue,
            now_ms_factory=lambda: now_ms,
            live_bootstrap_coordinator=bootstrap,
            season_widget_gate=_BlockAllWidgetGate(),
        )
        await LiveWorkerService(
            orchestrator=full_orchestrator,
            delayed_scheduler=_FakeDelayedScheduler(),
            queue=queue,
            lane="tier_3",
            consumer="worker-live-tier-3-bootstrap-smoke",
        ).handle(StreamEntry(stream=STREAM_LIVE_TIER_3, message_id="1-hydrate", values=hydrate_values))
        telemetry.append(
            {
                "step": "bootstrap",
                "http_calls": len(full_transport.seen_urls),
                "hydration_mode": "full" if bootstrap.marked == [event_id] else "live_delta",
                "live_bootstrap_done_at": bootstrap.is_done,
            }
        )

        for index in range(3):
            delta_transport = _FakeTransport(_football_responses(event_id=event_id, status_type="inprogress", start_timestamp=1_800_000_000))
            delta_orchestrator = _build_orchestrator(
                transport=delta_transport,
                live_state_store=LiveEventStateStore(live_backend),
                stream_queue=queue,
                now_ms_factory=lambda: now_ms,
                live_bootstrap_coordinator=bootstrap,
                season_widget_gate=_BlockAllWidgetGate(),
            )
            await LiveWorkerService(
                orchestrator=delta_orchestrator,
                delayed_scheduler=_FakeDelayedScheduler(),
                queue=queue,
                lane="hot",
                consumer="worker-live-hot-smoke",
            ).handle(
                StreamEntry(
                    stream="stream:etl:live_hot",
                    message_id=f"2-delta-{index}",
                    values={
                        "job_id": f"job-delta-{index}",
                        "job_type": "refresh_live_event",
                        "sport_slug": "football",
                        "event_id": str(event_id),
                        "lane": "hot",
                        "params_json": '{"hydration_mode":"live_delta"}',
                    },
                )
            )
            telemetry.append(
                {
                    "step": f"delta_{index + 1}",
                    "http_calls": len(delta_transport.seen_urls),
                    "hydration_mode": "full" if _url(event_id, "managers") in delta_transport.seen_urls else "live_delta",
                    "live_bootstrap_done_at": bootstrap.is_done,
                }
            )

        final_gate = _FakeFinalSweepGate()
        final_transport = _FakeTransport(_football_responses(event_id=event_id, status_type="finished", start_timestamp=1_800_000_000))
        final_orchestrator = _build_orchestrator(
            transport=final_transport,
            live_state_store=LiveEventStateStore(live_backend),
            stream_queue=queue,
            now_ms_factory=lambda: now_ms,
            live_bootstrap_coordinator=bootstrap,
            final_sweep_gate=final_gate,
            season_widget_gate=_BlockAllWidgetGate(),
        )
        final_report = await final_orchestrator.run_event(event_id=event_id, sport_slug="football", hydration_mode="live_delta")
        telemetry.append(
            {
                "step": "final_sweep",
                "http_calls": len(final_transport.seen_urls),
                "hydration_mode": "final_sweep" if final_report.finalized else "live_delta",
                "live_bootstrap_done_at": bootstrap.is_done,
            }
        )

        self.assertEqual([row["hydration_mode"] for row in telemetry], ["full", "live_delta", "live_delta", "live_delta", "final_sweep"])
        self.assertEqual([row["http_calls"] for row in telemetry], [3, 1, 1, 1, 1])
        self.assertEqual([row["live_bootstrap_done_at"] for row in telemetry], [True, True, True, True, False])
        self.assertEqual(bootstrap.marked, [event_id])
        self.assertEqual(bootstrap.reset, [event_id])
        self.assertEqual(final_gate.calls, 1)

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


def _football_responses(*, event_id: int, status_type: str, start_timestamp: int):
    event_url = f"https://www.sofascore.com/api/v1/event/{event_id}"
    status_code = 100 if status_type == "finished" else 7
    home_team = {"id": 201, "slug": "home-fc", "name": "Home FC", "shortName": "HOME"}
    away_team = {"id": 202, "slug": "away-fc", "name": "Away FC", "shortName": "AWAY"}
    responses = {
        event_url: _json_result(
            event_url,
            {
                "event": {
                    "id": event_id,
                    "slug": "home-away",
                    "tournament": {
                        "id": 301,
                        "slug": "test-league",
                        "name": "Test League",
                        "uniqueTournament": {"id": 401, "slug": "test-league", "name": "Test League"},
                    },
                    "season": {"id": 501, "name": "Test League 2026", "year": "2026"},
                    "status": {"code": status_code, "description": status_type.title(), "type": status_type},
                    "startTimestamp": start_timestamp,
                    "homeTeam": home_team,
                    "awayTeam": away_team,
                    "hasEventPlayerHeatMap": False,
                    "hasXg": False,
                }
            },
        ),
        _url(event_id, "statistics"): _json_result(_url(event_id, "statistics"), {"statistics": []}),
        _url(event_id, "lineups"): _json_result(_url(event_id, "lineups"), {"home": {"players": []}, "away": {"players": []}}),
        _url(event_id, "incidents"): _json_result(_url(event_id, "incidents"), {"incidents": []}),
        _url(event_id, "managers"): _json_result(_url(event_id, "managers"), {"homeManager": {}, "awayManager": {}}),
        _url(event_id, "h2h"): _json_result(_url(event_id, "h2h"), {"teamDuel": {}, "managerDuel": {}}),
        _url(event_id, "pregame-form"): _json_result(_url(event_id, "pregame-form"), {"homeTeam": {}, "awayTeam": {}}),
        _url(event_id, "votes"): _json_result(_url(event_id, "votes"), {"vote": {}}),
        _url(event_id, "comments"): _json_result(_url(event_id, "comments"), {"comments": []}),
        _url(event_id, "graph"): _json_result(_url(event_id, "graph"), {"graphPoints": []}),
        f"https://www.sofascore.com/api/v1/event/{event_id}/odds/1/all": _json_result(
            f"https://www.sofascore.com/api/v1/event/{event_id}/odds/1/all",
            {"markets": []},
        ),
        f"https://www.sofascore.com/api/v1/event/{event_id}/odds/1/featured": _json_result(
            f"https://www.sofascore.com/api/v1/event/{event_id}/odds/1/featured",
            {"featured": []},
        ),
        f"https://www.sofascore.com/api/v1/event/{event_id}/provider/1/winning-odds": _json_result(
            f"https://www.sofascore.com/api/v1/event/{event_id}/provider/1/winning-odds",
            {"home": {}, "away": {}},
        ),
        "https://www.sofascore.com/api/v1/team/201": _json_result("https://www.sofascore.com/api/v1/team/201", {"team": home_team}),
        "https://www.sofascore.com/api/v1/team/202": _json_result("https://www.sofascore.com/api/v1/team/202", {"team": away_team}),
    }
    return responses


class _FakeDiscoveryOrchestrator:
    def __init__(self, *, event_id: int) -> None:
        self.event_id = int(event_id)

    async def discover_live_events(self, *, sport_slug: str, timeout: float):
        del sport_slug, timeout
        return [self.event_id]


class _FakeDelayedScheduler:
    def schedule(self, job_id: str, *, run_at_epoch_ms: int) -> None:
        del job_id, run_at_epoch_ms


class _BlockAllWidgetGate:
    async def blocked_endpoint_patterns(self, **kwargs):
        return tuple(kwargs["endpoint_patterns"])


class _FakeFinalSweepGate:
    def __init__(self) -> None:
        self.calls = 0

    async def run(self, func):
        self.calls += 1
        return await func()


if __name__ == "__main__":
    unittest.main()
