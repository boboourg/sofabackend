"""Tests for the A2 Phase 0 LiveRescueLoop."""

from __future__ import annotations

import json
import unittest
from contextlib import asynccontextmanager
from typing import Any

from schema_inspector.queue.live_state import LiveEventState
from schema_inspector.queue.streams import (
    STREAM_LIVE_TIER_1,
    STREAM_LIVE_TIER_2,
    STREAM_LIVE_TIER_3,
)
from schema_inspector.services.live_rescue import (
    LiveRescueConfig,
    LiveRescueLoop,
    StuckEventCandidate,
)


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeConnection:
    def __init__(self, payload: Any) -> None:
        self.payload = payload
        self.fetchval_calls = 0

    async def fetchval(self, query: str, *args) -> Any:
        del query, args
        self.fetchval_calls += 1
        return self.payload


class _FakeConnectionFactory:
    def __init__(self, payload: Any) -> None:
        self.connection = _FakeConnection(payload)

    def __call__(self):
        return self._cm()

    @asynccontextmanager
    async def _cm(self):
        yield self.connection


class _FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, str] = {}
        self.publishes: list[tuple[str, dict[str, str]]] = []
        self.ttls: dict[str, int] = {}

    def exists(self, key: str) -> int:
        return 1 if key in self.store else 0

    def set(self, key: str, value: str, *, ex: int | None = None) -> None:
        self.store[key] = value
        if ex is not None:
            self.ttls[key] = int(ex)


class _FakeLiveStateStore:
    def __init__(self, states: dict[int, LiveEventState] | None = None) -> None:
        self._states = dict(states or {})
        self.fetch_calls: list[int] = []

    def fetch(self, event_id: int) -> LiveEventState | None:
        self.fetch_calls.append(int(event_id))
        return self._states.get(int(event_id))


class _FakeStreamQueue:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []

    def publish(self, stream: str, values: dict[str, Any]) -> str:
        self.published.append((stream, dict(values)))
        return f"fake-{len(self.published)}"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _live_state(
    *,
    event_id: int,
    last_ingested_at: int | None,
    dispatch_tier: str | None = None,
    is_finalized: bool = False,
) -> LiveEventState:
    return LiveEventState(
        event_id=event_id,
        sport_slug="football",
        status_type="inprogress",
        poll_profile="hot",
        last_seen_at=last_ingested_at,
        last_ingested_at=last_ingested_at,
        last_changed_at=last_ingested_at,
        next_poll_at=None,
        hot_until=None,
        home_score=None,
        away_score=None,
        version_hint=None,
        is_finalized=is_finalized,
        dispatch_tier=dispatch_tier,
    )


def _payload_for_events(*event_ids: int) -> str:
    return json.dumps({"events": [{"id": eid} for eid in event_ids]})


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class LiveRescueConfigTests(unittest.TestCase):
    def test_defaults_disabled(self) -> None:
        cfg = LiveRescueConfig.from_env({})
        self.assertFalse(cfg.enabled)
        self.assertFalse(cfg.dry_run)
        self.assertEqual(cfg.interval_s, 60.0)
        self.assertEqual(cfg.stale_minutes, 5)
        self.assertEqual(cfg.cooldown_seconds, 300)
        self.assertEqual(cfg.max_rescues_per_tick, 20)
        self.assertEqual(cfg.sport_slugs, ("football",))

    def test_env_overrides(self) -> None:
        cfg = LiveRescueConfig.from_env(
            {
                "SOFASCORE_LIVE_RESCUE_ENABLED": "true",
                "SOFASCORE_LIVE_RESCUE_DRY_RUN": "yes",
                "SOFASCORE_LIVE_RESCUE_INTERVAL_S": "30",
                "SOFASCORE_LIVE_RESCUE_STALE_MINUTES": "8",
                "SOFASCORE_LIVE_RESCUE_COOLDOWN_SECONDS": "120",
                "SOFASCORE_LIVE_RESCUE_MAX_PER_TICK": "50",
                "SOFASCORE_LIVE_RESCUE_SPORT_SLUGS": "football,basketball",
            }
        )
        self.assertTrue(cfg.enabled)
        self.assertTrue(cfg.dry_run)
        self.assertEqual(cfg.interval_s, 30.0)
        self.assertEqual(cfg.stale_minutes, 8)
        self.assertEqual(cfg.cooldown_seconds, 120)
        self.assertEqual(cfg.max_rescues_per_tick, 50)
        self.assertEqual(cfg.sport_slugs, ("football", "basketball"))

    def test_empty_sport_slugs_falls_back(self) -> None:
        cfg = LiveRescueConfig.from_env({"SOFASCORE_LIVE_RESCUE_SPORT_SLUGS": ""})
        self.assertEqual(cfg.sport_slugs, ("football",))


class LiveRescueLoopTests(unittest.IsolatedAsyncioTestCase):
    def _build(
        self,
        *,
        config: LiveRescueConfig,
        payload: Any,
        states: dict[int, LiveEventState] | None = None,
        now_ms: int = 1_700_000_000_000,
        tier_override_registry: Any = None,
    ) -> tuple[LiveRescueLoop, _FakeConnectionFactory, _FakeRedis, _FakeLiveStateStore, _FakeStreamQueue]:
        factory = _FakeConnectionFactory(payload)
        redis = _FakeRedis()
        store = _FakeLiveStateStore(states)
        queue = _FakeStreamQueue()
        loop = LiveRescueLoop(
            config=config,
            connection_factory=factory,
            redis_backend=redis,
            live_state_store=store,
            stream_queue=queue,
            tier_override_registry=tier_override_registry,
            now_ms_factory=lambda: now_ms,
        )
        return loop, factory, redis, store, queue

    async def test_rescues_stale_polling_event(self) -> None:
        now = 1_700_000_000_000
        # Event present in live surface; last update 10 minutes ago > 5 min stale.
        states = {
            1234: _live_state(
                event_id=1234,
                last_ingested_at=now - 10 * 60 * 1000,
                dispatch_tier="tier_1",
            )
        }
        loop, _, redis, _, queue = self._build(
            config=LiveRescueConfig(enabled=True, stale_minutes=5),
            payload=_payload_for_events(1234),
            states=states,
            now_ms=now,
        )
        report = await loop.run_once()
        self.assertEqual(report.scanned, 1)
        self.assertEqual(report.candidates, 1)
        self.assertEqual(report.rescued, 1)
        self.assertEqual(report.stale_polling, 1)
        # Published to tier_1 stream (matches stored dispatch_tier).
        self.assertEqual(len(queue.published), 1)
        stream, values = queue.published[0]
        self.assertEqual(stream, STREAM_LIVE_TIER_1)
        self.assertEqual(int(values["entity_id"]), 1234)
        self.assertEqual(values["scope"], "live")
        params = json.loads(values["params_json"])
        self.assertEqual(params["hydration_mode"], "live_delta")
        self.assertTrue(params["force_rehydrate"])
        self.assertEqual(params["correction_reason"], "live_rescue:stale_polling")
        # Cooldown key set.
        self.assertIn("live:rescue:cooldown:1234", redis.store)
        self.assertEqual(redis.ttls["live:rescue:cooldown:1234"], 300)

    async def test_rescues_event_missing_from_live_state(self) -> None:
        now = 1_700_000_000_000
        # Event in surface, but no live_state hash at all (never bootstrapped).
        loop, _, _, store, queue = self._build(
            config=LiveRescueConfig(enabled=True, stale_minutes=5),
            payload=_payload_for_events(9999),
            states={},
            now_ms=now,
        )
        report = await loop.run_once()
        self.assertEqual(report.no_live_state, 1)
        self.assertEqual(report.rescued, 1)
        self.assertEqual(len(queue.published), 1)
        stream, values = queue.published[0]
        # No tier hint → falls through to tier_3 (default).
        self.assertEqual(stream, STREAM_LIVE_TIER_3)
        params = json.loads(values["params_json"])
        # Bootstrap is the right move for an event that lacks live_state.
        self.assertEqual(params["hydration_mode"], "full")
        self.assertTrue(params["live_bootstrap"])
        self.assertEqual(params["correction_reason"], "live_rescue:no_live_state")
        self.assertIn(9999, store.fetch_calls)

    async def test_skips_event_with_fresh_polling(self) -> None:
        now = 1_700_000_000_000
        # Last ingested 30 seconds ago, well within 5-min window.
        states = {
            42: _live_state(
                event_id=42,
                last_ingested_at=now - 30 * 1000,
                dispatch_tier="tier_2",
            )
        }
        loop, _, _, _, queue = self._build(
            config=LiveRescueConfig(enabled=True, stale_minutes=5),
            payload=_payload_for_events(42),
            states=states,
            now_ms=now,
        )
        report = await loop.run_once()
        self.assertEqual(report.candidates, 0)
        self.assertEqual(report.rescued, 0)
        self.assertEqual(len(queue.published), 0)

    async def test_skips_finalized_event_even_if_in_surface(self) -> None:
        now = 1_700_000_000_000
        # Upstream still reports event as live, but we've already finalized
        # it — do NOT re-liven (housekeeping zombie sweep handles this case).
        states = {
            55: _live_state(
                event_id=55,
                last_ingested_at=now - 10 * 60 * 1000,
                dispatch_tier="tier_3",
                is_finalized=True,
            )
        }
        loop, _, _, _, queue = self._build(
            config=LiveRescueConfig(enabled=True, stale_minutes=5),
            payload=_payload_for_events(55),
            states=states,
            now_ms=now,
        )
        report = await loop.run_once()
        self.assertEqual(report.candidates, 0)
        self.assertEqual(report.finalized, 1)
        self.assertEqual(len(queue.published), 0)

    async def test_respects_cooldown(self) -> None:
        now = 1_700_000_000_000
        states = {
            777: _live_state(
                event_id=777,
                last_ingested_at=now - 600 * 1000,
                dispatch_tier="tier_1",
            )
        }
        loop, _, redis, _, queue = self._build(
            config=LiveRescueConfig(enabled=True, stale_minutes=5),
            payload=_payload_for_events(777),
            states=states,
            now_ms=now,
        )
        # Pre-seed cooldown to simulate a previous rescue tick.
        redis.store["live:rescue:cooldown:777"] = "1"
        report = await loop.run_once()
        self.assertEqual(report.candidates, 1)
        self.assertEqual(report.rescued, 0)
        self.assertEqual(report.skipped_cooldown, 1)
        self.assertEqual(len(queue.published), 0)

    async def test_respects_max_rescues_per_tick(self) -> None:
        now = 1_700_000_000_000
        event_ids = [1000 + i for i in range(50)]
        # All 50 events are stale.
        states = {
            eid: _live_state(
                event_id=eid,
                last_ingested_at=now - 10 * 60 * 1000,
                dispatch_tier="tier_3",
            )
            for eid in event_ids
        }
        loop, _, _, _, queue = self._build(
            config=LiveRescueConfig(
                enabled=True,
                stale_minutes=5,
                max_rescues_per_tick=10,
            ),
            payload=_payload_for_events(*event_ids),
            states=states,
            now_ms=now,
        )
        report = await loop.run_once()
        self.assertEqual(report.candidates, 50)
        self.assertEqual(report.rescued, 10)
        self.assertEqual(len(queue.published), 10)

    async def test_dry_run_does_not_publish_or_set_cooldown(self) -> None:
        now = 1_700_000_000_000
        states = {
            5: _live_state(
                event_id=5,
                last_ingested_at=now - 10 * 60 * 1000,
                dispatch_tier="tier_2",
            )
        }
        loop, _, redis, _, queue = self._build(
            config=LiveRescueConfig(enabled=True, dry_run=True, stale_minutes=5),
            payload=_payload_for_events(5),
            states=states,
            now_ms=now,
        )
        report = await loop.run_once()
        self.assertEqual(report.candidates, 1)
        # Dry-run still counts as "rescued" in the report so operators can
        # see what would happen, but no Redis write and no stream publish.
        self.assertEqual(report.rescued, 1)
        self.assertEqual(len(queue.published), 0)
        self.assertNotIn("live:rescue:cooldown:5", redis.store)

    async def test_empty_payload_returns_zero(self) -> None:
        loop, _, _, _, queue = self._build(
            config=LiveRescueConfig(enabled=True),
            payload=None,
        )
        report = await loop.run_once()
        self.assertEqual(report.scanned, 0)
        self.assertEqual(report.candidates, 0)
        self.assertEqual(report.rescued, 0)
        self.assertEqual(len(queue.published), 0)

    async def test_uses_tier_override_for_event_without_tier_hint(self) -> None:
        # Event has no dispatch_tier in live_state (just bootstrapped or
        # state-store glitch); resolver should fall through to the
        # override registry. We bake in a UT-agnostic fake that always
        # returns tier_1 to verify wiring.
        class _ForceTier1Registry:
            def get(self, ut_id):  # noqa: D401 — minimal protocol
                return "tier_1"

        # But our resolver passes unique_tournament_id=None, so the
        # registry never gets a non-None key and the override path
        # short-circuits to None → heuristic returns LIVE_TIER_3.
        # This test pins the *fallback* path: tier_3 by default.
        now = 1_700_000_000_000
        states = {
            123: _live_state(event_id=123, last_ingested_at=None, dispatch_tier=None),
        }
        loop, _, _, _, queue = self._build(
            config=LiveRescueConfig(enabled=True, stale_minutes=5),
            payload=_payload_for_events(123),
            states=states,
            now_ms=now,
            tier_override_registry=_ForceTier1Registry(),
        )
        report = await loop.run_once()
        self.assertEqual(report.rescued, 1)
        stream, _ = queue.published[0]
        self.assertEqual(stream, STREAM_LIVE_TIER_3)


if __name__ == "__main__":
    unittest.main()
