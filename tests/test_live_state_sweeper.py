"""Tests for the stale live-state sweeper (P0.B 2026-05-14).

Pins the contract:
- finalised events older than grace window are removed from
  zset:live:hot/warm/cold
- events still within grace window are kept (false-positive protection)
- events without terminal_status (still inprogress) are kept
- a sweep is idempotent — second run is a no-op
- failures in DB / Redis are swallowed; sweep returns an error report
- PlannerDaemon._maybe_sweep_live_state schedules the callback on cadence
"""

from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timedelta, timezone

from schema_inspector.services.live_state_sweeper import (
    LiveStateSweepReport,
    LiveStateSweeper,
)


class _StubBackend:
    """Hot/warm/cold zsets backed by simple dicts (member → score)."""

    def __init__(self) -> None:
        self.zsets: dict[str, dict[str, float]] = {
            "zset:live:hot": {},
            "zset:live:warm": {},
            "zset:live:cold": {},
        }
        self.zrange_calls: list[tuple[str, int, int]] = []
        self.zrem_calls: list[tuple[str, str]] = []
        self.raise_on_zrange = False
        self.raise_on_zrem = False

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        zset = self.zsets.setdefault(key, {})
        added = 0
        for member, score in mapping.items():
            if member not in zset:
                added += 1
            zset[member] = float(score)
        return added

    def zrange(self, key: str, start: int, end: int, withscores: bool = False):
        self.zrange_calls.append((key, start, end))
        if self.raise_on_zrange:
            raise RuntimeError("forced zrange error")
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: item[1])
        if end == -1:
            sliced = items[start:]
        else:
            sliced = items[start : end + 1]
        if withscores:
            return [(m, s) for m, s in sliced]
        return [m for m, _ in sliced]

    def zrem(self, key: str, member: str) -> int:
        self.zrem_calls.append((key, member))
        if self.raise_on_zrem:
            raise RuntimeError("forced zrem error")
        zset = self.zsets.get(key, {})
        if member in zset:
            del zset[member]
            return 1
        return 0


class _StubLiveStateStore:
    def __init__(self) -> None:
        self.backend = _StubBackend()
        self.hot_zset_key = "zset:live:hot"
        self.warm_zset_key = "zset:live:warm"
        self.cold_zset_key = "zset:live:cold"


class _StubExecutor:
    """asyncpg-shaped fetch executor returning controllable rows."""

    def __init__(self, *, finalized_event_ids: list[int] | None = None, raise_on_fetch: bool = False) -> None:
        self.finalized_event_ids = list(finalized_event_ids or [])
        self.raise_on_fetch = raise_on_fetch
        self.fetch_args: list[tuple[object, ...]] = []

    async def fetch(self, query: str, *args):
        self.fetch_args.append(args)
        if self.raise_on_fetch:
            raise RuntimeError("forced db error")
        del query
        return [{"event_id": eid} for eid in self.finalized_event_ids]


class LiveStateSweeperTests(unittest.IsolatedAsyncioTestCase):
    async def test_removes_finalised_events_past_grace_window(self) -> None:
        store = _StubLiveStateStore()
        now_ms = 1_700_000_000_000
        store.backend.zadd("zset:live:hot", {"1001": now_ms, "1002": now_ms, "1003": now_ms})
        store.backend.zadd("zset:live:warm", {"1002": now_ms - 60_000})
        store.backend.zadd("zset:live:cold", {"9999": now_ms})

        sweeper = LiveStateSweeper(live_state_store=store, grace_seconds=300)
        executor = _StubExecutor(finalized_event_ids=[1001, 1002])
        now = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)

        report = await sweeper.run_once(sql_executor=executor, now=now)

        self.assertEqual(report.scanned_finalized_count, 2)
        self.assertEqual(report.removed_event_count, 3)  # 1001 hot, 1002 hot, 1002 warm
        self.assertEqual(report.lane_removed_counts["hot"], 2)
        self.assertEqual(report.lane_removed_counts["warm"], 1)
        self.assertEqual(report.lane_removed_counts["cold"], 0)
        self.assertIsNone(report.error)
        self.assertNotIn("1001", store.backend.zsets["zset:live:hot"])
        self.assertNotIn("1002", store.backend.zsets["zset:live:hot"])
        self.assertNotIn("1002", store.backend.zsets["zset:live:warm"])
        # 1003 не в finalized_event_ids — должен остаться
        self.assertIn("1003", store.backend.zsets["zset:live:hot"])
        # cold contains an event not in finalized — must remain
        self.assertIn("9999", store.backend.zsets["zset:live:cold"])

    async def test_cutoff_is_passed_to_query(self) -> None:
        store = _StubLiveStateStore()
        sweeper = LiveStateSweeper(live_state_store=store, grace_seconds=600)
        executor = _StubExecutor(finalized_event_ids=[])
        now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)

        await sweeper.run_once(sql_executor=executor, now=now)

        self.assertEqual(len(executor.fetch_args), 1)
        cutoff, limit = executor.fetch_args[0]
        self.assertEqual(cutoff, now - timedelta(seconds=600))
        # Default raised 500 -> 5000 on 2026-05-15: a 24k/6h finalisation
        # backlog meant LIMIT 500 ASC burned its budget on long-finalised
        # events that were no longer in the zset, leaving fresh
        # zombie_stale entries stuck. See _fetch_finalized_event_ids
        # docstring for the DESC + bigger LIMIT rationale.
        self.assertEqual(limit, 5000)

    async def test_query_orders_finalized_desc_to_target_fresh_entries(self) -> None:
        """Sweep MUST query freshest finalisations first.

        Previously ORDER BY finalized_at ASC wasted the LIMIT budget on
        events finalised hours ago that were no longer in the zsets;
        recent zombie_stale entries (10-40 min) were never reached and
        kept oldest_hot_score_age permanently above the CRIT threshold.
        """

        store = _StubLiveStateStore()
        sweeper = LiveStateSweeper(live_state_store=store)
        captured_query: list[str] = []

        class _QueryCapturingExecutor:
            async def fetch(self, query: str, *args):
                captured_query.append(query)
                del args
                return []

        executor = _QueryCapturingExecutor()
        now = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
        await sweeper.run_once(sql_executor=executor, now=now)

        self.assertEqual(len(captured_query), 1)
        normalised = " ".join(captured_query[0].split()).upper()
        self.assertIn("ORDER BY FINALIZED_AT DESC", normalised)
        self.assertNotIn("ORDER BY FINALIZED_AT ASC", normalised)

    async def test_idempotent_second_run_is_noop(self) -> None:
        store = _StubLiveStateStore()
        now_ms = 1_700_000_000_000
        store.backend.zadd("zset:live:hot", {"1001": now_ms})
        sweeper = LiveStateSweeper(live_state_store=store, grace_seconds=300)
        executor = _StubExecutor(finalized_event_ids=[1001])
        now = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)

        first = await sweeper.run_once(sql_executor=executor, now=now)
        second = await sweeper.run_once(sql_executor=executor, now=now)

        self.assertEqual(first.removed_event_count, 1)
        self.assertEqual(second.removed_event_count, 0)
        self.assertNotIn("1001", store.backend.zsets["zset:live:hot"])

    async def test_returns_error_on_db_failure(self) -> None:
        store = _StubLiveStateStore()
        sweeper = LiveStateSweeper(live_state_store=store)
        executor = _StubExecutor(raise_on_fetch=True)
        now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)

        report = await sweeper.run_once(sql_executor=executor, now=now)

        self.assertIsNotNone(report.error)
        self.assertIn("forced db error", report.error)
        self.assertEqual(report.removed_event_count, 0)

    async def test_resilient_to_zrange_failure(self) -> None:
        store = _StubLiveStateStore()
        store.backend.raise_on_zrange = True
        sweeper = LiveStateSweeper(live_state_store=store)
        executor = _StubExecutor(finalized_event_ids=[1001])
        now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)

        report = await sweeper.run_once(sql_executor=executor, now=now)

        # zrange empty → no ZREMs → 0 removed, but sweep finishes cleanly.
        self.assertEqual(report.removed_event_count, 0)

    async def test_max_remove_per_tick_passed_to_query(self) -> None:
        store = _StubLiveStateStore()
        sweeper = LiveStateSweeper(live_state_store=store, max_remove_per_tick=42)
        executor = _StubExecutor(finalized_event_ids=[])
        now = datetime(2026, 5, 14, 12, 0, tzinfo=timezone.utc)

        await sweeper.run_once(sql_executor=executor, now=now)

        cutoff, limit = executor.fetch_args[0]
        del cutoff
        self.assertEqual(limit, 42)

    async def test_constructor_validates_arguments(self) -> None:
        store = _StubLiveStateStore()
        with self.assertRaises(ValueError):
            LiveStateSweeper(live_state_store=store, grace_seconds=-1)
        with self.assertRaises(ValueError):
            LiveStateSweeper(live_state_store=store, max_remove_per_tick=0)


class PlannerDaemonSweepIntegrationTests(unittest.IsolatedAsyncioTestCase):
    """Confirm PlannerDaemon._maybe_sweep_live_state schedules the callback."""

    async def test_callback_skipped_on_first_tick_then_fires_after_interval(self) -> None:
        from schema_inspector.services.planner_daemon import PlannerDaemon

        invocations: list[int] = []

        async def callback(now_ms: int) -> None:
            invocations.append(now_ms)

        daemon = PlannerDaemon.__new__(PlannerDaemon)
        daemon.live_state_sweep_callback = callback
        daemon.live_state_sweep_interval_ms = 1000
        daemon._next_live_state_sweep_ms = None

        # First tick — schedule next sweep but don't fire
        await daemon._maybe_sweep_live_state(now_ms=10_000)
        self.assertEqual(invocations, [])
        self.assertEqual(daemon._next_live_state_sweep_ms, 11_000)

        # Tick before interval elapsed — still no fire
        await daemon._maybe_sweep_live_state(now_ms=10_500)
        self.assertEqual(invocations, [])

        # Tick after interval elapsed — fires
        await daemon._maybe_sweep_live_state(now_ms=11_500)
        self.assertEqual(invocations, [11_500])
        self.assertEqual(daemon._next_live_state_sweep_ms, 12_500)

    async def test_callback_exception_does_not_crash_tick(self) -> None:
        from schema_inspector.services.planner_daemon import PlannerDaemon

        async def callback(now_ms: int) -> None:
            del now_ms
            raise RuntimeError("forced sweep failure")

        daemon = PlannerDaemon.__new__(PlannerDaemon)
        daemon.live_state_sweep_callback = callback
        daemon.live_state_sweep_interval_ms = 1000
        # Already past first defer:
        daemon._next_live_state_sweep_ms = 10_000

        # Should not raise
        await daemon._maybe_sweep_live_state(now_ms=11_000)

        # Next sweep is still scheduled despite the exception
        self.assertEqual(daemon._next_live_state_sweep_ms, 12_000)

    async def test_callback_disabled_when_callback_is_none(self) -> None:
        from schema_inspector.services.planner_daemon import PlannerDaemon

        daemon = PlannerDaemon.__new__(PlannerDaemon)
        daemon.live_state_sweep_callback = None
        daemon.live_state_sweep_interval_ms = 1000
        daemon._next_live_state_sweep_ms = None

        # No callback => no scheduling, no fire
        await daemon._maybe_sweep_live_state(now_ms=10_000)
        self.assertIsNone(daemon._next_live_state_sweep_ms)

    async def test_callback_disabled_when_interval_zero(self) -> None:
        from schema_inspector.services.planner_daemon import PlannerDaemon

        invocations: list[int] = []

        async def callback(now_ms: int) -> None:
            invocations.append(now_ms)

        daemon = PlannerDaemon.__new__(PlannerDaemon)
        daemon.live_state_sweep_callback = callback
        daemon.live_state_sweep_interval_ms = 0
        daemon._next_live_state_sweep_ms = None

        await daemon._maybe_sweep_live_state(now_ms=10_000)
        self.assertEqual(invocations, [])


if __name__ == "__main__":
    unittest.main()
