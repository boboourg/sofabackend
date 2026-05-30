"""TDD tests for Task 2 — Final-Sync Lifecycle (Batch 1 = Phase A + B).

Phase A — Schema + constants + helpers
* ``event_terminal_state.locked_at`` column + partial index.
* ``pipeline.pilot_orchestrator.FINAL_SYNC_DELAY_SECONDS`` constant
  (env override ``SOFASCORE_FINAL_SYNC_DELAY_SECONDS``).
* ``LiveStateRepository`` lock helpers: is_event_locked,
  set_event_locked, clear_event_lock, pending_lock_event_ids.

Phase B — FinalSyncPlannerDaemon + orchestrator integration
* New ``FinalSyncPlannerDaemon`` daemon that picks events with
  ``locked_at IS NULL AND finalized_at <= now() - delay`` and
  publishes ``JOB_HYDRATE_EVENT_ROOT`` to ``stream:etl:hydrate``
  with ``scope="final_sync"``, ``hydration_mode="final_sync"``.
* ``pilot_orchestrator.run_event`` stamps ``locked_at = now()``
  via ``set_event_locked`` on successful ``scope="final_sync"`` run.
"""

from __future__ import annotations

import asyncio
import os
import unittest


# ---------------------------------------------------------------------------
# Phase A — Constants
# ---------------------------------------------------------------------------


class FinalSyncConstantsTests(unittest.TestCase):
    def test_default_delay_is_2_hours(self) -> None:
        # Ensure env doesn't pollute the import
        prev = os.environ.pop("SOFASCORE_FINAL_SYNC_DELAY_SECONDS", None)
        try:
            import importlib

            from schema_inspector.pipeline import pilot_orchestrator

            importlib.reload(pilot_orchestrator)
            self.assertEqual(pilot_orchestrator.FINAL_SYNC_DELAY_SECONDS, 7200)
            self.assertEqual(pilot_orchestrator.FINAL_SYNC_SCOPE, "final_sync")
        finally:
            if prev is not None:
                os.environ["SOFASCORE_FINAL_SYNC_DELAY_SECONDS"] = prev


# ---------------------------------------------------------------------------
# Phase A — LiveStateRepository lock helpers
# ---------------------------------------------------------------------------


class _FakeRow(dict):
    def __getitem__(self, key):
        return super().__getitem__(key)


class _FakeExecutor:
    """In-memory event_terminal_state stand-in for repository unit tests."""

    def __init__(self) -> None:
        # event_id -> {"locked_at": str|None, "finalized_at": str}
        self.rows: dict[int, dict[str, object]] = {}
        self.queries: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object):
        self.queries.append((query, args))
        q = query.strip().lower()
        if "update event_terminal_state" in q and "set locked_at = now()" in q:
            event_id = int(args[0])
            row = self.rows.get(event_id)
            if row is not None and row.get("locked_at") is None:
                row["locked_at"] = "now"
                return "UPDATE 1"
            return "UPDATE 0"
        if "update event_terminal_state" in q and "set locked_at = null" in q:
            event_id = int(args[0])
            row = self.rows.get(event_id)
            if row is not None and row.get("locked_at") is not None:
                row["locked_at"] = None
                return "UPDATE 1"
            return "UPDATE 0"
        return "OK"

    async def fetchval(self, query: str, *args: object):
        q = query.strip().lower()
        if "select 1" in q and "locked_at is not null" in q:
            event_id = int(args[0])
            row = self.rows.get(event_id)
            if row is not None and row.get("locked_at") is not None:
                return 1
            return None
        return None

    async def fetch(self, query: str, *args: object):
        q = query.strip().lower()
        if "from event_terminal_state" in q and "locked_at is null" in q:
            delay_seconds = int(args[0])
            limit = int(args[1])
            # In this fake we keep finalized_at as a sortable int 'minutes ago'
            due = [
                (event_id, info["finalized_at"])
                for event_id, info in self.rows.items()
                if info.get("locked_at") is None
                and isinstance(info["finalized_at"], int)
                and info["finalized_at"] >= delay_seconds
            ]
            due.sort(key=lambda pair: (-pair[1], pair[0]))  # oldest first
            picked = [eid for eid, _ in due[:limit]]
            return [_FakeRow({"event_id": eid}) for eid in picked]
        return []


class LiveStateRepositoryLockHelpersTests(unittest.IsolatedAsyncioTestCase):
    async def test_is_event_locked_returns_false_when_no_row(self) -> None:
        from schema_inspector.storage.live_state_repository import (
            LiveStateRepository,
        )

        repo = LiveStateRepository()
        executor = _FakeExecutor()
        self.assertFalse(await repo.is_event_locked(executor, event_id=42))

    async def test_is_event_locked_returns_true_when_locked(self) -> None:
        from schema_inspector.storage.live_state_repository import (
            LiveStateRepository,
        )

        repo = LiveStateRepository()
        executor = _FakeExecutor()
        executor.rows[42] = {"locked_at": "2026-05-20T00:00:00Z", "finalized_at": 10000}
        self.assertTrue(await repo.is_event_locked(executor, event_id=42))

    async def test_set_event_locked_idempotent(self) -> None:
        from schema_inspector.storage.live_state_repository import (
            LiveStateRepository,
        )

        repo = LiveStateRepository()
        executor = _FakeExecutor()
        executor.rows[42] = {"locked_at": None, "finalized_at": 10000}
        # First call - locks
        self.assertTrue(await repo.set_event_locked(executor, event_id=42))
        # Second call - already locked, no-op
        self.assertFalse(await repo.set_event_locked(executor, event_id=42))

    async def test_clear_event_lock_unsets(self) -> None:
        from schema_inspector.storage.live_state_repository import (
            LiveStateRepository,
        )

        repo = LiveStateRepository()
        executor = _FakeExecutor()
        executor.rows[42] = {"locked_at": "2026-05-20T00:00:00Z", "finalized_at": 10000}
        self.assertTrue(await repo.clear_event_lock(executor, event_id=42))
        self.assertIsNone(executor.rows[42]["locked_at"])

    async def test_pending_lock_event_ids_filters_by_delay_and_locked_null(
        self,
    ) -> None:
        from schema_inspector.storage.live_state_repository import (
            LiveStateRepository,
        )

        repo = LiveStateRepository()
        executor = _FakeExecutor()
        # finalized_at fake = "seconds ago since fake now"
        executor.rows[1] = {"locked_at": None, "finalized_at": 10000}    # 10000s ago
        executor.rows[2] = {"locked_at": None, "finalized_at": 5000}     # 5000s ago
        executor.rows[3] = {"locked_at": None, "finalized_at": 7300}     # 7300s ago
        executor.rows[4] = {"locked_at": "stamped", "finalized_at": 99999}  # locked
        ids = await repo.pending_lock_event_ids(
            executor, delay_seconds=7200, limit=10
        )
        # only events 1, 3 are 7200+ old AND unlocked. Sorted oldest first.
        self.assertEqual(ids, [1, 3])


# ---------------------------------------------------------------------------
# Phase B — FinalSyncPlannerDaemon
# ---------------------------------------------------------------------------


class FinalSyncPlannerDaemonTests(unittest.IsolatedAsyncioTestCase):
    async def test_tick_publishes_due_events_with_correct_scope(self) -> None:
        from schema_inspector.services.final_sync_planner import (
            FinalSyncPlannerDaemon,
        )

        published: list[dict] = []

        class _FakeQueue:
            def publish(self, stream, payload):
                published.append({"stream": stream, "payload": payload})

        class _FakeRepo:
            async def pending_lock_event_ids(self, executor, *, delay_seconds, limit):
                return [101, 202, 303]

        class _FakeDb:
            class _Conn:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

            def connection(self):
                return _FakeDb._Conn()

        daemon = FinalSyncPlannerDaemon(
            database=_FakeDb(),
            queue=_FakeQueue(),
            repository=_FakeRepo(),
            delay_seconds=7200,
            batch_size=100,
            tick_interval_seconds=60,
        )

        count = await daemon.tick(now_ms=0)
        self.assertEqual(count, 3)
        self.assertEqual(len(published), 3)
        for entry in published:
            self.assertEqual(entry["stream"], "stream:etl:hydrate")
            # encode_stream_job emits a flat dict with stringified
            # fields ready for Redis Streams XADD. Probe the values
            # rather than the typed dataclass.
            payload = entry["payload"]
            self.assertEqual(payload["job_type"], "hydrate_event_root")
            self.assertEqual(payload["scope"], "final_sync")
            # encode_stream_job serializes params into params_json
            self.assertIn("final_sync", str(payload.get("params_json") or ""))
            self.assertTrue(payload.get("entity_id"))

    async def test_tick_returns_zero_when_no_due_events(self) -> None:
        from schema_inspector.services.final_sync_planner import (
            FinalSyncPlannerDaemon,
        )

        published: list[dict] = []

        class _FakeQueue:
            def publish(self, stream, payload):
                published.append({"stream": stream, "payload": payload})

        class _FakeRepo:
            async def pending_lock_event_ids(self, *args, **kwargs):
                return []

        class _FakeDb:
            class _Conn:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, exc_type, exc, tb):
                    return False

            def connection(self):
                return _FakeDb._Conn()

        daemon = FinalSyncPlannerDaemon(
            database=_FakeDb(),
            queue=_FakeQueue(),
            repository=_FakeRepo(),
            delay_seconds=7200,
            batch_size=100,
            tick_interval_seconds=60,
        )

        count = await daemon.tick(now_ms=0)
        self.assertEqual(count, 0)
        self.assertEqual(published, [])


# ---------------------------------------------------------------------------
# Phase B — Orchestrator stamps locked_at on final_sync scope
# ---------------------------------------------------------------------------


class OrchestratorStampsLockAfterFinalSyncTests(unittest.TestCase):
    """Source-level invariant: ``pilot_orchestrator.run_event`` must
    contain a call to ``set_event_locked`` gated on ``scope ==
    FINAL_SYNC_SCOPE``. This guarantees the freeze is wired even if
    the runtime tests don't exercise the full pipeline."""

    def test_run_event_calls_set_event_locked_on_final_sync_scope(self) -> None:
        from pathlib import Path

        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "pipeline"
            / "pilot_orchestrator.py"
        ).read_text(encoding="utf-8")
        # The orchestrator must mention FINAL_SYNC_SCOPE as a guard
        # AND must call set_event_locked somewhere.
        self.assertIn("FINAL_SYNC_SCOPE", text)
        self.assertIn("set_event_locked", text)


# ---------------------------------------------------------------------------
# Phase B regression — HybridApp.run_event must forward ``scope`` to the
# inner PilotOrchestrator. The 2026-05-20 production crashloop was a
# silent TypeError because hydrate_worker passed ``scope="final_sync"`` to
# HybridApp.run_event but the kwarg was never wired through. These
# checks pin the wiring at the source level.
# ---------------------------------------------------------------------------


class HybridAppForwardsScopeTests(unittest.TestCase):
    """``schema_inspector/cli.py:HybridApp`` is the orchestrator instance
    the hydrate worker holds. It must accept ``scope`` on ``run_event``
    AND propagate it through ``_persist_prefetched_run`` into the inner
    ``PilotOrchestrator.run_event`` so the locked_at stamping logic
    actually fires for final-sync jobs."""

    def test_hybrid_app_run_event_signature_has_scope_kwarg(self) -> None:
        import inspect
        from schema_inspector.cli import HybridApp

        sig = inspect.signature(HybridApp.run_event)
        self.assertIn(
            "scope",
            sig.parameters,
            msg=(
                "HybridApp.run_event must accept scope kwarg; otherwise "
                "hydrate_worker.handle() crashes with TypeError when the "
                "final_sync planner enqueues a job."
            ),
        )

    def test_hybrid_app_persist_prefetched_run_forwards_scope(self) -> None:
        import re
        from pathlib import Path

        text = (
            Path(__file__).resolve().parent.parent
            / "schema_inspector"
            / "cli.py"
        ).read_text(encoding="utf-8")
        # Locate _persist_prefetched_run and ensure the inner
        # ``orchestrator.run_event(...)`` invocation passes scope=
        match = re.search(
            r"async def _persist_prefetched_run\(.*?return result",
            text,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(
            match, msg="_persist_prefetched_run not found in cli.py"
        )
        body = match.group(0)
        self.assertIn(
            "scope=",
            body,
            msg=(
                "_persist_prefetched_run must forward scope= to the inner "
                "PilotOrchestrator.run_event call."
            ),
        )


class FinalSyncBackpressureAndCooldownTests(unittest.IsolatedAsyncioTestCase):
    """2026-05-30: the planner must (a) pause when cg:hydrate lag is high and
    (b) not re-publish the same event every tick (per-event cooldown). Without
    these it re-spammed the hydrate stream into a 3.8M-message backlog."""

    def _daemon(self, *, queue, repo_ids, dedupe=None, lag_threshold=5000, cooldown=3600):
        from schema_inspector.services.final_sync_planner import FinalSyncPlannerDaemon

        class _FakeRepo:
            async def pending_lock_event_ids(self, executor, *, delay_seconds, limit):
                return list(repo_ids)

        class _FakeDb:
            class _Conn:
                async def __aenter__(self):
                    return self

                async def __aexit__(self, *a):
                    return False

            def connection(self):
                return _FakeDb._Conn()

        return FinalSyncPlannerDaemon(
            database=_FakeDb(),
            queue=queue,
            repository=_FakeRepo(),
            dedupe_store=dedupe,
            lag_threshold=lag_threshold,
            publish_cooldown_seconds=cooldown,
        )

    async def test_skips_tick_under_hydrate_backpressure(self) -> None:
        published: list = []

        class _Q:
            def publish(self, stream, payload):
                published.append(payload)

            def group_info(self, stream, group):
                class _Info:
                    lag = 9999
                return _Info()

        daemon = self._daemon(queue=_Q(), repo_ids=[1, 2, 3], lag_threshold=5000)
        count = await daemon.tick(now_ms=0)
        self.assertEqual(count, 0)
        self.assertEqual(published, [])  # nothing published while saturated

    async def test_publishes_when_lag_below_threshold(self) -> None:
        published: list = []

        class _Q:
            def publish(self, stream, payload):
                published.append(payload)

            def group_info(self, stream, group):
                class _Info:
                    lag = 10
                return _Info()

        daemon = self._daemon(queue=_Q(), repo_ids=[1, 2, 3], lag_threshold=5000)
        count = await daemon.tick(now_ms=0)
        self.assertEqual(count, 3)

    async def test_cooldown_dedup_skips_recently_published(self) -> None:
        published: list = []

        class _Q:
            def publish(self, stream, payload):
                published.append(payload)

            def group_info(self, stream, group):
                class _Info:
                    lag = 0
                return _Info()

        class _Dedupe:
            def __init__(self) -> None:
                self.claimed: set[str] = set()

            def claim_job(self, key, *, ttl_ms, now_ms=None):
                if key in self.claimed:
                    return False
                self.claimed.add(key)
                return True

        dedupe = _Dedupe()
        daemon = self._daemon(queue=_Q(), repo_ids=[1, 2, 3], dedupe=dedupe)
        first = await daemon.tick(now_ms=0)
        second = await daemon.tick(now_ms=0)
        self.assertEqual(first, 3)  # first sweep publishes all
        self.assertEqual(second, 0)  # same events in cooldown -> none re-published
        self.assertEqual(len(published), 3)  # no duplicate amplification


if __name__ == "__main__":
    unittest.main()
