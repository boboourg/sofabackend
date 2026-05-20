"""TDD tests for Task 2 Batch 2 (Phase C + D) — defensive lock gates.

Phase C: terminal-lock skip in workers
* ``HydrateWorker.handle()`` — skip + ACK with ``"terminal_locked_skip"``
  when the event has ``event_terminal_state.locked_at IS NOT NULL``,
  UNLESS the job scope is ``"final_sync"`` (the lock-stamp path itself)
  or starts with ``"historical"`` (deliberate replay).
* ``LiveWorkerService.handle()`` — same gate.
* ``LiveRescue`` — skip locked events in the ``no_live_state`` branch.

Phase D: read-path freeze + retention pin
* ``_fetch_event_root_payload`` — if event is locked, serve the captured
  ``event_terminal_state.final_snapshot_id`` payload 1:1 and skip the
  live-overlay synthesis / normalize fallback chain.
* ``event_payload_cache`` TTL = 86400s (1 day) for locked events.
* ``retention_repository.delete_legacy_snapshot_batch`` — skip snapshots
  referenced by ``event_terminal_state.final_snapshot_id`` when the
  matching row has ``locked_at IS NOT NULL``.
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Phase C — HydrateWorker terminal-lock gate
# ---------------------------------------------------------------------------


class _StubOrchestrator:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def run_event(self, **kwargs):
        self.calls.append(kwargs)
        return {"event_id": kwargs.get("event_id")}


class _StubInflightStore:
    def claim(self, *, event_id, owner):
        return True

    def release(self, *, event_id, owner):
        pass


class _StubLiveStateRepository:
    def __init__(self, locked_event_ids: set[int]) -> None:
        self.locked = set(locked_event_ids)
        self.checks: list[int] = []

    async def is_event_locked(self, executor, *, event_id):
        self.checks.append(int(event_id))
        return int(event_id) in self.locked


class _StubDb:
    class _Conn:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def connection(self):
        return _StubDb._Conn()


def _make_entry(*, event_id: int, scope: str = "discovery", job_id: str = "j1"):
    from schema_inspector.queue.streams import StreamEntry

    return StreamEntry(
        stream="stream:etl:hydrate",
        message_id="1-0",
        values={
            "job_type": "hydrate_event_root",
            "entity_id": str(event_id),
            "entity_type": "event",
            "sport_slug": "football",
            "job_id": job_id,
            "scope": scope,
        },
    )


def _build_hydrate_worker(*, orchestrator, lock_repo=None, locked_ids=None):
    from schema_inspector.workers.hydrate_worker import HydrateWorker

    worker = HydrateWorker.__new__(HydrateWorker)
    worker.orchestrator = orchestrator
    worker.consumer = "hydrate-test"
    worker.default_sport_slug = "football"
    worker.circuit_breaker = None
    worker.circuit_breaker_inprogress_count_provider = None
    worker.now_ms_factory = lambda: 0
    worker.hydrate_inflight_store = _StubInflightStore()
    if lock_repo is None:
        repo = _StubLiveStateRepository(locked_ids or set())
        worker.live_state_repository = repo
    else:
        worker.live_state_repository = lock_repo
    worker.database = _StubDb()
    return worker


class HydrateWorkerTerminalLockGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_handle_skips_when_event_is_locked(self) -> None:
        orchestrator = _StubOrchestrator()
        worker = _build_hydrate_worker(
            orchestrator=orchestrator, locked_ids={12345}
        )
        result = await worker.handle(_make_entry(event_id=12345))
        self.assertEqual(result, "terminal_locked_skip")
        self.assertEqual(orchestrator.calls, [])

    async def test_handle_bypasses_lock_for_final_sync_scope(self) -> None:
        orchestrator = _StubOrchestrator()
        worker = _build_hydrate_worker(
            orchestrator=orchestrator, locked_ids={12345}
        )
        # final_sync path is the one that STAMPS locked_at - must not skip.
        result = await worker.handle(
            _make_entry(event_id=12345, scope="final_sync")
        )
        self.assertEqual(result, "completed")
        self.assertEqual(len(orchestrator.calls), 1)

    async def test_handle_bypasses_lock_for_historical_scope(self) -> None:
        orchestrator = _StubOrchestrator()
        worker = _build_hydrate_worker(
            orchestrator=orchestrator, locked_ids={12345}
        )
        result = await worker.handle(
            _make_entry(event_id=12345, scope="historical_tournament")
        )
        self.assertEqual(result, "completed")
        self.assertEqual(len(orchestrator.calls), 1)

    async def test_handle_processes_normally_when_not_locked(self) -> None:
        orchestrator = _StubOrchestrator()
        worker = _build_hydrate_worker(
            orchestrator=orchestrator, locked_ids=set()
        )
        result = await worker.handle(_make_entry(event_id=12345))
        self.assertEqual(result, "completed")
        self.assertEqual(len(orchestrator.calls), 1)

    async def test_handle_processes_normally_when_repo_is_none(self) -> None:
        """Backwards-compat: workers built without the live_state_repository
        (e.g. legacy CLI shims) must run as before — no gate."""
        orchestrator = _StubOrchestrator()
        worker = _build_hydrate_worker(orchestrator=orchestrator)
        worker.live_state_repository = None
        result = await worker.handle(_make_entry(event_id=12345))
        self.assertEqual(result, "completed")


# ---------------------------------------------------------------------------
# Phase D — Read-path freeze + retention pin (source-level invariants)
# ---------------------------------------------------------------------------


class ReadPathLockedFreezeSourceTests(unittest.TestCase):
    """Source-level checks that the read-path treats locked events
    differently. We assert the presence of the gate keywords; integration
    tests against a real DB are out of scope here."""

    def setUp(self) -> None:
        self.api_text = (
            REPO_ROOT / "schema_inspector" / "local_api_server.py"
        ).read_text(encoding="utf-8")

    def test_fetch_event_root_payload_consults_locked_at(self) -> None:
        # The handler body must reference locked_at to decide whether
        # to skip overlays and return the snapshot 1:1.
        m = re.search(
            r"async def _fetch_event_root_payload\(.*?(?=\n    async def )",
            self.api_text,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate _fetch_event_root_payload body")
        body = m.group(0)
        self.assertIn("locked_at", body)


class CacheTtlForLockedEventsSourceTests(unittest.TestCase):
    """``event_payload_cache`` TTL must rise to 86400s when the event is
    flagged as locked. The hot path returns the bytes directly without
    re-running snapshot waterfall."""

    def setUp(self) -> None:
        self.cache_text = (
            REPO_ROOT
            / "schema_inspector"
            / "event_payload_cache_repository.py"
        ).read_text(encoding="utf-8")

    def test_cache_repository_carries_locked_ttl_constant(self) -> None:
        # The constant naming is normative — pin it so downstream code
        # (and tests) reference the same identifier.
        self.assertRegex(
            self.cache_text,
            re.compile(r"LOCKED_TTL_SECONDS\s*=\s*86400", re.MULTILINE),
        )


class RetentionPinsLockedSnapshotsSourceTests(unittest.TestCase):
    """``delete_legacy_snapshot_batch`` must filter out snapshots whose
    id appears as ``final_snapshot_id`` of any locked
    ``event_terminal_state`` row."""

    def setUp(self) -> None:
        self.retention_text = (
            REPO_ROOT
            / "schema_inspector"
            / "storage"
            / "retention_repository.py"
        ).read_text(encoding="utf-8")

    def test_retention_query_excludes_locked_final_snapshots(self) -> None:
        m = re.search(
            r"async def delete_legacy_snapshot_batch\(.*?(?=\n    async def )",
            self.retention_text,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(m, "could not locate delete_legacy_snapshot_batch body")
        body = m.group(0)
        self.assertIn("event_terminal_state", body)
        self.assertIn("locked_at IS NOT NULL", body)
        # The DELETE query must reference final_snapshot_id (the column we pin).
        self.assertIn("final_snapshot_id", body)


# ---------------------------------------------------------------------------
# Phase C — LiveWorkerService source invariant
# (LiveWorkerService.handle is large; verify the gate is wired into source)
# ---------------------------------------------------------------------------


class LiveWorkerServiceTerminalLockSourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.text = (
            REPO_ROOT
            / "schema_inspector"
            / "workers"
            / "live_worker_service.py"
        ).read_text(encoding="utf-8")

    def test_handle_consults_live_state_repository_for_lock(self) -> None:
        # Look for the terminal-lock gate keywords.
        self.assertIn("is_event_locked", self.text)
        self.assertIn("terminal_locked_skip", self.text)


class LiveRescueTerminalLockSourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.text = (
            REPO_ROOT
            / "schema_inspector"
            / "services"
            / "live_rescue.py"
        ).read_text(encoding="utf-8")

    def test_no_live_state_branch_consults_lock(self) -> None:
        self.assertIn("is_event_locked", self.text)


if __name__ == "__main__":
    unittest.main()
