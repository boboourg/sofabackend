"""Conservative terminal-freeze guards (2026-05-30).

Two independent layers stop a STILL-LIVE event from being frozen by the
final-sync lock:
  (a) pilot_orchestrator.run_event stamps locked_at only when the run is
      genuinely terminal (finalized OR status_type in TERMINAL_STATUS_TYPES).
  (b) LiveStateRepository.pending_lock_event_ids excludes synthetic
      terminal rows (zombie_stale, not_found) from the candidate feed.

Guard (b) is exercised behaviourally against the in-memory fake; guard
(a) is pinned at the source level (run_event is a ~700-line method that
cannot be unit-instantiated on the prod venv without the full wiring).
"""

from __future__ import annotations

import re
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


class _FakeRow(dict):
    pass


class _FakeExecutor:
    """In-memory event_terminal_state stand-in that models terminal_status
    and the synthetic-row exclusion arg ($3 = text[]).

    rows: event_id -> {locked_at: str|None, finalized_at: int ("secs ago"),
                       terminal_status: str}
    """

    def __init__(self) -> None:
        self.rows: dict[int, dict[str, object]] = {}
        self.queries: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, query: str, *args: object):
        self.queries.append((query, args))
        q = query.strip().lower()
        if "from event_terminal_state" in q and "locked_at is null" in q:
            delay_seconds = int(args[0])
            limit = int(args[1])
            excluded: set[str] = set()
            # New guard: third bind param is the excluded terminal_status[].
            if len(args) >= 3 and args[2] is not None:
                excluded = {str(s) for s in args[2]}
            due = [
                (eid, info["finalized_at"])
                for eid, info in self.rows.items()
                if info.get("locked_at") is None
                and isinstance(info["finalized_at"], int)
                and info["finalized_at"] >= delay_seconds
                and (
                    "terminal_status <> all" not in q
                    or str(info.get("terminal_status")) not in excluded
                )
            ]
            due.sort(key=lambda pair: (-pair[1], pair[0]))  # oldest first
            return [_FakeRow({"event_id": eid}) for eid, _ in due[:limit]]
        return []


class PendingLockExcludesSyntheticRowsTests(unittest.IsolatedAsyncioTestCase):
    def _repo(self):
        from schema_inspector.storage.live_state_repository import (
            LiveStateRepository,
        )

        return LiveStateRepository()

    async def test_excludes_zombie_stale_rows(self) -> None:
        repo = self._repo()
        ex = _FakeExecutor()
        ex.rows[1] = {"locked_at": None, "finalized_at": 10000, "terminal_status": "zombie_stale"}
        ids = await repo.pending_lock_event_ids(ex, delay_seconds=7200, limit=10)
        self.assertEqual(ids, [])

    async def test_excludes_not_found_rows(self) -> None:
        repo = self._repo()
        ex = _FakeExecutor()
        ex.rows[2] = {"locked_at": None, "finalized_at": 10000, "terminal_status": "not_found"}
        ids = await repo.pending_lock_event_ids(ex, delay_seconds=7200, limit=10)
        self.assertEqual(ids, [])

    async def test_keeps_genuine_terminal_rows(self) -> None:
        repo = self._repo()
        ex = _FakeExecutor()
        ex.rows[10] = {"locked_at": None, "finalized_at": 9000, "terminal_status": "finished"}
        ex.rows[11] = {"locked_at": None, "finalized_at": 12000, "terminal_status": "afterpen"}
        ex.rows[12] = {"locked_at": None, "finalized_at": 8000, "terminal_status": "postponed"}
        ex.rows[13] = {"locked_at": None, "finalized_at": 9500, "terminal_status": "zombie_stale"}  # excluded
        ex.rows[14] = {"locked_at": "stamped", "finalized_at": 99999, "terminal_status": "finished"}  # locked
        ids = await repo.pending_lock_event_ids(ex, delay_seconds=7200, limit=10)
        self.assertEqual(ids, [11, 10, 12])  # oldest finalized_at first; zombie + locked dropped

    def test_non_terminal_lock_statuses_constant(self) -> None:
        from schema_inspector.storage.live_state_repository import (
            LiveStateRepository,
        )

        self.assertEqual(
            LiveStateRepository._NON_TERMINAL_LOCK_STATUSES,
            ("zombie_stale", "not_found"),
        )

    async def test_query_text_filters_terminal_status(self) -> None:
        repo = self._repo()
        ex = _FakeExecutor()
        await repo.pending_lock_event_ids(ex, delay_seconds=7200, limit=5)
        self.assertTrue(ex.queries, "pending_lock_event_ids issued no query")
        sql = ex.queries[-1][0].lower()
        self.assertIn("terminal_status <> all", sql)


class OrchestratorStampGatedOnTerminalSourceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.text = (
            REPO_ROOT / "schema_inspector" / "pipeline" / "pilot_orchestrator.py"
        ).read_text(encoding="utf-8")

    def test_stamp_block_is_gated_on_terminal(self) -> None:
        self.assertIn("is_terminal_for_lock", self.text)
        self.assertIn("TERMINAL_STATUS_TYPES", self.text)

    def test_stamp_not_unconditional_on_classification(self) -> None:
        # Between the final_sync branch header and the set_event_locked
        # call, the terminal gate must appear — proving the stamp is no
        # longer reachable on a non-terminal successful root fetch.
        m = re.search(
            r"normalized_scope == FINAL_SYNC_SCOPE.*?set_event_locked\(",
            self.text,
            flags=re.DOTALL,
        )
        self.assertIsNotNone(
            m, "could not locate the final_sync stamp block"
        )
        self.assertIn("is_terminal_for_lock", m.group(0))


if __name__ == "__main__":
    unittest.main()
