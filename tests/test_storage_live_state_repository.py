from __future__ import annotations

from datetime import datetime
import unittest

from schema_inspector.storage.live_state_repository import (
    EventLiveStateHistoryRecord,
    EventTerminalStateRecord,
    LiveStateRepository,
)


class _FakeExecutor:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, query: str, *args: object) -> str:
        self.execute_calls.append((query, args))
        return "OK"


class LiveStateRepositoryTests(unittest.IsolatedAsyncioTestCase):
    async def test_repository_writes_live_history_and_terminal_state(self) -> None:
        repository = LiveStateRepository()
        executor = _FakeExecutor()

        await repository.insert_live_state_history(
            executor,
            EventLiveStateHistoryRecord(
                event_id=14201603,
                observed_status_type="inprogress",
                poll_profile="hot",
                home_score=2,
                away_score=1,
                period_label="P3",
                observed_at="2026-04-16T19:00:00+00:00",
            ),
        )
        await repository.upsert_terminal_state(
            executor,
            EventTerminalStateRecord(
                event_id=14201603,
                terminal_status="finished",
                finalized_at="2026-04-16T20:05:00+00:00",
                final_snapshot_id=777,
            ),
        )

        statements = [sql for sql, _ in executor.execute_calls]
        self.assertTrue(any("INSERT INTO event_live_state_history" in sql for sql in statements))
        self.assertTrue(any("INSERT INTO event_terminal_state" in sql for sql in statements))
        history_args = executor.execute_calls[0][1]
        terminal_args = executor.execute_calls[1][1]
        self.assertIsInstance(history_args[6], datetime)
        self.assertIsInstance(terminal_args[2], datetime)


if __name__ == "__main__":
    unittest.main()
