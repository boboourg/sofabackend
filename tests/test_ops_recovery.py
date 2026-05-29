from __future__ import annotations

import unittest

from schema_inspector.storage.live_state_repository import EventLiveStateHistoryRecord, EventTerminalStateRecord


class OpsRecoveryTests(unittest.IsolatedAsyncioTestCase):
    def test_replay_snapshot_ids_replays_in_order(self) -> None:
        from schema_inspector.ops.recovery import replay_snapshot_ids

        orchestrator = _FakeOrchestrator()

        results = replay_snapshot_ids(orchestrator, (11, 12, 13))

        self.assertEqual(orchestrator.replayed, [11, 12, 13])
        self.assertEqual(results, ("parsed:11", "parsed:12", "parsed:13"))

    async def test_rebuild_live_state_from_postgres_history(self) -> None:
        from schema_inspector.ops.recovery import rebuild_live_state_from_postgres
        from schema_inspector.queue.live_state import LiveEventStateStore

        backend = _FakeRedisBackend()
        store = LiveEventStateStore(backend)
        repository = _FakeLiveStateRepository(
            history_rows=(
                EventLiveStateHistoryRecord(
                    event_id=100,
                    observed_status_type="scheduled",
                    poll_profile="warm",
                    home_score=None,
                    away_score=None,
                    period_label=None,
                    observed_at="2026-04-17T10:00:00+00:00",
                ),
                EventLiveStateHistoryRecord(
                    event_id=200,
                    observed_status_type="inprogress",
                    poll_profile="hot",
                    home_score=1,
                    away_score=0,
                    period_label="2nd",
                    observed_at="2026-04-17T10:01:00+00:00",
                ),
            ),
            terminal_rows=(
                EventTerminalStateRecord(
                    event_id=300,
                    terminal_status="finished",
                    finalized_at="2026-04-17T10:02:00+00:00",
                    final_snapshot_id=999,
                ),
            ),
        )

        report = await rebuild_live_state_from_postgres(
            repository=repository,
            sql_executor=object(),
            live_state_store=store,
            now_ms=1_800_000_000_000,
        )

        self.assertEqual(report.restored_hot, 1)
        self.assertEqual(report.restored_warm, 1)
        self.assertEqual(report.restored_terminal, 1)
        self.assertIn("200", backend.sorted_sets[store.hot_zset_key])
        self.assertIn("100", backend.sorted_sets[store.warm_zset_key])
        self.assertNotIn("300", backend.sorted_sets[store.hot_zset_key])

    async def test_rebuild_routes_unknown_poll_profile_to_warm_not_cold(self) -> None:
        from schema_inspector.ops.recovery import rebuild_live_state_from_postgres
        from schema_inspector.queue.live_state import LiveEventStateStore

        backend = _FakeRedisBackend()
        store = LiveEventStateStore(backend)
        repository = _FakeLiveStateRepository(
            history_rows=(
                # poll_profile missing (NULL in DB) -> must NOT go to cold
                EventLiveStateHistoryRecord(
                    event_id=401,
                    observed_status_type="inprogress",
                    poll_profile=None,
                    home_score=0,
                    away_score=0,
                    period_label="1st",
                    observed_at="2026-04-17T10:00:00+00:00",
                ),
                # unrecognised label (not hot/warm/cold) -> must NOT go to cold
                EventLiveStateHistoryRecord(
                    event_id=402,
                    observed_status_type="inprogress",
                    poll_profile="scheduled",
                    home_score=1,
                    away_score=1,
                    period_label="2nd",
                    observed_at="2026-04-17T10:01:00+00:00",
                ),
            ),
            terminal_rows=(),
        )

        report = await rebuild_live_state_from_postgres(
            repository=repository,
            sql_executor=object(),
            live_state_store=store,
            now_ms=1_800_000_000_000,
        )

        # Both unknown-profile events land in the polled warm lane.
        self.assertEqual(report.restored_warm, 2)
        # Nothing should leak into the un-polled, un-sweepable cold lane.
        self.assertEqual(report.restored_cold, 0)
        warm_members = backend.sorted_sets.get(store.warm_zset_key, {})
        cold_members = backend.sorted_sets.get(store.cold_zset_key, {})
        self.assertIn("401", warm_members)
        self.assertIn("402", warm_members)
        self.assertNotIn("401", cold_members)
        self.assertNotIn("402", cold_members)
        # Per-event hash reflects the corrected lane.
        self.assertEqual(backend.hashes["live:event:401"]["poll_profile"], "warm")
        self.assertEqual(backend.hashes["live:event:402"]["poll_profile"], "warm")


class _FakeOrchestrator:
    def __init__(self) -> None:
        self.replayed = []

    def replay_snapshot(self, snapshot_id: int):
        self.replayed.append(snapshot_id)
        return f"parsed:{snapshot_id}"


class _FakeLiveStateRepository:
    def __init__(self, *, history_rows, terminal_rows) -> None:
        self.history_rows = history_rows
        self.terminal_rows = terminal_rows

    async def fetch_latest_live_state_history(self, executor):
        del executor
        return self.history_rows

    async def fetch_terminal_states(self, executor):
        del executor
        return self.terminal_rows


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.hashes = {}
        self.sorted_sets = {}

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        self.hashes[key] = dict(mapping)
        return 1

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.sorted_sets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[str(member)] = float(score)
        return len(mapping)

    def zrem(self, key: str, *members: str) -> int:
        bucket = self.sorted_sets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    def zrangebyscore(self, key: str, min_score: float, max_score: float):
        bucket = self.sorted_sets.get(key, {})
        return [member for member, score in bucket.items() if min_score <= score <= max_score]


if __name__ == "__main__":
    unittest.main()
