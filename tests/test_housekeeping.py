from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import unittest

from schema_inspector.queue.live_state import (
    LIVE_COLD_ZSET,
    LIVE_HOT_ZSET,
    LIVE_WARM_ZSET,
    LiveEventState,
)


class HousekeepingConfigParsingTests(unittest.TestCase):
    def test_housekeeping_config_accepts_operational_env_aliases(self) -> None:
        from schema_inspector.services.housekeeping import HousekeepingConfig

        config = HousekeepingConfig.from_env(
            {
                "SOFASCORE_HOUSEKEEPING_ENABLED": "true",
                "SOFASCORE_HOUSEKEEPING_DRY_RUN": "true",
                "SOFASCORE_HOUSEKEEPING_INTERVAL_SECONDS": "60",
                "SOFASCORE_HOUSEKEEPING_BATCH_SIZE": "20000",
                "SOFASCORE_HOUSEKEEPING_ZOMBIE_MAX_AGE_MINUTES": "120",
            }
        )

        self.assertTrue(config.enabled)
        self.assertTrue(config.dry_run)
        self.assertEqual(config.interval_s, 60.0)
        self.assertEqual(config.batch_size, 20_000)
        self.assertEqual(config.zombie_max_age_minutes, 120)


class HousekeepingLoopTests(unittest.IsolatedAsyncioTestCase):
    async def test_housekeeping_loop_reports_dry_run_counts_and_zombies_without_mutation(self) -> None:
        from schema_inspector.services.housekeeping import HousekeepingConfig, HousekeepingLoop

        now = datetime(2026, 4, 20, 5, 0, tzinfo=timezone.utc)
        now_ms = int(now.timestamp() * 1000)
        retention = _FakeRetentionRepository(
            request_log_count=11,
            snapshot_count=22,
            live_history_count=33,
        )
        live_state_repository = _FakeLiveStateRepository()
        redis_backend = _FakeRedisBackend(
            zsets={
                LIVE_HOT_ZSET: ("7001", "7002"),
                LIVE_WARM_ZSET: ("7003",),
            }
        )
        live_state_store = _FakeLiveStateStore(
            {
                7001: _live_state(event_id=7001, last_ingested_at=now_ms - 3 * 60 * 60 * 1000),
                7002: _live_state(event_id=7002, last_ingested_at=now_ms - 5 * 60 * 1000),
            }
        )
        loop = HousekeepingLoop(
            config=HousekeepingConfig(enabled=True, dry_run=True, zombie_max_age_minutes=120),
            connection_factory=lambda: _FakeConnectionContext(),
            retention_repository=retention,
            live_state_store=live_state_store,
            live_state_repository=live_state_repository,
            redis_backend=redis_backend,
            now_ms_factory=lambda: now_ms,
            clock=lambda: now,
        )

        report = await loop.run_once()

        self.assertEqual(
            report.would_delete,
            {
                "api_request_log": 11,
                "api_payload_snapshot_legacy": 22,
                "event_live_state_history": 33,
            },
        )
        self.assertEqual(report.zombies_found, 2)
        self.assertEqual(report.zombies_cleared, 0)
        self.assertEqual(redis_backend.deleted_keys, [])
        self.assertEqual(live_state_repository.records, [])

    async def test_housekeeping_loop_clears_stale_live_events_and_records_terminal_state(self) -> None:
        from schema_inspector.services.housekeeping import HousekeepingConfig, HousekeepingLoop, ZOMBIE_TERMINAL_STATUS

        now = datetime(2026, 4, 20, 5, 0, tzinfo=timezone.utc)
        now_ms = int(now.timestamp() * 1000)
        live_state_repository = _FakeLiveStateRepository()
        redis_backend = _FakeRedisBackend(
            zsets={
                LIVE_HOT_ZSET: ("8001",),
                LIVE_WARM_ZSET: ("8001", "8002"),
                LIVE_COLD_ZSET: ("8001",),
            }
        )
        live_state_store = _FakeLiveStateStore(
            {
                8001: _live_state(event_id=8001, last_ingested_at=now_ms - 3 * 60 * 60 * 1000),
                8002: _live_state(event_id=8002, last_ingested_at=now_ms - 10 * 60 * 1000),
            }
        )
        loop = HousekeepingLoop(
            config=HousekeepingConfig(enabled=True, dry_run=False, zombie_max_age_minutes=120),
            connection_factory=lambda: _FakeConnectionContext(),
            retention_repository=_FakeRetentionRepository(),
            live_state_store=live_state_store,
            live_state_repository=live_state_repository,
            redis_backend=redis_backend,
            now_ms_factory=lambda: now_ms,
            clock=lambda: now,
        )

        report = await loop.run_once()

        self.assertEqual(report.zombies_found, 1)
        self.assertEqual(report.zombies_cleared, 1)
        self.assertNotIn("8001", redis_backend.zsets.get(LIVE_HOT_ZSET, ()))
        self.assertNotIn("8001", redis_backend.zsets.get(LIVE_WARM_ZSET, ()))
        self.assertNotIn("8001", redis_backend.zsets.get(LIVE_COLD_ZSET, ()))
        self.assertEqual(redis_backend.deleted_keys, ["live:event:8001"])
        self.assertEqual(len(live_state_repository.records), 1)
        record = live_state_repository.records[0]
        self.assertEqual(record.event_id, 8001)
        self.assertEqual(record.terminal_status, ZOMBIE_TERMINAL_STATUS)
        self.assertIsNone(record.final_snapshot_id)


class MaintenanceHousekeepingIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_maintenance_worker_runs_housekeeping_loop_and_requests_shutdown(self) -> None:
        from schema_inspector.workers.maintenance_worker import MaintenanceWorker

        housekeeping_loop = _FakeHousekeepingLoop()
        worker = MaintenanceWorker(
            handler=lambda entry: "handled",
            queue=_FakeQueue(),
            consumer="worker-maintenance-1",
            housekeeping_loop=housekeeping_loop,
        )

        async def fake_runtime_run_forever(*, install_signal_handlers: bool = True) -> None:
            del install_signal_handlers
            await housekeeping_loop.started.wait()

        async def fake_reclaim_loop() -> None:
            await asyncio.sleep(10)

        worker.runtime.run_forever = fake_runtime_run_forever  # type: ignore[method-assign]
        worker.run_reclaim_loop = fake_reclaim_loop  # type: ignore[method-assign]

        await worker.run_forever(install_signal_handlers=False)

        self.assertEqual(housekeeping_loop.run_calls, 1)
        self.assertEqual(housekeeping_loop.shutdown_calls, 1)

    async def test_service_app_builds_housekeeping_for_primary_maintenance_worker_only(self) -> None:
        from schema_inspector.services.service_app import ServiceApp

        app = _FakeServiceHybridApp()
        service_app = ServiceApp(app)

        worker = service_app.build_maintenance_worker(consumer_name="worker-maintenance-1")
        historical_worker = service_app.build_historical_maintenance_worker(
            consumer_name="worker-historical-maintenance-1"
        )

        self.assertIsNotNone(worker.housekeeping_loop)
        self.assertIsNone(historical_worker.housekeeping_loop)
        assert worker.housekeeping_loop is not None
        self.assertIs(worker.housekeeping_loop.live_state_store, app.live_state_store)
        self.assertIs(worker.housekeeping_loop.redis_backend, app.redis_backend)


class _FakeConnectionContext:
    async def __aenter__(self) -> "_FakeConnectionContext":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        return None


class _FakeRetentionRepository:
    def __init__(self, *, request_log_count: int = 0, snapshot_count: int = 0, live_history_count: int = 0) -> None:
        self.request_log_count = request_log_count
        self.snapshot_count = snapshot_count
        self.live_history_count = live_history_count

    async def count_expired_request_logs(self, executor, *, cutoff: datetime) -> int:
        del executor, cutoff
        return self.request_log_count

    async def count_expired_legacy_snapshots(self, executor, *, cutoff: datetime) -> int:
        del executor, cutoff
        return self.snapshot_count

    async def count_expired_live_state_history(self, executor, *, cutoff: datetime) -> int:
        del executor, cutoff
        return self.live_history_count

    async def delete_request_log_batch(self, executor, *, cutoff: datetime, batch_size: int) -> int:
        del executor, cutoff, batch_size
        return 0

    async def delete_legacy_snapshot_batch(self, executor, *, cutoff: datetime, batch_size: int) -> int:
        del executor, cutoff, batch_size
        return 0

    async def delete_live_state_history_batch(self, executor, *, cutoff: datetime, batch_size: int) -> int:
        del executor, cutoff, batch_size
        return 0


class _FakeLiveStateRepository:
    def __init__(self) -> None:
        self.records = []

    async def insert_terminal_state_if_missing(self, executor, record) -> None:
        del executor
        self.records.append(record)


class _FakeLiveStateStore:
    def __init__(self, states_by_event: dict[int, LiveEventState]) -> None:
        self.states_by_event = dict(states_by_event)

    def fetch(self, event_id: int) -> LiveEventState | None:
        return self.states_by_event.get(int(event_id))


class _FakeRedisBackend:
    def __init__(self, *, zsets: dict[str, tuple[str, ...]]) -> None:
        self.zsets = {key: list(values) for key, values in zsets.items()}
        self.deleted_keys: list[str] = []

    def zrange(self, key: str, start: int, end: int):
        del start, end
        return list(self.zsets.get(key, ()))

    def zrem(self, key: str, *members: str) -> int:
        bucket = self.zsets.setdefault(key, [])
        removed = 0
        for member in members:
            while member in bucket:
                bucket.remove(member)
                removed += 1
        return removed

    def delete(self, key: str) -> int:
        self.deleted_keys.append(key)
        return 1


class _FakeHousekeepingLoop:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.run_calls = 0
        self.shutdown_calls = 0

    async def run_forever(self) -> None:
        self.run_calls += 1
        self.started.set()
        while self.shutdown_calls == 0:
            await asyncio.sleep(0)

    def request_shutdown(self) -> None:
        self.shutdown_calls += 1


class _FakeQueue:
    def ensure_group(self, *args, **kwargs) -> None:
        del args, kwargs
        return None

    def read_group(self, *args, **kwargs):
        del args, kwargs
        return ()

    def ack(self, *args, **kwargs):
        del args, kwargs
        return 0


class _FakeDatabase:
    @asynccontextmanager
    async def connection(self):
        yield _FakeConnectionContext()


class _FakeRedisForServiceApp:
    def hset(self, key: str, mapping: dict[str, object]) -> int:
        del key
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        del key
        return {}

    def hdel(self, key: str, *members: str) -> int:
        del key, members
        return 0


class _FakeServiceHybridApp:
    def __init__(self) -> None:
        self.redis_backend = _FakeRedisForServiceApp()
        self.stream_queue = _FakeQueue()
        self.live_state_store = _FakeLiveStateStore({})
        self.database = _FakeDatabase()


def _live_state(*, event_id: int, last_ingested_at: int) -> LiveEventState:
    return LiveEventState(
        event_id=event_id,
        sport_slug="football",
        status_type="inprogress",
        poll_profile="hot",
        last_seen_at=last_ingested_at,
        last_ingested_at=last_ingested_at,
        last_changed_at=last_ingested_at,
        next_poll_at=last_ingested_at + 30_000,
        hot_until=last_ingested_at + 30_000,
        home_score=1,
        away_score=0,
        version_hint=None,
        is_finalized=False,
    )


if __name__ == "__main__":
    unittest.main()
