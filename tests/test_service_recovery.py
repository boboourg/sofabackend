from __future__ import annotations

import unittest

from schema_inspector.queue.streams import STREAM_HYDRATE, StreamEntry


class ServiceRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_service_app_restores_live_state_before_planner_and_live_workers(self) -> None:
        from schema_inspector.services.service_app import ServiceApp

        fake_app = _FakeServiceHybridApp()
        service_app = ServiceApp(fake_app)
        planner = _FakeRunner()
        live_worker = _FakeRunner()
        service_app.build_planner_daemon = lambda **kwargs: planner
        service_app.build_live_worker = lambda **kwargs: live_worker

        await service_app.run_planner_daemon(sport_slugs=("football",), scheduled_interval_seconds=60.0, loop_interval_seconds=2.0)
        await service_app.run_live_worker(lane="hot", consumer_name="live-hot-1", block_ms=1500)

        self.assertEqual(fake_app.recover_calls, 2)
        self.assertEqual(planner.run_calls, 1)
        self.assertEqual(live_worker.run_calls, 1)

    async def test_runtime_skips_reclaimed_completed_job_without_duplicate_handler_call(self) -> None:
        from schema_inspector.queue.dedupe import DedupeStore
        from schema_inspector.services.worker_runtime import WorkerRuntime

        backend = _FakeDedupeBackend()
        dedupe = DedupeStore(backend)
        dedupe.mark_fresh("completed:job:job-77", ttl_ms=60_000, now_ms=0)
        handled: list[str] = []
        queue = _FakeQueue()
        runtime = WorkerRuntime(
            name="hydrate",
            queue=queue,
            stream=STREAM_HYDRATE,
            group="cg:hydrate",
            consumer="worker-a",
            handler=lambda entry: handled.append(entry.message_id),
            completion_store=dedupe,
            now_ms_factory=lambda: 1_000,
        )

        outcome = await runtime.handle_reclaimed_entry(
            StreamEntry(
                stream=STREAM_HYDRATE,
                message_id="1-77",
                values={"job_id": "job-77", "entity_id": "777"},
            )
        )

        self.assertEqual(outcome, "skipped_completed")
        self.assertEqual(handled, [])


class _FakeRunner:
    def __init__(self) -> None:
        self.run_calls = 0

    async def run_forever(self) -> None:
        self.run_calls += 1


class _FakeServiceHybridApp:
    def __init__(self) -> None:
        self.redis_backend = _FakeRedisBackend()
        self.stream_queue = None
        self.live_state_store = object()
        self.recover_calls = 0

    async def recover_live_state(self) -> None:
        self.recover_calls += 1
        return None


class _FakeRedisBackend:
    def __init__(self) -> None:
        self.hashes = {}
        self.sorted_sets = {}
        self.groups = []

    def hset(self, key: str, mapping: dict[str, object]) -> int:
        self.hashes.setdefault(key, {}).update(dict(mapping))
        return len(mapping)

    def hgetall(self, key: str) -> dict[str, object]:
        return dict(self.hashes.get(key, {}))

    def hdel(self, key: str, *members: str) -> int:
        bucket = self.hashes.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                removed += 1
                del bucket[member]
        return removed

    def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.sorted_sets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[str(member)] = float(score)
        return len(mapping)

    def zrangebyscore(self, key: str, _min: float, _max: float, *, start: int = 0, num: int | None = None, withscores: bool = False):
        values = list(sorted(self.sorted_sets.get(key, {}).items()))
        sliced = values[start : start + num if num is not None else None]
        if withscores:
            return sliced
        return [member for member, _ in sliced]

    def zrem(self, key: str, *members: str) -> int:
        bucket = self.sorted_sets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                removed += 1
                del bucket[member]
        return removed

    def xgroup_create(self, stream: str, group: str, *, id: str = "0-0", mkstream: bool = False) -> bool:
        del id, mkstream
        self.groups.append((stream, group))
        return True

    def xadd(self, stream: str, payload: dict[str, object]) -> str:
        del stream, payload
        return "1-1"


class _FakeQueue:
    def read_group(self, *args, **kwargs):
        del args, kwargs
        return ()

    def ack(self, *args, **kwargs):
        del args, kwargs
        return 0


class _FakeDedupeBackend:
    def __init__(self) -> None:
        self.values: dict[str, int | None] = {}

    def set(self, key: str, value: str, *, nx: bool = False, px: int | None = None, now_ms: int = 0) -> bool:
        del value
        current_expire = self.values.get(key)
        if current_expire is not None and current_expire <= now_ms:
            del self.values[key]
            current_expire = None
        if nx and key in self.values:
            return False
        self.values[key] = None if px is None else now_ms + px
        return True

    def exists(self, key: str, *, now_ms: int = 0) -> bool:
        current_expire = self.values.get(key)
        if current_expire is None:
            return key in self.values
        if current_expire <= now_ms:
            del self.values[key]
            return False
        return True


if __name__ == "__main__":
    unittest.main()
