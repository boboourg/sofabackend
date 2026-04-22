from __future__ import annotations

import unittest

from schema_inspector.queue.streams import STREAM_LIVE_HOT, STREAM_LIVE_WARM, StreamEntry


class LiveWorkerServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_worker_hot_runs_full_hydration_from_runtime_stream(self) -> None:
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        orchestrator = _FakeOrchestrator()
        worker = LiveWorkerService(
            orchestrator=orchestrator,
            delayed_scheduler=_FakeDelayedScheduler(),
            queue=_FakeQueue(),
            lane="hot",
            consumer="worker-live-hot-1",
        )

        result = await worker.handle(
            StreamEntry(
                stream=STREAM_LIVE_HOT,
                message_id="2-1",
                values={
                    "job_id": "job-live-1",
                    "job_type": "refresh_live_event",
                    "sport_slug": "football",
                    "event_id": "7001",
                    "lane": "hot",
                    "attempt": "1",
                },
            )
        )

        self.assertEqual(result, "completed")
        self.assertEqual(orchestrator.calls, [(7001, "football", "full")])
        self.assertEqual(worker.runtime.stream, STREAM_LIVE_HOT)
        self.assertEqual(worker.runtime.group, "cg:live_hot")
        self.assertEqual(worker.runtime.max_concurrency, 1)

    async def test_live_worker_warm_uses_independent_runtime(self) -> None:
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        worker = LiveWorkerService(
            orchestrator=_FakeOrchestrator(),
            delayed_scheduler=_FakeDelayedScheduler(),
            queue=_FakeQueue(),
            lane="warm",
            consumer="worker-live-warm-1",
        )

        self.assertEqual(worker.runtime.stream, STREAM_LIVE_WARM)
        self.assertEqual(worker.runtime.group, "cg:live_warm")
        self.assertEqual(worker.runtime.consumer, "worker-live-warm-1")
        self.assertEqual(worker.runtime.max_concurrency, 2)

    async def test_live_worker_schedules_retry_for_lock_errors(self) -> None:
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        worker = LiveWorkerService(
            orchestrator=_FakeOrchestrator(),
            delayed_scheduler=_FakeDelayedScheduler(),
            queue=_FakeQueue(),
            lane="hot",
            consumer="worker-live-hot-1",
            now_ms_factory=lambda: 1_800_000_000_000,
        )

        await worker.retry_later(
            StreamEntry(
                stream=STREAM_LIVE_HOT,
                message_id="2-9",
                values={"job_id": "job-live-9", "event_id": "7009", "lane": "hot"},
            ),
            RuntimeError("canceling statement due to lock timeout"),
            delay_ms=5_000,
        )

        self.assertEqual(worker.delayed_scheduler.calls, [("job-live-9", 1_800_000_005_000)])


class _FakeOrchestrator:
    def __init__(self) -> None:
        self.calls: list[tuple[int, str | None, str]] = []

    async def run_event(self, *, event_id: int, sport_slug: str | None, hydration_mode: str = "full"):
        self.calls.append((event_id, sport_slug, hydration_mode))
        return {"event_id": event_id}


class _FakeDelayedScheduler:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def schedule(self, job_id: str, *, run_at_epoch_ms: int) -> None:
        self.calls.append((job_id, run_at_epoch_ms))


class _FakeQueue:
    def read_group(self, *args, **kwargs):
        del args, kwargs
        return ()

    def ack(self, *args, **kwargs):
        del args, kwargs
        return 0


if __name__ == "__main__":
    unittest.main()
