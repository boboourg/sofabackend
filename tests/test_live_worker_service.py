from __future__ import annotations

import os
import unittest
from contextlib import contextmanager

from schema_inspector.queue.streams import STREAM_LIVE_HOT, STREAM_LIVE_WARM, StreamEntry


class LiveWorkerServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_worker_hot_runs_full_hydration_from_runtime_stream(self) -> None:
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        orchestrator = _FakeOrchestrator()
        with _cleared_env(
            "SOFASCORE_WORKER_MAX_CONCURRENCY",
            "SOFASCORE_LIVE_HOT_WORKER_MAX_CONCURRENCY",
            "SOFASCORE_LIVE_WARM_WORKER_MAX_CONCURRENCY",
        ):
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
        self.assertEqual(worker.runtime.prefetch_count, 25)
        self.assertIsNotNone(worker.runtime.batch_preprocessor)

    async def test_live_worker_auto_mode_limits_delta_to_canary_sports(self) -> None:
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        orchestrator = _FakeOrchestrator()
        with _patched_env(
            SOFASCORE_LIVE_HYDRATION_MODE="auto",
            SOFASCORE_LIVE_HYDRATION_CANARY_SPORTS="football",
            CANARY_SPORTS=None,
        ):
            worker = LiveWorkerService(
                orchestrator=orchestrator,
                delayed_scheduler=_FakeDelayedScheduler(),
                queue=_FakeQueue(),
                lane="hot",
                consumer="worker-live-hot-1",
            )
            await worker.handle(
                StreamEntry(
                    stream=STREAM_LIVE_HOT,
                    message_id="2-football",
                    values={
                        "job_id": "job-live-football",
                        "job_type": "refresh_live_event",
                        "sport_slug": "football",
                        "event_id": "7101",
                        "lane": "hot",
                        "params_json": '{"hydration_mode":"live_delta"}',
                    },
                )
            )
            await worker.handle(
                StreamEntry(
                    stream=STREAM_LIVE_HOT,
                    message_id="2-basketball",
                    values={
                        "job_id": "job-live-basketball",
                        "job_type": "refresh_live_event",
                        "sport_slug": "basketball",
                        "event_id": "7102",
                        "lane": "hot",
                        "params_json": '{"hydration_mode":"live_delta"}',
                    },
                )
            )

        self.assertEqual(
            orchestrator.calls,
            [(7101, "football", "live_delta"), (7102, "basketball", "full")],
        )

    async def test_live_worker_warm_uses_independent_runtime(self) -> None:
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        with _cleared_env(
            "SOFASCORE_WORKER_MAX_CONCURRENCY",
            "SOFASCORE_LIVE_HOT_WORKER_MAX_CONCURRENCY",
            "SOFASCORE_LIVE_WARM_WORKER_MAX_CONCURRENCY",
        ):
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
        self.assertIsNone(worker.runtime.batch_preprocessor)

    async def test_live_worker_hot_coalesces_stale_refreshes_by_event_id(self) -> None:
        from schema_inspector.services.worker_runtime import BatchDispatchPlan
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        with _cleared_env(
            "SOFASCORE_WORKER_MAX_CONCURRENCY",
            "SOFASCORE_LIVE_HOT_WORKER_MAX_CONCURRENCY",
            "SOFASCORE_LIVE_WARM_WORKER_MAX_CONCURRENCY",
        ):
            worker = LiveWorkerService(
                orchestrator=_FakeOrchestrator(),
                delayed_scheduler=_FakeDelayedScheduler(),
                queue=_FakeQueue(),
                lane="hot",
                consumer="worker-live-hot-1",
            )

        preprocessor = worker.runtime.batch_preprocessor
        self.assertIsNotNone(preprocessor)

        plan = preprocessor(
            (
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
                ),
                StreamEntry(
                    stream=STREAM_LIVE_HOT,
                    message_id="2-2",
                    values={
                        "job_id": "job-live-2",
                        "job_type": "refresh_live_event",
                        "sport_slug": "football",
                        "event_id": "7002",
                        "lane": "hot",
                        "attempt": "1",
                    },
                ),
                StreamEntry(
                    stream=STREAM_LIVE_HOT,
                    message_id="2-3",
                    values={
                        "job_id": "job-live-3",
                        "job_type": "refresh_live_event",
                        "sport_slug": "football",
                        "event_id": "7001",
                        "lane": "hot",
                        "attempt": "1",
                    },
                ),
            )
        )

        self.assertIsInstance(plan, BatchDispatchPlan)
        self.assertEqual(
            [entry.message_id for entry in plan.entries_to_process],
            ["2-2", "2-3"],
        )
        self.assertEqual(
            [entry.message_id for entry in plan.stale_entries_to_ack],
            ["2-1"],
        )
        self.assertEqual(plan.coalesced_counts, (("7001", 1),))

    async def test_live_worker_schedules_retry_for_lock_errors(self) -> None:
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        with _cleared_env(
            "SOFASCORE_WORKER_MAX_CONCURRENCY",
            "SOFASCORE_LIVE_HOT_WORKER_MAX_CONCURRENCY",
            "SOFASCORE_LIVE_WARM_WORKER_MAX_CONCURRENCY",
        ):
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

    async def test_live_worker_warm_honours_lane_specific_concurrency_env(self) -> None:
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        previous_specific = os.environ.get("SOFASCORE_LIVE_WARM_WORKER_MAX_CONCURRENCY")
        previous_global = os.environ.get("SOFASCORE_WORKER_MAX_CONCURRENCY")
        os.environ["SOFASCORE_LIVE_WARM_WORKER_MAX_CONCURRENCY"] = "4"
        os.environ["SOFASCORE_WORKER_MAX_CONCURRENCY"] = "9"
        try:
            worker = LiveWorkerService(
                orchestrator=_FakeOrchestrator(),
                delayed_scheduler=_FakeDelayedScheduler(),
                queue=_FakeQueue(),
                lane="warm",
                consumer="worker-live-warm-1",
            )
            self.assertEqual(worker.runtime.max_concurrency, 4)
        finally:
            if previous_specific is None:
                os.environ.pop("SOFASCORE_LIVE_WARM_WORKER_MAX_CONCURRENCY", None)
            else:
                os.environ["SOFASCORE_LIVE_WARM_WORKER_MAX_CONCURRENCY"] = previous_specific
            if previous_global is None:
                os.environ.pop("SOFASCORE_WORKER_MAX_CONCURRENCY", None)
            else:
                os.environ["SOFASCORE_WORKER_MAX_CONCURRENCY"] = previous_global

    async def test_live_worker_hot_falls_back_to_global_concurrency_env(self) -> None:
        from schema_inspector.workers.live_worker_service import LiveWorkerService

        previous_specific = os.environ.get("SOFASCORE_LIVE_HOT_WORKER_MAX_CONCURRENCY")
        previous_global = os.environ.get("SOFASCORE_WORKER_MAX_CONCURRENCY")
        os.environ.pop("SOFASCORE_LIVE_HOT_WORKER_MAX_CONCURRENCY", None)
        os.environ["SOFASCORE_WORKER_MAX_CONCURRENCY"] = "3"
        try:
            worker = LiveWorkerService(
                orchestrator=_FakeOrchestrator(),
                delayed_scheduler=_FakeDelayedScheduler(),
                queue=_FakeQueue(),
                lane="hot",
                consumer="worker-live-hot-1",
            )
            self.assertEqual(worker.runtime.max_concurrency, 3)
        finally:
            if previous_specific is None:
                os.environ.pop("SOFASCORE_LIVE_HOT_WORKER_MAX_CONCURRENCY", None)
            else:
                os.environ["SOFASCORE_LIVE_HOT_WORKER_MAX_CONCURRENCY"] = previous_specific
            if previous_global is None:
                os.environ.pop("SOFASCORE_WORKER_MAX_CONCURRENCY", None)
            else:
                os.environ["SOFASCORE_WORKER_MAX_CONCURRENCY"] = previous_global


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


@contextmanager
def _cleared_env(*names: str):
    previous = {name: os.environ.get(name) for name in names}
    try:
        for name in names:
            os.environ.pop(name, None)
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


@contextmanager
def _patched_env(**values: str | None):
    previous = {name: os.environ.get(name) for name in values}
    try:
        for name, value in values.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        yield
    finally:
        for name, value in previous.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value


if __name__ == "__main__":
    unittest.main()
