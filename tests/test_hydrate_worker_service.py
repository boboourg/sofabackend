from __future__ import annotations

import os
import unittest
from contextlib import contextmanager

from schema_inspector.queue.streams import STREAM_HISTORICAL_HYDRATE, STREAM_HYDRATE, StreamEntry


class HydrateWorkerServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_hydrate_worker_calls_orchestrator_with_decoded_job(self) -> None:
        from schema_inspector.workers.hydrate_worker import HydrateWorker

        orchestrator = _FakeOrchestrator()
        scheduler = _FakeDelayedScheduler()
        with _cleared_env(
            "SOFASCORE_WORKER_MAX_CONCURRENCY",
            "SOFASCORE_HYDRATE_WORKER_MAX_CONCURRENCY",
            "SOFASCORE_HISTORICAL_HYDRATE_WORKER_MAX_CONCURRENCY",
        ):
            worker = HydrateWorker(
                orchestrator=orchestrator,
                delayed_scheduler=scheduler,
                queue=_FakeQueue(),
                consumer="worker-hydrate-1",
            )

        result = await worker.handle(
            StreamEntry(
                stream=STREAM_HYDRATE,
                message_id="1-1",
                values={
                    "job_id": "job-1",
                    "job_type": "hydrate_event_root",
                    "sport_slug": "football",
                    "entity_type": "event",
                    "entity_id": "501",
                    "scope": "scheduled",
                    "params_json": '{"hydration_mode":"core"}',
                    "attempt": "2",
                },
            )
        )

        self.assertEqual(result, "completed")
        self.assertEqual(orchestrator.calls, [(501, "football", "core")])
        self.assertEqual(worker.runtime.stream, STREAM_HYDRATE)
        self.assertEqual(worker.runtime.group, "cg:hydrate")
        self.assertEqual(worker.runtime.consumer, "worker-hydrate-1")
        self.assertEqual(worker.runtime.max_concurrency, 2)
        self.assertEqual(scheduler.calls, [])

    async def test_hydrate_worker_schedules_retry_for_lock_errors(self) -> None:
        from schema_inspector.workers.hydrate_worker import HydrateWorker

        with _cleared_env(
            "SOFASCORE_WORKER_MAX_CONCURRENCY",
            "SOFASCORE_HYDRATE_WORKER_MAX_CONCURRENCY",
            "SOFASCORE_HISTORICAL_HYDRATE_WORKER_MAX_CONCURRENCY",
        ):
            worker = HydrateWorker(
                orchestrator=_FakeOrchestrator(),
                delayed_scheduler=_FakeDelayedScheduler(),
                queue=_FakeQueue(),
                consumer="worker-hydrate-1",
                now_ms_factory=lambda: 1_800_000_000_000,
            )
        entry = StreamEntry(
            stream=STREAM_HYDRATE,
            message_id="1-9",
            values={
                "job_id": "job-9",
                "job_type": "hydrate_event_root",
                "sport_slug": "football",
                "entity_type": "event",
                "entity_id": "909",
                "params_json": "{}",
                "attempt": "3",
            },
        )

        await worker.retry_later(entry, RuntimeError("deadlock detected"), delay_ms=20_000)

        self.assertEqual(worker.delayed_scheduler.calls, [("job-9", 1_800_000_020_000)])

    async def test_hydrate_worker_honours_worker_specific_concurrency_env(self) -> None:
        from schema_inspector.workers.hydrate_worker import HydrateWorker

        previous_specific = os.environ.get("SOFASCORE_HISTORICAL_HYDRATE_WORKER_MAX_CONCURRENCY")
        previous_global = os.environ.get("SOFASCORE_WORKER_MAX_CONCURRENCY")
        os.environ["SOFASCORE_HISTORICAL_HYDRATE_WORKER_MAX_CONCURRENCY"] = "4"
        os.environ["SOFASCORE_WORKER_MAX_CONCURRENCY"] = "9"
        try:
            worker = HydrateWorker(
                orchestrator=_FakeOrchestrator(),
                delayed_scheduler=_FakeDelayedScheduler(),
                queue=_FakeQueue(),
                consumer="worker-historical-hydrate-1",
                stream=STREAM_HISTORICAL_HYDRATE,
                group="cg:historical_hydrate",
            )
            self.assertEqual(worker.runtime.max_concurrency, 4)
        finally:
            if previous_specific is None:
                os.environ.pop("SOFASCORE_HISTORICAL_HYDRATE_WORKER_MAX_CONCURRENCY", None)
            else:
                os.environ["SOFASCORE_HISTORICAL_HYDRATE_WORKER_MAX_CONCURRENCY"] = previous_specific
            if previous_global is None:
                os.environ.pop("SOFASCORE_WORKER_MAX_CONCURRENCY", None)
            else:
                os.environ["SOFASCORE_WORKER_MAX_CONCURRENCY"] = previous_global

    async def test_hydrate_worker_falls_back_to_global_concurrency_env(self) -> None:
        from schema_inspector.workers.hydrate_worker import HydrateWorker

        previous_global = os.environ.get("SOFASCORE_WORKER_MAX_CONCURRENCY")
        previous_specific = os.environ.get("SOFASCORE_HYDRATE_WORKER_MAX_CONCURRENCY")
        os.environ["SOFASCORE_WORKER_MAX_CONCURRENCY"] = "5"
        os.environ.pop("SOFASCORE_HYDRATE_WORKER_MAX_CONCURRENCY", None)
        try:
            worker = HydrateWorker(
                orchestrator=_FakeOrchestrator(),
                delayed_scheduler=_FakeDelayedScheduler(),
                queue=_FakeQueue(),
                consumer="worker-hydrate-1",
            )
            self.assertEqual(worker.runtime.max_concurrency, 5)
        finally:
            if previous_global is None:
                os.environ.pop("SOFASCORE_WORKER_MAX_CONCURRENCY", None)
            else:
                os.environ["SOFASCORE_WORKER_MAX_CONCURRENCY"] = previous_global
            if previous_specific is None:
                os.environ.pop("SOFASCORE_HYDRATE_WORKER_MAX_CONCURRENCY", None)
            else:
                os.environ["SOFASCORE_HYDRATE_WORKER_MAX_CONCURRENCY"] = previous_specific


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


if __name__ == "__main__":
    unittest.main()
