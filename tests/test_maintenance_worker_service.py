from __future__ import annotations

import unittest

from schema_inspector.queue.streams import STREAM_DISCOVERY, STREAM_MAINTENANCE, StreamEntry


class MaintenanceWorkerServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_maintenance_worker_uses_runtime_and_calls_handler(self) -> None:
        from schema_inspector.workers.maintenance_worker import MaintenanceWorker

        seen: list[str] = []

        async def handler(entry: StreamEntry) -> str:
            seen.append(entry.message_id)
            return "handled"

        worker = MaintenanceWorker(
            handler=handler,
            queue=_FakeQueue(),
            consumer="worker-maint-1",
        )

        result = await worker.handle(StreamEntry(stream=STREAM_MAINTENANCE, message_id="5-1", values={"job_id": "job-maint-1"}))

        self.assertEqual(result, "handled")
        self.assertEqual(seen, ["5-1"])
        self.assertEqual(worker.runtime.stream, STREAM_MAINTENANCE)
        self.assertEqual(worker.runtime.group, "cg:maintenance")

    def test_maintenance_worker_reclaims_discovery_stream_by_default(self) -> None:
        from schema_inspector.workers.maintenance_worker import MaintenanceWorker

        worker = MaintenanceWorker(
            handler=lambda entry: "handled",
            queue=_FakeQueue(),
            consumer="worker-maint-1",
        )

        self.assertIn((STREAM_DISCOVERY, "cg:discovery"), tuple((item.stream, item.group) for item in worker.reclaim_targets))


class _FakeQueue:
    def read_group(self, *args, **kwargs):
        del args, kwargs
        return ()

    def ack(self, *args, **kwargs):
        del args, kwargs
        return 0


if __name__ == "__main__":
    unittest.main()
