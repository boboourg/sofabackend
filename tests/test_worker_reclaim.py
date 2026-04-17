from __future__ import annotations

import unittest

from schema_inspector.queue.dedupe import DedupeStore
from schema_inspector.queue.streams import STREAM_DLQ, STREAM_HYDRATE, StreamEntry


class WorkerReclaimTests(unittest.IsolatedAsyncioTestCase):
    async def test_maintenance_worker_requeues_claimed_stale_message(self) -> None:
        from schema_inspector.workers.maintenance_worker import MaintenanceWorker, ReclaimTarget

        queue = _FakeReclaimQueue()
        queue.seed_pending(
            STREAM_HYDRATE,
            "cg:hydrate",
            StreamEntry(stream=STREAM_HYDRATE, message_id="1-1", values={"job_id": "job-1", "entity_id": "501"}),
            consumer="worker-dead",
            idle_ms=45_000,
            delivery_count=1,
        )
        worker = MaintenanceWorker(
            handler=lambda entry: "handled",
            queue=queue,
            consumer="maint-1",
            delayed_scheduler=None,
            completion_store=DedupeStore(_FakeDedupeBackend()),
            reclaim_targets=(ReclaimTarget(stream=STREAM_HYDRATE, group="cg:hydrate"),),
            reclaim_min_idle_ms=30_000,
            max_delivery_count=5,
            reclaim_consumer="maint-1",
        )

        report = await worker.reclaim_once()

        self.assertEqual(report.reclaimed, 1)
        self.assertEqual(report.requeued, 1)
        self.assertEqual(report.dlq, 0)
        self.assertEqual(report.skipped_completed, 0)
        self.assertEqual(queue.published, [(STREAM_HYDRATE, {"job_id": "job-1", "entity_id": "501"})])
        self.assertEqual(queue.acked, [(STREAM_HYDRATE, "cg:hydrate", ("1-1",))])

    async def test_maintenance_worker_sends_hopeless_message_to_dlq(self) -> None:
        from schema_inspector.workers.maintenance_worker import MaintenanceWorker, ReclaimTarget

        queue = _FakeReclaimQueue()
        queue.seed_pending(
            STREAM_HYDRATE,
            "cg:hydrate",
            StreamEntry(stream=STREAM_HYDRATE, message_id="1-2", values={"job_id": "job-2", "entity_id": "502"}),
            consumer="worker-dead",
            idle_ms=45_000,
            delivery_count=7,
        )
        worker = MaintenanceWorker(
            handler=lambda entry: "handled",
            queue=queue,
            consumer="maint-1",
            delayed_scheduler=None,
            completion_store=DedupeStore(_FakeDedupeBackend()),
            reclaim_targets=(ReclaimTarget(stream=STREAM_HYDRATE, group="cg:hydrate"),),
            reclaim_min_idle_ms=30_000,
            max_delivery_count=5,
            reclaim_consumer="maint-1",
        )

        report = await worker.reclaim_once()

        self.assertEqual(report.dlq, 1)
        self.assertEqual(queue.published[0][0], STREAM_DLQ)
        self.assertEqual(queue.published[0][1]["source_stream"], STREAM_HYDRATE)
        self.assertEqual(queue.published[0][1]["job_id"], "job-2")

    async def test_maintenance_worker_skips_completed_job_without_requeue(self) -> None:
        from schema_inspector.workers.maintenance_worker import MaintenanceWorker, ReclaimTarget

        dedupe_backend = _FakeDedupeBackend()
        completion_store = DedupeStore(dedupe_backend)
        completion_store.mark_fresh("completed:job:job-3", ttl_ms=60_000, now_ms=0)

        queue = _FakeReclaimQueue()
        queue.seed_pending(
            STREAM_HYDRATE,
            "cg:hydrate",
            StreamEntry(stream=STREAM_HYDRATE, message_id="1-3", values={"job_id": "job-3", "entity_id": "503"}),
            consumer="worker-dead",
            idle_ms=45_000,
            delivery_count=2,
        )
        worker = MaintenanceWorker(
            handler=lambda entry: "handled",
            queue=queue,
            consumer="maint-1",
            delayed_scheduler=None,
            completion_store=completion_store,
            reclaim_targets=(ReclaimTarget(stream=STREAM_HYDRATE, group="cg:hydrate"),),
            reclaim_min_idle_ms=30_000,
            max_delivery_count=5,
            reclaim_consumer="maint-1",
            now_ms_factory=lambda: 1_000,
        )

        report = await worker.reclaim_once()

        self.assertEqual(report.skipped_completed, 1)
        self.assertEqual(queue.published, [])
        self.assertEqual(queue.acked, [(STREAM_HYDRATE, "cg:hydrate", ("1-3",))])


class _FakeReclaimQueue:
    def __init__(self) -> None:
        self.pending_by_group: dict[tuple[str, str], dict[str, dict[str, object]]] = {}
        self.entries_by_group: dict[tuple[str, str], dict[str, StreamEntry]] = {}
        self.published: list[tuple[str, dict[str, str]]] = []
        self.acked: list[tuple[str, str, tuple[str, ...]]] = []

    def seed_pending(
        self,
        stream: str,
        group: str,
        entry: StreamEntry,
        *,
        consumer: str,
        idle_ms: int,
        delivery_count: int,
    ) -> None:
        self.entries_by_group.setdefault((stream, group), {})[entry.message_id] = entry
        self.pending_by_group.setdefault((stream, group), {})[entry.message_id] = {
            "consumer": consumer,
            "idle_ms": idle_ms,
            "delivery_count": delivery_count,
        }

    def claim_stale(self, stream: str, group: str, consumer: str, *, min_idle_ms: int, start_id: str = "0-0", count: int = 100):
        del start_id
        claimed = []
        pending = self.pending_by_group.setdefault((stream, group), {})
        entries = self.entries_by_group.setdefault((stream, group), {})
        for message_id in sorted(pending):
            meta = pending[message_id]
            if int(meta["idle_ms"]) < min_idle_ms:
                continue
            meta["consumer"] = consumer
            meta["idle_ms"] = 0
            meta["delivery_count"] = int(meta["delivery_count"]) + 1
            claimed.append(entries[message_id])
            if len(claimed) >= count:
                break
        return tuple(claimed)

    def pending_entries(self, stream: str, group: str, *, start_id: str = "-", end_id: str = "+", count: int = 100, consumer: str | None = None):
        del start_id, end_id
        rows = []
        pending = self.pending_by_group.setdefault((stream, group), {})
        for message_id in sorted(pending):
            meta = pending[message_id]
            if consumer is not None and meta["consumer"] != consumer:
                continue
            rows.append(
                type(
                    "PendingEntry",
                    (),
                    {
                        "message_id": message_id,
                        "consumer": meta["consumer"],
                        "idle_ms": meta["idle_ms"],
                        "delivery_count": meta["delivery_count"],
                    },
                )()
            )
            if len(rows) >= count:
                break
        return tuple(rows)

    def publish(self, stream: str, values: dict[str, object]) -> str:
        payload = {str(key): str(value) for key, value in values.items()}
        self.published.append((stream, payload))
        return f"{stream}:{len(self.published)}"

    def ack(self, stream: str, group: str, message_ids: tuple[str, ...]) -> int:
        self.acked.append((stream, group, message_ids))
        pending = self.pending_by_group.setdefault((stream, group), {})
        entries = self.entries_by_group.setdefault((stream, group), {})
        acknowledged = 0
        for message_id in message_ids:
            if message_id in pending:
                acknowledged += 1
                del pending[message_id]
                entries.pop(message_id, None)
        return acknowledged

    def read_group(self, *args, **kwargs):
        del args, kwargs
        return ()


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
