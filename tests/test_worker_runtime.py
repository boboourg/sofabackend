from __future__ import annotations

import asyncio
import unittest

from schema_inspector.queue.streams import STREAM_HYDRATE, StreamEntry


class WorkerRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_runtime_acks_success_and_stops_after_shutdown_request(self) -> None:
        from schema_inspector.services.worker_runtime import WorkerRuntime

        queue = _FakeQueue(
            entries=(
                StreamEntry(stream=STREAM_HYDRATE, message_id="1-1", values={"attempt": "1"}),
            )
        )
        seen: list[str] = []
        runtime: WorkerRuntime | None = None

        async def handler(entry: StreamEntry) -> str:
            seen.append(entry.message_id)
            assert runtime is not None
            runtime.request_shutdown()
            return "ok"

        runtime = WorkerRuntime(
            name="hydrate",
            queue=queue,
            stream=STREAM_HYDRATE,
            group="cg:hydrate",
            consumer="worker-a",
            handler=handler,
            block_ms=0,
        )

        await runtime.run_forever(install_signal_handlers=False)

        self.assertEqual(seen, ["1-1"])
        self.assertEqual(queue.acked, [(STREAM_HYDRATE, "cg:hydrate", ("1-1",))])
        self.assertTrue(runtime.shutdown_requested)
        self.assertFalse(runtime.accepting_new_work)

    async def test_worker_runtime_requeues_retryable_db_errors_via_callback(self) -> None:
        from schema_inspector.services.worker_runtime import WorkerRuntime

        queue = _FakeQueue(
            entries=(
                StreamEntry(stream=STREAM_HYDRATE, message_id="1-2", values={"attempt": "2"}),
            )
        )
        retries: list[tuple[str, int]] = []
        runtime: WorkerRuntime | None = None

        async def handler(entry: StreamEntry) -> str:
            del entry
            raise RuntimeError("canceling statement due to lock timeout")

        async def on_retry(entry: StreamEntry, exc: Exception, *, delay_ms: int) -> None:
            del exc
            retries.append((entry.message_id, delay_ms))
            assert runtime is not None
            runtime.request_shutdown()

        runtime = WorkerRuntime(
            name="hydrate",
            queue=queue,
            stream=STREAM_HYDRATE,
            group="cg:hydrate",
            consumer="worker-a",
            handler=handler,
            retry_handler=on_retry,
            block_ms=0,
        )

        await runtime.run_forever(install_signal_handlers=False)

        self.assertEqual(retries, [("1-2", 10_000)])
        self.assertEqual(queue.acked, [(STREAM_HYDRATE, "cg:hydrate", ("1-2",))])


class _FakeQueue:
    def __init__(self, *, entries: tuple[StreamEntry, ...]) -> None:
        self._entries = list(entries)
        self.acked: list[tuple[str, str, tuple[str, ...]]] = []
        self.read_calls = 0

    def read_group(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        count: int = 1,
        block_ms: int | None = None,
    ) -> tuple[StreamEntry, ...]:
        del stream, group, consumer, count, block_ms
        self.read_calls += 1
        if not self._entries:
            return ()
        entries = tuple(self._entries)
        self._entries.clear()
        return entries

    def ack(self, stream: str, group: str, message_ids: tuple[str, ...]) -> int:
        self.acked.append((stream, group, message_ids))
        return len(message_ids)


if __name__ == "__main__":
    unittest.main()
