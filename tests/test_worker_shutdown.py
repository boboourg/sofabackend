from __future__ import annotations

import asyncio
import signal
import unittest

from schema_inspector.queue.streams import STREAM_HYDRATE, StreamEntry


class WorkerShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_runtime_drains_inflight_task_before_exiting(self) -> None:
        from schema_inspector.services.worker_runtime import WorkerRuntime

        queue = _FakeQueue(
            entries=(
                StreamEntry(stream=STREAM_HYDRATE, message_id="1-9", values={"attempt": "1"}),
            )
        )
        started = asyncio.Event()
        release = asyncio.Event()
        runtime: WorkerRuntime | None = None

        async def handler(entry: StreamEntry) -> str:
            del entry
            started.set()
            await release.wait()
            return "done"

        runtime = WorkerRuntime(
            name="hydrate",
            queue=queue,
            stream=STREAM_HYDRATE,
            group="cg:hydrate",
            consumer="worker-a",
            handler=handler,
            block_ms=0,
        )

        task = asyncio.create_task(runtime.run_forever(install_signal_handlers=False))
        await started.wait()
        runtime.request_shutdown()
        self.assertFalse(task.done())
        release.set()
        await asyncio.wait_for(task, timeout=1.0)

        self.assertEqual(queue.acked, [(STREAM_HYDRATE, "cg:hydrate", ("1-9",))])

    def test_worker_runtime_installs_sigterm_and_sigint_handlers(self) -> None:
        from schema_inspector.services.worker_runtime import WorkerRuntime

        runtime = WorkerRuntime(
            name="hydrate",
            queue=_FakeQueue(entries=()),
            stream=STREAM_HYDRATE,
            group="cg:hydrate",
            consumer="worker-a",
            handler=lambda entry: entry,
        )
        loop = _FakeLoop()

        runtime.install_signal_handlers(loop=loop)

        self.assertEqual(set(loop.handlers), {signal.SIGTERM, signal.SIGINT})
        loop.handlers[signal.SIGTERM]()
        self.assertTrue(runtime.shutdown_requested)
        self.assertFalse(runtime.accepting_new_work)


class _FakeQueue:
    def __init__(self, *, entries: tuple[StreamEntry, ...]) -> None:
        self._entries = list(entries)
        self.acked: list[tuple[str, str, tuple[str, ...]]] = []

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
        if not self._entries:
            return ()
        entries = tuple(self._entries)
        self._entries.clear()
        return entries

    def ack(self, stream: str, group: str, message_ids: tuple[str, ...]) -> int:
        self.acked.append((stream, group, message_ids))
        return len(message_ids)


class _FakeLoop:
    def __init__(self) -> None:
        self.handlers: dict[signal.Signals, object] = {}

    def add_signal_handler(self, sig: signal.Signals, callback) -> None:
        self.handlers[sig] = callback


if __name__ == "__main__":
    unittest.main()
