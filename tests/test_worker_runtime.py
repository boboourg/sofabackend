from __future__ import annotations

import asyncio
import unittest

from curl_cffi.requests import RequestsError

from schema_inspector.queue.streams import STREAM_HYDRATE, StreamEntry


class WorkerRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_worker_runtime_prefetches_and_acks_stale_entries_from_batch_preprocessor(self) -> None:
        from schema_inspector.queue.streams import STREAM_LIVE_HOT
        from schema_inspector.services.worker_runtime import BatchDispatchPlan, WorkerRuntime

        queue = _FakeQueue(
            entries=(
                StreamEntry(stream=STREAM_LIVE_HOT, message_id="hot-1", values={"event_id": "7001"}),
                StreamEntry(stream=STREAM_LIVE_HOT, message_id="hot-2", values={"event_id": "7002"}),
                StreamEntry(stream=STREAM_LIVE_HOT, message_id="hot-3", values={"event_id": "7001"}),
            )
        )
        seen: list[str] = []
        runtime: WorkerRuntime | None = None

        def preprocess(entries: tuple[StreamEntry, ...]) -> BatchDispatchPlan:
            return BatchDispatchPlan(
                entries_to_process=(entries[1], entries[2]),
                stale_entries_to_ack=(entries[0],),
                coalesced_counts=(("7001", 1),),
            )

        async def handler(entry: StreamEntry) -> str:
            seen.append(entry.message_id)
            assert runtime is not None
            if len(seen) >= 2:
                runtime.request_shutdown()
            return "ok"

        runtime = WorkerRuntime(
            name="live-hot",
            queue=queue,
            stream=STREAM_LIVE_HOT,
            group="cg:live_hot",
            consumer="live-hot-1",
            handler=handler,
            block_ms=0,
            max_concurrency=1,
            prefetch_count=3,
            batch_preprocessor=preprocess,
        )

        with self.assertLogs("schema_inspector.services.worker_runtime", level="INFO") as captured:
            await runtime.run_forever(install_signal_handlers=False)

        self.assertEqual(seen, ["hot-2", "hot-3"])
        self.assertEqual(queue.read_counts, [3])
        self.assertIn((STREAM_LIVE_HOT, "cg:live_hot", ("hot-1",)), queue.acked)
        self.assertTrue(
            any("Coalesced 1 stale messages for entity 7001" in line for line in captured.output)
        )

    async def test_worker_runtime_exposes_execution_context_and_reuses_job_run_id_for_audit(self) -> None:
        from schema_inspector.services.job_execution_context import current_job_execution_context
        from schema_inspector.services.worker_runtime import WorkerRuntime

        queue = _FakeQueue(
            entries=(
                StreamEntry(
                    stream=STREAM_HYDRATE,
                    message_id="ctx-1",
                    values={
                        "job_id": "job-ctx",
                        "job_type": "enrich_tournament_archive",
                        "trace_id": "trace-ctx",
                        "attempt": "1",
                    },
                ),
            )
        )
        audit = _FakeAuditLogger()
        seen_context = None
        runtime: WorkerRuntime | None = None

        async def handler(entry: StreamEntry) -> str:
            del entry
            nonlocal seen_context
            seen_context = current_job_execution_context()
            self.assertIsNotNone(seen_context)
            self.assertEqual(seen_context.job_id, "job-ctx")
            self.assertEqual(seen_context.job_type, "enrich_tournament_archive")
            self.assertEqual(seen_context.trace_id, "trace-ctx")
            self.assertEqual(seen_context.worker_id, "worker-a")
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
            job_audit_logger=audit,
            block_ms=0,
        )

        await runtime.run_forever(install_signal_handlers=False)

        self.assertIsNotNone(seen_context)
        self.assertEqual(len(audit.calls), 1)
        self.assertEqual(audit.calls[0]["status"], "succeeded")
        self.assertEqual(audit.calls[0]["job_run_id"], seen_context.job_run_id)

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

    async def test_worker_runtime_records_successful_job_runs(self) -> None:
        from schema_inspector.services.worker_runtime import WorkerRuntime

        queue = _FakeQueue(entries=(StreamEntry(stream=STREAM_HYDRATE, message_id="1-3", values={"job_id": "job-3"}),))
        audit = _FakeAuditLogger()
        runtime: WorkerRuntime | None = None

        async def handler(entry: StreamEntry) -> str:
            del entry
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
            job_audit_logger=audit,
            block_ms=0,
        )

        await runtime.run_forever(install_signal_handlers=False)

        self.assertEqual(len(audit.calls), 1)
        self.assertEqual(audit.calls[0]["status"], "succeeded")
        self.assertEqual(audit.calls[0]["worker_id"], "worker-a")

    async def test_worker_runtime_records_retries(self) -> None:
        from schema_inspector.services.worker_runtime import WorkerRuntime

        queue = _FakeQueue(entries=(StreamEntry(stream=STREAM_HYDRATE, message_id="1-4", values={"job_id": "job-4"}),))
        audit = _FakeAuditLogger()
        runtime: WorkerRuntime | None = None

        async def handler(entry: StreamEntry) -> str:
            del entry
            raise RuntimeError("deadlock detected")

        async def on_retry(entry: StreamEntry, exc: Exception, *, delay_ms: int) -> None:
            del entry, exc, delay_ms
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
            job_audit_logger=audit,
            block_ms=0,
        )

        await runtime.run_forever(install_signal_handlers=False)

        self.assertEqual(len(audit.calls), 1)
        self.assertEqual(audit.calls[0]["status"], "retry_scheduled")
        self.assertIsNotNone(audit.calls[0]["retry_scheduled_for"])

    async def test_worker_runtime_requeues_timeout_errors_via_callback(self) -> None:
        from schema_inspector.services.worker_runtime import WorkerRuntime

        queue = _FakeQueue(entries=(StreamEntry(stream=STREAM_HYDRATE, message_id="1-5", values={"attempt": "1"}),))
        retries: list[tuple[str, int]] = []
        runtime: WorkerRuntime | None = None

        async def handler(entry: StreamEntry) -> str:
            del entry
            raise TimeoutError()

        async def on_retry(entry: StreamEntry, exc: Exception, *, delay_ms: int) -> None:
            self.assertIsInstance(exc, TimeoutError)
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

        self.assertEqual(retries, [("1-5", 5_000)])
        self.assertEqual(queue.acked, [(STREAM_HYDRATE, "cg:hydrate", ("1-5",))])

    async def test_worker_runtime_requeues_upstream_access_denied_errors_via_callback(self) -> None:
        from schema_inspector.services.worker_runtime import WorkerRuntime
        from schema_inspector.sofascore_client import SofascoreAccessDeniedError

        queue = _FakeQueue(entries=(StreamEntry(stream=STREAM_HYDRATE, message_id="1-5b", values={"attempt": "1"}),))
        retries: list[tuple[str, int]] = []
        runtime: WorkerRuntime | None = None

        async def handler(entry: StreamEntry) -> str:
            del entry
            raise SofascoreAccessDeniedError("Access denied by upstream")

        async def on_retry(entry: StreamEntry, exc: Exception, *, delay_ms: int) -> None:
            self.assertIsInstance(exc, SofascoreAccessDeniedError)
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

        self.assertEqual(retries, [("1-5b", 30_000)])
        self.assertEqual(queue.acked, [(STREAM_HYDRATE, "cg:hydrate", ("1-5b",))])

    async def test_worker_runtime_requeues_transport_requests_errors_via_callback(self) -> None:
        from schema_inspector.services.worker_runtime import WorkerRuntime

        queue = _FakeQueue(entries=(StreamEntry(stream=STREAM_HYDRATE, message_id="1-5c", values={"attempt": "2"}),))
        retries: list[tuple[str, int]] = []
        runtime: WorkerRuntime | None = None

        async def handler(entry: StreamEntry) -> str:
            del entry
            raise RequestsError("SSL_ERROR_SYSCALL", 35, None)

        async def on_retry(entry: StreamEntry, exc: Exception, *, delay_ms: int) -> None:
            self.assertIsInstance(exc, RequestsError)
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

        self.assertEqual(retries, [("1-5c", 20_000)])
        self.assertEqual(queue.acked, [(STREAM_HYDRATE, "cg:hydrate", ("1-5c",))])

    async def test_worker_runtime_uses_custom_retry_delay_for_admission_deferrals(self) -> None:
        from schema_inspector.services.retry_policy import AdmissionDeferredError
        from schema_inspector.services.worker_runtime import WorkerRuntime

        queue = _FakeQueue(entries=(StreamEntry(stream=STREAM_HYDRATE, message_id="1-6", values={"attempt": "1"}),))
        retries: list[tuple[str, int]] = []
        runtime: WorkerRuntime | None = None

        async def handler(entry: StreamEntry) -> str:
            del entry
            raise AdmissionDeferredError("hydrate backlog", delay_ms=30_000)

        async def on_retry(entry: StreamEntry, exc: Exception, *, delay_ms: int) -> None:
            self.assertIsInstance(exc, AdmissionDeferredError)
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

        self.assertEqual(retries, [("1-6", 30_000)])
        self.assertEqual(queue.acked, [(STREAM_HYDRATE, "cg:hydrate", ("1-6",))])

    async def test_worker_runtime_records_backpressure_deferrals_separately_from_retries(self) -> None:
        from schema_inspector.services.retry_policy import AdmissionDeferredError
        from schema_inspector.services.worker_runtime import WorkerRuntime

        queue = _FakeQueue(entries=(StreamEntry(stream=STREAM_HYDRATE, message_id="1-6b", values={"job_id": "job-6b"}),))
        audit = _FakeAuditLogger()
        runtime: WorkerRuntime | None = None

        async def handler(entry: StreamEntry) -> str:
            del entry
            raise AdmissionDeferredError("hydrate backlog", delay_ms=30_000)

        async def on_retry(entry: StreamEntry, exc: Exception, *, delay_ms: int) -> None:
            del entry, exc, delay_ms
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
            job_audit_logger=audit,
            block_ms=0,
        )

        await runtime.run_forever(install_signal_handlers=False)

        self.assertEqual(len(audit.calls), 1)
        self.assertEqual(audit.calls[0]["status"], "deferred_backpressure")
        self.assertIsNotNone(audit.calls[0]["retry_scheduled_for"])

    async def test_worker_runtime_max_concurrency_defaults_to_serial(self) -> None:
        # Fix #2: default behaviour must stay strictly serial so that
        # untouched deployments keep identical semantics to the old
        # `await task` loop.
        from schema_inspector.services.worker_runtime import WorkerRuntime

        entries = tuple(
            StreamEntry(stream=STREAM_HYDRATE, message_id=f"1-{index}", values={"attempt": "1"})
            for index in range(4)
        )
        queue = _FakeQueue(entries=entries)

        overlap = 0
        max_overlap = 0
        order: list[str] = []
        runtime: WorkerRuntime | None = None

        async def handler(entry: StreamEntry) -> str:
            nonlocal overlap, max_overlap
            overlap += 1
            max_overlap = max(max_overlap, overlap)
            order.append(entry.message_id)
            await asyncio.sleep(0.01)
            overlap -= 1
            assert runtime is not None
            if entry.message_id == "1-3":
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

        self.assertEqual(max_overlap, 1)
        self.assertEqual(order, ["1-0", "1-1", "1-2", "1-3"])
        self.assertEqual(runtime.max_concurrency, 1)

    async def test_worker_runtime_max_concurrency_dispatches_in_parallel(self) -> None:
        # Fix #2: with max_concurrency=4 the runtime must execute up to four
        # handlers concurrently rather than strictly one at a time.
        from schema_inspector.services.worker_runtime import WorkerRuntime

        entries = tuple(
            StreamEntry(stream=STREAM_HYDRATE, message_id=f"p-{index}", values={"attempt": "1"})
            for index in range(4)
        )
        queue = _FakeQueue(entries=entries)

        overlap = 0
        max_overlap = 0
        completed: list[str] = []
        all_started = asyncio.Event()
        started = 0
        runtime: WorkerRuntime | None = None

        async def handler(entry: StreamEntry) -> str:
            nonlocal overlap, max_overlap, started
            overlap += 1
            max_overlap = max(max_overlap, overlap)
            started += 1
            if started >= 4:
                all_started.set()
            # Hold each task until all siblings have started so we can prove
            # they were dispatched concurrently.
            await all_started.wait()
            overlap -= 1
            completed.append(entry.message_id)
            assert runtime is not None
            if len(completed) >= 4:
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
            max_concurrency=4,
        )

        await runtime.run_forever(install_signal_handlers=False)

        self.assertEqual(max_overlap, 4)
        self.assertEqual(sorted(completed), ["p-0", "p-1", "p-2", "p-3"])
        acked_ids = {msg_id for _, _, ids in queue.acked for msg_id in ids}
        self.assertEqual(acked_ids, {"p-0", "p-1", "p-2", "p-3"})

    async def test_worker_runtime_honours_max_concurrency_env_var(self) -> None:
        # Fix #2: max_concurrency is configurable via environment so tmux
        # sessions can ramp individual workers without touching code.
        import os

        from schema_inspector.services.worker_runtime import WorkerRuntime

        previous = os.environ.get("SOFASCORE_WORKER_MAX_CONCURRENCY")
        os.environ["SOFASCORE_WORKER_MAX_CONCURRENCY"] = "6"
        try:
            runtime = WorkerRuntime(
                name="hydrate",
                queue=_FakeQueue(entries=()),
                stream=STREAM_HYDRATE,
                group="cg:hydrate",
                consumer="worker-a",
                handler=lambda entry: "ok",
            )
            self.assertEqual(runtime.max_concurrency, 6)
        finally:
            if previous is None:
                os.environ.pop("SOFASCORE_WORKER_MAX_CONCURRENCY", None)
            else:
                os.environ["SOFASCORE_WORKER_MAX_CONCURRENCY"] = previous

    async def test_worker_runtime_completion_ttl_defaults_to_one_hour(self) -> None:
        # Fix #3: shrink completion-key TTL from 24h to 1h so Redis memory
        # stays bounded (71k keys observed in prod after 24h).
        from schema_inspector.services.worker_runtime import WorkerRuntime

        runtime = WorkerRuntime(
            name="hydrate",
            queue=_FakeQueue(entries=()),
            stream=STREAM_HYDRATE,
            group="cg:hydrate",
            consumer="worker-a",
            handler=lambda entry: "ok",
        )
        self.assertEqual(runtime.completion_ttl_ms, 3_600_000)

    async def test_worker_runtime_completion_ttl_env_override(self) -> None:
        import os

        from schema_inspector.services.worker_runtime import WorkerRuntime

        previous = os.environ.get("SOFASCORE_WORKER_COMPLETION_TTL_MS")
        os.environ["SOFASCORE_WORKER_COMPLETION_TTL_MS"] = "900000"
        try:
            runtime = WorkerRuntime(
                name="hydrate",
                queue=_FakeQueue(entries=()),
                stream=STREAM_HYDRATE,
                group="cg:hydrate",
                consumer="worker-a",
                handler=lambda entry: "ok",
            )
            self.assertEqual(runtime.completion_ttl_ms, 900_000)
        finally:
            if previous is None:
                os.environ.pop("SOFASCORE_WORKER_COMPLETION_TTL_MS", None)
            else:
                os.environ["SOFASCORE_WORKER_COMPLETION_TTL_MS"] = previous

    async def test_worker_runtime_reraises_non_retryable_handler_errors(self) -> None:
        # Regression guard: the new fire-and-forget dispatch loop must still
        # bubble up unknown handler errors so tmux crash-recovery works.
        from schema_inspector.services.worker_runtime import WorkerRuntime

        queue = _FakeQueue(
            entries=(StreamEntry(stream=STREAM_HYDRATE, message_id="boom-1", values={"attempt": "1"}),)
        )

        async def handler(entry: StreamEntry) -> str:
            del entry
            raise ValueError("unknown handler failure")

        runtime = WorkerRuntime(
            name="hydrate",
            queue=queue,
            stream=STREAM_HYDRATE,
            group="cg:hydrate",
            consumer="worker-a",
            handler=handler,
            block_ms=0,
        )

        with self.assertRaises(ValueError) as caught:
            await runtime.run_forever(install_signal_handlers=False)
        self.assertIn("unknown handler failure", str(caught.exception))


class _FakeQueue:
    def __init__(self, *, entries: tuple[StreamEntry, ...]) -> None:
        self._entries = list(entries)
        self.acked: list[tuple[str, str, tuple[str, ...]]] = []
        self.read_calls = 0
        self.read_counts: list[int] = []

    def read_group(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        count: int = 1,
        block_ms: int | None = None,
    ) -> tuple[StreamEntry, ...]:
        del stream, group, consumer, block_ms
        self.read_calls += 1
        self.read_counts.append(count)
        if not self._entries:
            return ()
        take = max(1, int(count))
        taken = self._entries[:take]
        self._entries = self._entries[take:]
        return tuple(taken)

    def ack(self, stream: str, group: str, message_ids: tuple[str, ...]) -> int:
        self.acked.append((stream, group, message_ids))
        return len(message_ids)


class _FakeAuditLogger:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def record_run(self, **payload) -> None:
        self.calls.append(dict(payload))


if __name__ == "__main__":
    unittest.main()
