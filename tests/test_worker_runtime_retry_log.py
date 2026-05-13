"""Tests for the structured retry log enhancement in worker_runtime.

The 2026-05-13 firebreak audit revealed live-discovery's "scheduling retry"
WARNING messages were carrying ``exc=`` (empty string) for TimeoutError()
instances with no args, which made it impossible to distinguish a libcurl
timeout from a PostgreSQL deadlock without enabling DEBUG-level logging.

This test file pins the new log contract:
    exc_type=<class name>
    exc=<repr capped at 240 chars>
    sqlstate=<asyncpg SQLSTATE or None>
    duration_ms=<time spent in handler>
"""

from __future__ import annotations

import unittest

from schema_inspector.queue.streams import STREAM_HYDRATE, StreamEntry
from schema_inspector.services.worker_runtime import WorkerRuntime


class _FakeQueue:
    def __init__(self, *, entries):
        self._entries = list(entries)
        self.acked = []
        self.read_counts = []

    def read_group(self, stream, group, consumer, *, count=1, block_ms=None):
        del stream, group, consumer, block_ms
        self.read_counts.append(count)
        if not self._entries:
            return ()
        taken = self._entries[: max(1, int(count))]
        self._entries = self._entries[max(1, int(count)) :]
        return tuple(taken)

    def ack(self, stream, group, message_ids):
        self.acked.append((stream, group, message_ids))
        return len(message_ids)


class _FakeAuditLogger:
    def __init__(self):
        self.calls = []

    async def record_run(self, **payload):
        self.calls.append(dict(payload))


class _FakeDeadlock(Exception):
    """Mimics asyncpg.exceptions.DeadlockDetectedError shape (sqlstate=40P01)."""

    sqlstate = "40P01"


async def _run_worker_with_failing_handler(*, handler_exc):
    queue = _FakeQueue(
        entries=(
            StreamEntry(stream=STREAM_HYDRATE, message_id="retry-1", values={"event_id": "1"}),
        )
    )
    audit_logger = _FakeAuditLogger()
    retry_recorded = []
    runtime_ref: dict = {}

    async def handler(entry):
        # Stop runtime after raising once so the loop exits cleanly.
        runtime_ref["runtime"].request_shutdown()
        raise handler_exc

    async def retry_handler(entry, exc, *, delay_ms):
        retry_recorded.append((entry.message_id, type(exc).__name__, delay_ms))

    runtime = WorkerRuntime(
        name="hydrate-test",
        queue=queue,
        stream=STREAM_HYDRATE,
        group="cg:hydrate",
        consumer="hydrate-test-1",
        handler=handler,
        block_ms=0,
        max_concurrency=1,
        prefetch_count=1,
        retry_handler=retry_handler,
        job_audit_logger=audit_logger,
    )
    runtime_ref["runtime"] = runtime
    return runtime, retry_recorded


class RetryLogStructuredFieldsTests(unittest.IsolatedAsyncioTestCase):
    async def test_retry_log_emits_exc_type_when_exc_str_empty(self) -> None:
        """``TimeoutError()`` has no args -> ``str(exc)`` is empty. The new
        log must still emit ``exc_type=TimeoutError`` so journal parsers can
        classify the failure without DEBUG traceback."""

        runtime, recorded = await _run_worker_with_failing_handler(
            handler_exc=TimeoutError()
        )
        with self.assertLogs(
            "schema_inspector.services.worker_runtime", level="WARNING"
        ) as captured:
            await runtime.run_forever(install_signal_handlers=False)

        retry_lines = [
            line for line in captured.output if "scheduling retry" in line
        ]
        self.assertEqual(len(retry_lines), 1)
        line = retry_lines[0]
        self.assertIn("exc_type=TimeoutError", line)
        self.assertIn("exc=TimeoutError()", line)
        self.assertIn("sqlstate=None", line)
        self.assertIn("duration_ms=", line)
        # Sanity: retry handler was invoked
        self.assertEqual(len(recorded), 1)
        self.assertEqual(recorded[0][1], "TimeoutError")

    async def test_retry_log_emits_sqlstate_for_deadlock_class(self) -> None:
        """asyncpg DeadlockDetectedError carries ``.sqlstate='40P01'`` —
        the new log must surface it so DB deadlocks are distinguishable
        from libcurl timeouts at a glance."""

        runtime, recorded = await _run_worker_with_failing_handler(
            handler_exc=_FakeDeadlock("deadlock detected on tuple (...)")
        )
        with self.assertLogs(
            "schema_inspector.services.worker_runtime", level="WARNING"
        ) as captured:
            await runtime.run_forever(install_signal_handlers=False)

        retry_lines = [
            line for line in captured.output if "scheduling retry" in line
        ]
        self.assertEqual(len(retry_lines), 1)
        line = retry_lines[0]
        self.assertIn("exc_type=_FakeDeadlock", line)
        self.assertIn("sqlstate=40P01", line)
        self.assertIn("duration_ms=", line)
        # Non-empty retry payload
        self.assertEqual(recorded[0][1], "_FakeDeadlock")

    async def test_retry_log_includes_duration_ms_field(self) -> None:
        """Even a fast failure must populate ``duration_ms=<int>``; the
        previous log line did not emit timings at all."""

        runtime, _ = await _run_worker_with_failing_handler(
            handler_exc=TimeoutError("operation timed out after 20001 ms")
        )
        with self.assertLogs(
            "schema_inspector.services.worker_runtime", level="WARNING"
        ) as captured:
            await runtime.run_forever(install_signal_handlers=False)

        retry_lines = [
            line for line in captured.output if "scheduling retry" in line
        ]
        line = retry_lines[0]
        # Extract "duration_ms=X" and check it parses as int >= 0
        match_segment = line.split("duration_ms=")[-1].strip()
        # The duration may be followed by other text; take the first token.
        token = match_segment.split()[0]
        duration = int(token)
        self.assertGreaterEqual(duration, 0)


if __name__ == "__main__":
    unittest.main()
