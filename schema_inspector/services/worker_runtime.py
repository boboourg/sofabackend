"""Shared runtime loop for continuous Redis Streams workers."""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import signal
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any

from ..queue.streams import StreamEntry
from ..workers._stream_jobs import decode_stream_payload
from .job_execution_context import (
    JobExecutionContext,
    push_job_execution_context,
    reset_job_execution_context,
)
from .retry_policy import is_retryable_db_error, retry_audit_status, retry_delay_ms

StreamHandler = Callable[[StreamEntry], object]
RetryHandler = Callable[[StreamEntry, Exception], object]
logger = logging.getLogger(__name__)

_ENV_MAX_CONCURRENCY = "SOFASCORE_WORKER_MAX_CONCURRENCY"
_ENV_COMPLETION_TTL_MS = "SOFASCORE_WORKER_COMPLETION_TTL_MS"
_DEFAULT_COMPLETION_TTL_MS = 3_600_000  # 1 hour; was 24h — see Fix #3 (Apr 2026).


def _env_positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning("Invalid %s=%r; falling back to %d", name, raw, default)
        return default
    if value < 1:
        logger.warning("%s must be >= 1 (got %d); falling back to %d", name, value, default)
        return default
    return value


class WorkerRuntime:
    """Runs a single consumer loop with graceful shutdown and delayed requeue hooks."""

    def __init__(
        self,
        *,
        name: str,
        queue: Any,
        stream: str,
        group: str,
        consumer: str,
        handler: StreamHandler,
        retry_handler: Callable[..., object] | None = None,
        completion_store=None,
        completion_ttl_ms: int | None = None,
        now_ms_factory=None,
        block_ms: int = 5_000,
        idle_sleep_s: float = 0.05,
        job_audit_logger=None,
        max_concurrency: int | None = None,
    ) -> None:
        self.name = name
        self.queue = queue
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.handler = handler
        self.retry_handler = retry_handler
        self.completion_store = completion_store
        # Fix #3: completion TTL defaults to 1 hour (was 24h) and can be
        # overridden via SOFASCORE_WORKER_COMPLETION_TTL_MS. 1h is more than
        # enough to dedupe a reclaimed PENDING entry — autoclaim kicks in
        # within minutes, not hours — and keeps Redis memory tidy.
        if completion_ttl_ms is None:
            completion_ttl_ms = _env_positive_int(_ENV_COMPLETION_TTL_MS, _DEFAULT_COMPLETION_TTL_MS)
        self.completion_ttl_ms = int(completion_ttl_ms)
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))
        self.block_ms = block_ms
        self.idle_sleep_s = idle_sleep_s
        self.job_audit_logger = job_audit_logger
        # Fix #2: parallel dispatch. Default=1 preserves the old strictly-serial
        # behaviour; tmux sessions can opt into more concurrency by exporting
        # SOFASCORE_WORKER_MAX_CONCURRENCY=N before launching the worker.
        if max_concurrency is None:
            max_concurrency = _env_positive_int(_ENV_MAX_CONCURRENCY, 1)
        self.max_concurrency = int(max_concurrency)
        self.shutdown_requested = False
        self.accepting_new_work = True
        self._signal_handlers_installed = False
        self._inflight: set[asyncio.Task[Any]] = set()
        self._fatal_exc: BaseException | None = None

    def request_shutdown(self) -> None:
        self.shutdown_requested = True
        self.accepting_new_work = False

    def install_signal_handlers(self, *, loop: asyncio.AbstractEventLoop | None = None) -> None:
        if self._signal_handlers_installed:
            return
        resolved_loop = loop or asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            try:
                resolved_loop.add_signal_handler(signum, self.request_shutdown)
            except (AttributeError, NotImplementedError, RuntimeError, ValueError):
                signal.signal(signum, self._fallback_signal_handler)
        self._signal_handlers_installed = True

    async def run_forever(self, *, install_signal_handlers: bool = True) -> None:
        if install_signal_handlers:
            self.install_signal_handlers()
        try:
            while self.accepting_new_work:
                # 1) Back-pressure: block until at least one slot is free.
                #    With max_concurrency=1 this degrades into "wait for the
                #    single inflight task", matching the previous await-task
                #    behaviour but without blocking the whole loop body.
                await self._await_free_slot()
                if not self.accepting_new_work:
                    break

                slots = self.max_concurrency - len(self._inflight)
                if slots <= 0:
                    continue

                # 2) Read as many entries as we can dispatch in parallel.
                entries = self.queue.read_group(
                    self.stream,
                    self.group,
                    self.consumer,
                    count=slots,
                    block_ms=self.block_ms,
                )
                if not entries:
                    await asyncio.sleep(self.idle_sleep_s)
                    continue

                # 3) Fire-and-track: never `await task` here — that would
                #    serialise dispatch. Fatal task errors are captured via
                #    _on_task_done and re-raised after drain().
                for entry in entries:
                    if not self.accepting_new_work:
                        break
                    task = asyncio.create_task(self._handle_entry(entry))
                    self._track_task(task)
        finally:
            await self.drain()
            if self._fatal_exc is not None:
                raise self._fatal_exc

    async def _await_free_slot(self) -> None:
        while self.accepting_new_work and len(self._inflight) >= self.max_concurrency:
            pending = tuple(self._inflight)
            if not pending:
                return
            done, _ = await asyncio.wait(
                pending,
                timeout=self.idle_sleep_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
            # `done` callbacks run asynchronously; trim eagerly so the outer
            # loop doesn't spin waiting for discard-callbacks to fire.
            if done:
                self._inflight.difference_update(done)

    async def drain(self, *, timeout_s: float | None = None) -> None:
        if not self._inflight:
            return
        tasks = tuple(self._inflight)
        done, pending = await asyncio.wait(tasks, timeout=timeout_s)
        self._inflight.difference_update(done)
        self._inflight.difference_update(pending)

    def _track_task(self, task: asyncio.Task[Any]) -> None:
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        # Preserve crash-on-unknown-error semantics from the original
        # `await task` loop: if a task surfaces a non-retryable exception
        # (retryable ones are consumed inside _handle_entry), capture the
        # first one, stop accepting new work, and let run_forever re-raise
        # after draining the other inflight tasks.
        if task.cancelled():
            return
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        except asyncio.InvalidStateError:
            return
        if exc is None:
            return
        if self._fatal_exc is None:
            self._fatal_exc = exc
        self.accepting_new_work = False

    async def _handle_entry(self, entry: StreamEntry) -> object:
        started_at = _utc_now_iso()
        started_perf = time.perf_counter()
        job_context = _build_job_execution_context(entry=entry, worker_id=self.consumer)
        context_token = push_job_execution_context(job_context)
        try:
            try:
                outcome = await _await_maybe(self.handler(entry))
            except Exception as exc:
                if self.retry_handler is not None and is_retryable_db_error(exc):
                    delay_ms = retry_delay_ms(attempt=_entry_attempt(entry), exc=exc)
                    retry_status = retry_audit_status(exc)
                    await _await_maybe(self.retry_handler(entry, exc, delay_ms=delay_ms))
                    await self._record_job_run(
                        entry,
                        status=retry_status,
                        started_at=started_at,
                        duration_ms=_duration_ms(started_perf),
                        error=exc,
                        retry_delay_ms=delay_ms,
                        job_context=job_context,
                    )
                    self.queue.ack(entry.stream, self.group, (entry.message_id,))
                    return "requeued"
                await self._record_job_run(
                    entry,
                    status="failed",
                    started_at=started_at,
                    duration_ms=_duration_ms(started_perf),
                    error=exc,
                    job_context=job_context,
                )
                raise
            await self._record_job_run(
                entry,
                status="succeeded",
                started_at=started_at,
                duration_ms=_duration_ms(started_perf),
                job_context=job_context,
            )
            self.mark_entry_completed(entry)
            self.queue.ack(entry.stream, self.group, (entry.message_id,))
            return outcome
        except Exception as exc:
            raise
        finally:
            reset_job_execution_context(context_token)

    async def handle_reclaimed_entry(self, entry: StreamEntry) -> object:
        if self.is_entry_completed(entry):
            return "skipped_completed"
        return await self._handle_entry(entry)

    def _fallback_signal_handler(self, signum: int, frame) -> None:
        del signum, frame
        self.request_shutdown()

    def mark_entry_completed(self, entry: StreamEntry) -> None:
        if self.completion_store is None:
            return
        self.completion_store.mark_fresh(
            _completion_key(entry),
            ttl_ms=self.completion_ttl_ms,
            now_ms=int(self.now_ms_factory()),
        )

    def is_entry_completed(self, entry: StreamEntry) -> bool:
        if self.completion_store is None:
            return False
        return bool(
            self.completion_store.is_fresh(
                _completion_key(entry),
                now_ms=int(self.now_ms_factory()),
            )
        )

    async def _record_job_run(
        self,
        entry: StreamEntry,
        *,
        status: str,
        started_at: str,
        duration_ms: int,
        job_context: JobExecutionContext,
        error: Exception | None = None,
        retry_delay_ms: int | None = None,
    ) -> None:
        if self.job_audit_logger is None:
            return
        retry_scheduled_for = None
        if retry_delay_ms is not None:
            retry_at = datetime.now(timezone.utc) + timedelta(milliseconds=int(retry_delay_ms))
            retry_scheduled_for = retry_at.isoformat()
        try:
            await _await_maybe(
                self.job_audit_logger.record_run(
                    values=entry.values,
                    fallback_job_id=entry.message_id,
                    job_run_id=job_context.job_run_id,
                    worker_id=self.consumer,
                    status=status,
                    started_at=started_at,
                    finished_at=_utc_now_iso(),
                    duration_ms=duration_ms,
                    error=error,
                    retry_scheduled_for=retry_scheduled_for,
                )
            )
        except Exception as exc:  # pragma: no cover - best-effort guard
            logger.warning("Job audit logging failed for worker=%s status=%s: %s", self.consumer, status, exc)


async def _await_maybe(value: object) -> object:
    if inspect.isawaitable(value):
        return await value
    return value


def _entry_attempt(entry: StreamEntry) -> int:
    raw = entry.values.get("attempt")
    try:
        return max(1, int(raw)) if raw is not None else 1
    except (TypeError, ValueError):
        return 1


def _completion_key(entry: StreamEntry) -> str:
    job_id = entry.values.get("job_id") or entry.message_id
    return f"completed:job:{job_id}"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _duration_ms(started_perf: float) -> int:
    return max(0, int((time.perf_counter() - float(started_perf)) * 1000))


def _build_job_execution_context(*, entry: StreamEntry, worker_id: str) -> JobExecutionContext:
    job = decode_stream_payload(entry.values, fallback_job_id=entry.message_id)
    return JobExecutionContext(
        job_run_id=str(uuid.uuid4()),
        job_id=job.job_id,
        job_type=job.job_type,
        trace_id=job.trace_id,
        worker_id=worker_id,
    )
