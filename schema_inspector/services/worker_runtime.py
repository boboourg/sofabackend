"""Shared runtime loop for continuous Redis Streams workers."""

from __future__ import annotations

import asyncio
import inspect
import signal
from collections.abc import Awaitable, Callable
from typing import Any

from ..queue.streams import StreamEntry
from .retry_policy import is_retryable_db_error, retry_delay_ms

StreamHandler = Callable[[StreamEntry], object]
RetryHandler = Callable[[StreamEntry, Exception], object]


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
        block_ms: int = 5_000,
        idle_sleep_s: float = 0.05,
    ) -> None:
        self.name = name
        self.queue = queue
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.handler = handler
        self.retry_handler = retry_handler
        self.block_ms = block_ms
        self.idle_sleep_s = idle_sleep_s
        self.shutdown_requested = False
        self.accepting_new_work = True
        self._signal_handlers_installed = False
        self._inflight: set[asyncio.Task[Any]] = set()

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
                entries = self.queue.read_group(
                    self.stream,
                    self.group,
                    self.consumer,
                    count=1,
                    block_ms=self.block_ms,
                )
                if not entries:
                    await asyncio.sleep(self.idle_sleep_s)
                    continue
                for entry in entries:
                    if not self.accepting_new_work:
                        break
                    task = asyncio.create_task(self._handle_entry(entry))
                    self._track_task(task)
                    await task
        finally:
            await self.drain()

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

    async def _handle_entry(self, entry: StreamEntry) -> object:
        try:
            outcome = await _await_maybe(self.handler(entry))
        except Exception as exc:
            if self.retry_handler is not None and is_retryable_db_error(exc):
                delay_ms = retry_delay_ms(attempt=_entry_attempt(entry))
                await _await_maybe(self.retry_handler(entry, exc, delay_ms=delay_ms))
                self.queue.ack(entry.stream, self.group, (entry.message_id,))
                return "requeued"
            raise
        self.queue.ack(entry.stream, self.group, (entry.message_id,))
        return outcome

    def _fallback_signal_handler(self, signum: int, frame) -> None:
        del signum, frame
        self.request_shutdown()


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
