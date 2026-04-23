"""Jittered concurrency gate for terminal live final sweeps."""

from __future__ import annotations

import asyncio
import inspect
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


class FinalSweepGate:
    def __init__(
        self,
        *,
        max_concurrency: int = 3,
        jitter_seconds_factory: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None] | None] = asyncio.sleep,
    ) -> None:
        self._semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))
        self._jitter_seconds_factory = jitter_seconds_factory or (lambda: random.uniform(0, 180))
        self._sleep = sleep

    async def run(self, func: Callable[[], Awaitable[T]]) -> T:
        sleep_result = self._sleep(float(self._jitter_seconds_factory()))
        if inspect.isawaitable(sleep_result):
            await sleep_result
        async with self._semaphore:
            return await func()
