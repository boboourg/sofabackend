"""Jittered concurrency gate for terminal live final sweeps."""

from __future__ import annotations

import asyncio
import inspect
import os
import random
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


# Env-tunable so we can adjust without redeploy. Defaults reduced from
# (max_concurrency=3, jitter=0..180s) → (max_concurrency=8, jitter=0..30s).
# Rotating GB-billed proxy lets us bump concurrency; jitter primarily exists
# to spread thundering-herd on match-day finalisations.
_DEFAULT_MAX_CONCURRENCY = _env_int("SOFASCORE_FINAL_SWEEP_MAX_CONCURRENCY", 8)
_DEFAULT_JITTER_MAX_SECONDS = _env_float("SOFASCORE_FINAL_SWEEP_JITTER_MAX_SECONDS", 30.0)


class FinalSweepGate:
    def __init__(
        self,
        *,
        max_concurrency: int = _DEFAULT_MAX_CONCURRENCY,
        jitter_seconds_factory: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None] | None] = asyncio.sleep,
        jitter_max_seconds: float = _DEFAULT_JITTER_MAX_SECONDS,
    ) -> None:
        self._semaphore = asyncio.Semaphore(max(1, int(max_concurrency)))
        upper = max(0.0, float(jitter_max_seconds))
        self._jitter_seconds_factory = jitter_seconds_factory or (lambda: random.uniform(0, upper))
        self._sleep = sleep

    async def run(self, func: Callable[[], Awaitable[T]]) -> T:
        sleep_result = self._sleep(float(self._jitter_seconds_factory()))
        if inspect.isawaitable(sleep_result):
            await sleep_result
        async with self._semaphore:
            return await func()
