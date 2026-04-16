"""Delayed scheduling helpers backed by a Redis sorted set."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

DELAYED_JOBS_KEY = "zset:etl:delayed"


@dataclass(frozen=True)
class DelayedJob:
    job_id: str
    run_at_epoch_ms: int


class DelayedJobScheduler:
    """Tracks due jobs in a sorted set keyed by next run time."""

    def __init__(self, backend: Any, *, key: str = DELAYED_JOBS_KEY) -> None:
        self.backend = backend
        self.key = key

    def schedule(self, job_id: str, *, run_at_epoch_ms: int) -> None:
        self.backend.zadd(self.key, {job_id: float(run_at_epoch_ms)})

    def pop_due(self, *, now_epoch_ms: int, limit: int = 100) -> tuple[DelayedJob, ...]:
        rows = self.backend.zrangebyscore(
            self.key,
            float("-inf"),
            float(now_epoch_ms),
            start=0,
            num=limit,
            withscores=True,
        )
        jobs = tuple(DelayedJob(job_id=str(job_id), run_at_epoch_ms=int(score)) for job_id, score in rows)
        if jobs:
            self.backend.zrem(self.key, *(job.job_id for job in jobs))
        return jobs
