"""Queue lag guards for throttling archival planners."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BackpressureLimit:
    stream: str
    group: str
    max_lag: int


@dataclass(frozen=True)
class QueueBackpressure:
    queue: object
    limits: tuple[BackpressureLimit, ...]

    def blocking_reason(self) -> str | None:
        group_info = getattr(self.queue, "group_info", None)
        if not callable(group_info):
            return None
        for limit in self.limits:
            info = group_info(limit.stream, limit.group)
            if info is None:
                continue
            lag = getattr(info, "lag", None)
            if lag is None:
                continue
            if int(lag) > int(limit.max_lag):
                return f"{limit.stream}:{limit.group}:lag={int(lag)}>{int(limit.max_lag)}"
        return None
