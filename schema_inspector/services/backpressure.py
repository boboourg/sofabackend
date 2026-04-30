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
        for limit in self.limits:
            reason = self.blocking_reason_for_stream(limit.stream)
            if reason is not None:
                return reason
        return None

    def blocking_reason_for_stream(self, stream: str) -> str | None:
        group_info = getattr(self.queue, "group_info", None)
        if not callable(group_info):
            return None
        for limit in self.limits:
            if str(limit.stream or "") != str(stream or ""):
                continue
            info = group_info(limit.stream, limit.group)
            if info is None:
                continue
            lag = getattr(info, "lag", None)
            if lag is None:
                continue
            if int(lag) > int(limit.max_lag):
                return f"{limit.stream}:{limit.group}:lag={int(lag)}>{int(limit.max_lag)}"
        return None
