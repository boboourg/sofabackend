"""Operational queue summary helpers shared by health and local API endpoints."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from ..queue.delayed import DELAYED_JOBS_KEY
from ..queue.streams import ALL_CONSUMER_GROUPS


@dataclass(frozen=True)
class StreamQueueSummary:
    stream: str
    group: str
    length: int
    pending_total: int
    smallest_id: str | None
    largest_id: str | None
    consumers: dict[str, int] = field(default_factory=dict)
    group_consumers: int = 0
    entries_read: int | None = None
    lag: int | None = None
    last_delivered_id: str | None = None


@dataclass(frozen=True)
class QueueSummary:
    redis_backend_kind: str
    live_lanes: dict[str, int] = field(default_factory=dict)
    streams: tuple[StreamQueueSummary, ...] = field(default_factory=tuple)
    delayed_total: int = 0
    delayed_due: int = 0
    # F-7 Phase 0 observability: cumulative claim/clear/publish counters
    # by tier, and a snapshot of currently-tracked events per dispatch
    # tier. Both are collected in this read-only ops path only — never in
    # the planner hot path. Empty when the live state store backend
    # does not expose HGETALL (test fakes / dev fallbacks).
    live_dispatch_metrics: dict[str, int] = field(default_factory=dict)
    live_tier_counts: dict[str, int] = field(default_factory=dict)


async def collect_queue_summary(
    *,
    stream_queue=None,
    live_state_store=None,
    redis_backend=None,
    now_ms: float | None = None,
) -> QueueSummary:
    lane_counts = {
        "hot": _lane_count(live_state_store, "hot"),
        "warm": _lane_count(live_state_store, "warm"),
        "cold": _lane_count(live_state_store, "cold"),
    }
    live_dispatch_metrics = _dispatch_metrics_snapshot(live_state_store)
    live_tier_counts = _tier_active_counts(live_state_store)

    stream_summaries: list[StreamQueueSummary] = []
    if stream_queue is not None:
        for stream_name, group_name in ALL_CONSUMER_GROUPS:
            try:
                pending = stream_queue.pending_summary(stream_name, group_name)
            except Exception:
                pending = None
            try:
                stream_length = stream_queue.stream_length(stream_name)
            except Exception:
                stream_length = 0
            try:
                group_info = stream_queue.group_info(stream_name, group_name)
            except Exception:
                group_info = None
            stream_summaries.append(
                StreamQueueSummary(
                    stream=stream_name,
                    group=group_name,
                    length=int(stream_length),
                    pending_total=0 if pending is None else int(pending.total),
                    smallest_id=None if pending is None else pending.smallest_id,
                    largest_id=None if pending is None else pending.largest_id,
                    consumers={} if pending is None else dict(pending.consumers),
                    group_consumers=0 if group_info is None else int(group_info.consumers),
                    entries_read=None if group_info is None else group_info.entries_read,
                    lag=None if group_info is None else group_info.lag,
                    last_delivered_id=None if group_info is None else group_info.last_delivered_id,
                )
            )

    delayed_total = 0
    delayed_due = 0
    if redis_backend is not None:
        zrangebyscore = getattr(redis_backend, "zrangebyscore", None)
        if callable(zrangebyscore):
            cutoff = float("inf") if now_ms is None else float(now_ms)
            delayed_total = len(tuple(zrangebyscore(DELAYED_JOBS_KEY, float("-inf"), float("inf"))))
            delayed_due = len(tuple(zrangebyscore(DELAYED_JOBS_KEY, float("-inf"), cutoff)))

    return QueueSummary(
        redis_backend_kind=type(redis_backend).__name__ if redis_backend is not None else "none",
        live_lanes=lane_counts,
        streams=tuple(stream_summaries),
        delayed_total=int(delayed_total),
        delayed_due=int(delayed_due),
        live_dispatch_metrics=live_dispatch_metrics,
        live_tier_counts=live_tier_counts,
    )


def _lane_count(live_state_store, lane: str) -> int:
    if live_state_store is None:
        return 0
    members = live_state_store.backend.zrangebyscore(
        live_state_store._lane_key(lane),
        float("-inf"),
        float("inf"),
    )
    return len(tuple(members))


def _dispatch_metrics_snapshot(live_state_store) -> dict[str, int]:
    if live_state_store is None:
        return {}
    snapshot = getattr(live_state_store, "dispatch_metrics_snapshot", None)
    if not callable(snapshot):
        return {}
    try:
        return dict(snapshot() or {})
    except Exception:
        return {}


def _tier_active_counts(live_state_store) -> dict[str, int]:
    if live_state_store is None:
        return {}
    counts = getattr(live_state_store, "tier_active_counts", None)
    if not callable(counts):
        return {}
    try:
        return dict(counts() or {})
    except Exception:
        return {}
