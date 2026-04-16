"""Health and metrics helpers for the hybrid ETL runtime."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HealthReport:
    snapshot_count: int
    capability_rollup_count: int
    live_hot_count: int
    live_warm_count: int
    live_cold_count: int


async def collect_health_report(*, sql_executor, live_state_store=None) -> HealthReport:
    snapshot_count = int(await _fetch_count(sql_executor, "SELECT COUNT(*) FROM api_payload_snapshot"))
    capability_rollup_count = int(await _fetch_count(sql_executor, "SELECT COUNT(*) FROM endpoint_capability_rollup"))
    return HealthReport(
        snapshot_count=snapshot_count,
        capability_rollup_count=capability_rollup_count,
        live_hot_count=_lane_count(live_state_store, "hot"),
        live_warm_count=_lane_count(live_state_store, "warm"),
        live_cold_count=_lane_count(live_state_store, "cold"),
    )


async def _fetch_count(sql_executor, query: str) -> int:
    fetchval = getattr(sql_executor, "fetchval", None)
    if callable(fetchval):
        value = await fetchval(query)
        return int(value or 0)
    rows = await sql_executor.fetch(query)
    if not rows:
        return 0
    first = rows[0]
    if isinstance(first, dict):
        return int(next(iter(first.values())))
    return int(first[0])


def _lane_count(live_state_store, lane: str) -> int:
    if live_state_store is None:
        return 0
    members = live_state_store.backend.zrangebyscore(
        live_state_store._lane_key(lane),
        float("-inf"),
        float("inf"),
    )
    return len(tuple(members))
