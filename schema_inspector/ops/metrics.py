"""Small metrics helpers for hybrid ETL operations."""

from __future__ import annotations

from dataclasses import dataclass

from .health import HealthReport


@dataclass(frozen=True)
class MetricsSnapshot:
    snapshot_count: int
    capability_rollup_count: int
    live_load: int

    @classmethod
    def from_health_report(cls, report: HealthReport) -> "MetricsSnapshot":
        return cls(
            snapshot_count=report.snapshot_count,
            capability_rollup_count=report.capability_rollup_count,
            live_load=report.live_hot_count + report.live_warm_count + report.live_cold_count,
        )
