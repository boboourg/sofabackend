"""Traffic-based proxy health aggregates from request logs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ProxyTrafficAggregate:
    proxy_address: str
    total_requests: int
    success_count: int
    failure_count: int
    status_403_count: int
    status_404_count: int
    status_5xx_count: int
    latest_started_at: object | None

    @property
    def success_rate(self) -> float:
        if self.total_requests <= 0:
            return 0.0
        return float(self.success_count) / float(self.total_requests)


class ProxyHealthRepository:
    """Reads recent proxy traffic aggregates from ``api_request_log``."""

    async def fetch_proxy_traffic(self, connection: Any, *, window_seconds: int) -> tuple[ProxyTrafficAggregate, ...]:
        rows = await connection.fetch(
            """
            SELECT
                proxy_address,
                COUNT(*)::int AS total_requests,
                COUNT(*) FILTER (WHERE http_status >= 200 AND http_status < 300)::int AS success_count,
                COUNT(*) FILTER (WHERE http_status IS NULL OR http_status < 200 OR http_status >= 300)::int AS failure_count,
                COUNT(*) FILTER (WHERE http_status = 403)::int AS status_403_count,
                COUNT(*) FILTER (WHERE http_status = 404)::int AS status_404_count,
                COUNT(*) FILTER (WHERE http_status >= 500)::int AS status_5xx_count,
                MAX(started_at) AS latest_started_at
            FROM api_request_log
            WHERE started_at >= NOW() - ($1::double precision * INTERVAL '1 second')
              AND proxy_address IS NOT NULL
            GROUP BY proxy_address
            """,
            int(window_seconds),
        )
        return tuple(_row_to_aggregate(row) for row in rows)


def _row_to_aggregate(row: Any) -> ProxyTrafficAggregate:
    return ProxyTrafficAggregate(
        proxy_address=str(row["proxy_address"]),
        total_requests=int(row["total_requests"]),
        success_count=int(row["success_count"]),
        failure_count=int(row["failure_count"]),
        status_403_count=int(row["status_403_count"]),
        status_404_count=int(row["status_404_count"]),
        status_5xx_count=int(row["status_5xx_count"]),
        latest_started_at=row["latest_started_at"],
    )
