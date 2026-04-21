"""Health and metrics helpers for the hybrid ETL runtime."""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class DriftFlag:
    surface: str
    sport_slug: str
    reason: str


@dataclass(frozen=True)
class DriftSummary:
    flag_count: int = 0
    flags: tuple[DriftFlag, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class HealthReport:
    snapshot_count: int
    capability_rollup_count: int
    live_hot_count: int
    live_warm_count: int
    live_cold_count: int
    database_ok: bool
    redis_ok: bool
    redis_backend_kind: str
    drift_summary: DriftSummary = DriftSummary()


async def collect_health_report(*, sql_executor, live_state_store=None, redis_backend=None) -> HealthReport:
    snapshot_count = int(await _fetch_count(sql_executor, "SELECT COUNT(*) FROM api_payload_snapshot"))
    capability_rollup_count = int(await _fetch_count(sql_executor, "SELECT COUNT(*) FROM endpoint_capability_rollup"))
    drift_summary = await _fetch_drift_summary(sql_executor)
    return HealthReport(
        snapshot_count=snapshot_count,
        capability_rollup_count=capability_rollup_count,
        live_hot_count=_lane_count(live_state_store, "hot"),
        live_warm_count=_lane_count(live_state_store, "warm"),
        live_cold_count=_lane_count(live_state_store, "cold"),
        database_ok=True,
        redis_ok=_ping_redis(redis_backend),
        redis_backend_kind=_backend_kind(redis_backend),
        drift_summary=drift_summary,
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


def _ping_redis(redis_backend) -> bool:
    if redis_backend is None:
        return False
    ping = getattr(redis_backend, "ping", None)
    if not callable(ping):
        return False
    try:
        return bool(ping())
    except Exception:
        return False


def _backend_kind(redis_backend) -> str:
    if redis_backend is None:
        return "none"
    class_name = type(redis_backend).__name__.lower().lstrip("_")
    if class_name == "memoryredisbackend":
        return "memory"
    return class_name


async def _fetch_drift_summary(sql_executor) -> DriftSummary:
    rows = await sql_executor.fetch(
        """
        WITH latest_live_snapshot AS (
            SELECT
                aps.sport_slug,
                MAX(aps.fetched_at) AS latest_fetched_at
            FROM api_payload_snapshot AS aps
            WHERE aps.endpoint_pattern = '/api/v1/sport/{sport_slug}/events/live'
              AND aps.sport_slug IS NOT NULL
            GROUP BY aps.sport_slug
        ),
        latest_terminal_state AS (
            SELECT
                s.slug AS sport_slug,
                MAX(ets.finalized_at) AS latest_finalized_at
            FROM event_terminal_state AS ets
            JOIN event AS e ON e.id = ets.event_id
            JOIN unique_tournament AS ut ON ut.id = e.unique_tournament_id
            JOIN category AS c ON c.id = ut.category_id
            JOIN sport AS s ON s.id = c.sport_id
            WHERE ets.finalized_at IS NOT NULL
            GROUP BY s.slug
        )
        SELECT
            'sport_live_events' AS surface,
            terminal_state.sport_slug AS sport_slug,
            'snapshot_older_than_terminal_state' AS reason
        FROM latest_terminal_state AS terminal_state
        JOIN latest_live_snapshot AS live_snapshot
            ON live_snapshot.sport_slug = terminal_state.sport_slug
        WHERE live_snapshot.latest_fetched_at < terminal_state.latest_finalized_at
        ORDER BY terminal_state.sport_slug
        """
    )
    flags = tuple(
        DriftFlag(
            surface=str(row["surface"]),
            sport_slug=str(row["sport_slug"]),
            reason=str(row["reason"]),
        )
        for row in rows
    )
    return DriftSummary(flag_count=len(flags), flags=flags)
