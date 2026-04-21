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
class CoverageSummary:
    tracked_scope_count: int = 0
    fresh_scope_count: int = 0
    stale_scope_count: int = 0
    other_scope_count: int = 0
    source_count: int = 0
    sport_count: int = 0
    surface_count: int = 0
    avg_completeness_ratio: float = 0.0


@dataclass(frozen=True)
class CoverageAlert:
    severity: str
    reason: str
    stale_scope_count: int


@dataclass(frozen=True)
class CoverageAlertSummary:
    flag_count: int = 0
    flags: tuple[CoverageAlert, ...] = field(default_factory=tuple)


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
    coverage_summary: CoverageSummary = CoverageSummary()
    coverage_alert_summary: CoverageAlertSummary = CoverageAlertSummary()


async def collect_health_report(*, sql_executor, live_state_store=None, redis_backend=None) -> HealthReport:
    snapshot_count = int(await _fetch_count(sql_executor, "SELECT COUNT(*) FROM api_payload_snapshot"))
    capability_rollup_count = int(await _fetch_count(sql_executor, "SELECT COUNT(*) FROM endpoint_capability_rollup"))
    drift_summary = await _fetch_drift_summary(sql_executor)
    coverage_summary = await _fetch_coverage_summary(sql_executor)
    coverage_alert_summary = _build_coverage_alert_summary(coverage_summary)
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
        coverage_summary=coverage_summary,
        coverage_alert_summary=coverage_alert_summary,
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


async def _fetch_coverage_summary(sql_executor) -> CoverageSummary:
    rows = await sql_executor.fetch(
        """
        SELECT
            COUNT(*)::bigint AS tracked_scope_count,
            COUNT(*) FILTER (WHERE freshness_status = 'fresh')::bigint AS fresh_scope_count,
            COUNT(*) FILTER (WHERE freshness_status = 'stale')::bigint AS stale_scope_count,
            COUNT(*) FILTER (
                WHERE freshness_status <> 'fresh' AND freshness_status <> 'stale'
            )::bigint AS other_scope_count,
            COUNT(DISTINCT source_slug)::bigint AS source_count,
            COUNT(DISTINCT sport_slug)::bigint AS sport_count,
            COUNT(DISTINCT surface_name)::bigint AS surface_count,
            COALESCE(AVG(completeness_ratio), 0)::double precision AS avg_completeness_ratio
        FROM coverage_ledger
        """
    )
    if not rows:
        return CoverageSummary()
    row = rows[0]
    return CoverageSummary(
        tracked_scope_count=int(row.get("tracked_scope_count") or 0),
        fresh_scope_count=int(row.get("fresh_scope_count") or 0),
        stale_scope_count=int(row.get("stale_scope_count") or 0),
        other_scope_count=int(row.get("other_scope_count") or 0),
        source_count=int(row.get("source_count") or 0),
        sport_count=int(row.get("sport_count") or 0),
        surface_count=int(row.get("surface_count") or 0),
        avg_completeness_ratio=float(row.get("avg_completeness_ratio") or 0.0),
    )


def _build_coverage_alert_summary(coverage_summary: CoverageSummary) -> CoverageAlertSummary:
    stale_scope_count = int(coverage_summary.stale_scope_count)
    if stale_scope_count <= 0:
        return CoverageAlertSummary()
    flags = (
        CoverageAlert(
            severity="warning",
            reason="stale_coverage_scopes_present",
            stale_scope_count=stale_scope_count,
        ),
    )
    return CoverageAlertSummary(flag_count=len(flags), flags=flags)
