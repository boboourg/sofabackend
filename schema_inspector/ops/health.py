"""Health and metrics helpers for the hybrid ETL runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from ..queue.streams import (
    STREAM_HISTORICAL_ENRICHMENT,
    STREAM_HISTORICAL_HYDRATE,
    STREAM_HYDRATE,
    STREAM_LIVE_HOT,
    STREAM_LIVE_WARM,
    STREAM_STRUCTURE_SYNC,
)
from ..services.backpressure_config import (
    HISTORICAL_HYDRATE_MAX_LAG,
    HYDRATE_MAX_LAG,
    LIVE_HOT_MAX_LAG,
    LIVE_WARM_MAX_LAG,
    STRUCTURE_SYNC_MAX_LAG,
)
from ..services.housekeeping import HousekeepingConfig
from ..sport_profiles import resolve_sport_profile
from ..source_priority import SOURCE_PRIORITY
from .queue_summary import QueueSummary, collect_queue_summary


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
class ReconcilePolicySourceEntry:
    source_slug: str
    priority: int


@dataclass(frozen=True)
class ReconcilePolicySummary:
    policy_enabled: bool
    primary_source_slug: str | None
    source_count: int
    sources: tuple[ReconcilePolicySourceEntry, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class GoLiveFlag:
    severity: str
    reason: str
    actual: Any = None
    threshold: Any = None


@dataclass(frozen=True)
class GoLiveSummary:
    ready: bool
    flag_count: int = 0
    flags: tuple[GoLiveFlag, ...] = field(default_factory=tuple)
    snapshot_latest_fetched_at: datetime | None = None
    snapshot_age_seconds: int | None = None
    historical_enrichment_lag: int = 0
    historical_retry_share: float = 0.0
    housekeeping_dry_run: bool = False
    queue_lag_summary: dict[str, int] = field(default_factory=dict)


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
    reconcile_policy_summary: ReconcilePolicySummary = ReconcilePolicySummary(
        policy_enabled=False,
        primary_source_slug=None,
        source_count=0,
    )
    queue_summary: QueueSummary = QueueSummary(redis_backend_kind="none")
    go_live: GoLiveSummary = GoLiveSummary(ready=False)


_SNAPSHOT_FRESHNESS_MAX_AGE_SECONDS = 300
_HISTORICAL_ENRICHMENT_GO_LIVE_MAX_LAG = 1000
_HISTORICAL_RETRY_SHARE_MAX = 0.01
_LIVE_SNAPSHOT_TERMINAL_GRACE_SECONDS = 30


async def collect_health_report(
    *,
    sql_executor,
    live_state_store=None,
    redis_backend=None,
    stream_queue=None,
    housekeeping_dry_run: bool | None = None,
    now: datetime | None = None,
) -> HealthReport:
    now_utc = now or datetime.now(timezone.utc)
    snapshot_count = int(await _fetch_count(sql_executor, "SELECT COUNT(*) FROM api_payload_snapshot"))
    capability_rollup_count = int(await _fetch_count(sql_executor, "SELECT COUNT(*) FROM endpoint_capability_rollup"))
    drift_summary = await _fetch_drift_summary(sql_executor)
    coverage_summary = await _fetch_coverage_summary(sql_executor)
    coverage_alert_summary = _build_coverage_alert_summary(coverage_summary)
    reconcile_policy_summary = _build_reconcile_policy_summary()
    queue_summary = await collect_queue_summary(
        stream_queue=stream_queue,
        live_state_store=live_state_store,
        redis_backend=redis_backend,
        now_ms=now_utc.timestamp() * 1000.0,
    )
    snapshot_latest_fetched_at = await _fetch_latest_snapshot_fetched_at(sql_executor)
    historical_retry_share = await _fetch_historical_retry_share(sql_executor)
    resolved_housekeeping_dry_run = (
        HousekeepingConfig.from_env().dry_run
        if housekeeping_dry_run is None
        else bool(housekeeping_dry_run)
    )
    go_live = _build_go_live_summary(
        database_ok=True,
        redis_ok=_ping_redis(redis_backend),
        drift_summary=drift_summary,
        coverage_alert_summary=coverage_alert_summary,
        queue_summary=queue_summary,
        snapshot_latest_fetched_at=snapshot_latest_fetched_at,
        historical_retry_share=historical_retry_share,
        housekeeping_dry_run=resolved_housekeeping_dry_run,
        now=now_utc,
    )
    return HealthReport(
        snapshot_count=snapshot_count,
        capability_rollup_count=capability_rollup_count,
        live_hot_count=int(queue_summary.live_lanes.get("hot", 0)),
        live_warm_count=int(queue_summary.live_lanes.get("warm", 0)),
        live_cold_count=int(queue_summary.live_lanes.get("cold", 0)),
        database_ok=True,
        redis_ok=_ping_redis(redis_backend),
        redis_backend_kind=_backend_kind(redis_backend),
        drift_summary=drift_summary,
        coverage_summary=coverage_summary,
        coverage_alert_summary=coverage_alert_summary,
        reconcile_policy_summary=reconcile_policy_summary,
        queue_summary=queue_summary,
        go_live=go_live,
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
                COALESCE(
                    NULLIF(aps.sport_slug, ''),
                    CASE
                        WHEN aps.endpoint_pattern LIKE '/api/v1/sport/%/events/live'
                            THEN split_part(aps.endpoint_pattern, '/', 5)
                        WHEN aps.source_url LIKE '%/api/v1/sport/%/events/live%'
                            THEN split_part(split_part(aps.source_url, '/api/v1/sport/', 2), '/', 1)
                        ELSE NULL
                    END
                ) AS sport_slug,
                MAX(aps.fetched_at) AS latest_fetched_at
            FROM api_payload_snapshot AS aps
            WHERE (
                aps.endpoint_pattern = '/api/v1/sport/{sport_slug}/events/live'
                OR aps.endpoint_pattern LIKE '/api/v1/sport/%/events/live'
                OR aps.source_url LIKE '%/api/v1/sport/%/events/live%'
            )
            GROUP BY 1
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
            'snapshot_older_than_terminal_state' AS reason,
            live_snapshot.latest_fetched_at AS latest_fetched_at,
            terminal_state.latest_finalized_at AS latest_finalized_at
        FROM latest_terminal_state AS terminal_state
        JOIN latest_live_snapshot AS live_snapshot
            ON live_snapshot.sport_slug = terminal_state.sport_slug
        WHERE live_snapshot.sport_slug IS NOT NULL
          AND live_snapshot.latest_fetched_at < terminal_state.latest_finalized_at
        ORDER BY terminal_state.sport_slug
        """
    )
    flags: list[DriftFlag] = []
    for row in rows:
        sport_slug = str(row["sport_slug"])
        latest_fetched_at = row.get("latest_fetched_at")
        latest_finalized_at = row.get("latest_finalized_at")
        if isinstance(latest_fetched_at, datetime) and isinstance(latest_finalized_at, datetime):
            lag_seconds = max(0, int((latest_finalized_at - latest_fetched_at).total_seconds()))
            allowed_lag_seconds = int(resolve_sport_profile(sport_slug).live_discovery_interval_seconds) + _LIVE_SNAPSHOT_TERMINAL_GRACE_SECONDS
            if lag_seconds <= allowed_lag_seconds:
                continue
        flags.append(
            DriftFlag(
                surface=str(row["surface"]),
                sport_slug=sport_slug,
                reason=str(row["reason"]),
            )
        )
    return DriftSummary(flag_count=len(flags), flags=tuple(flags))


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


def _build_reconcile_policy_summary() -> ReconcilePolicySummary:
    sources = tuple(
        ReconcilePolicySourceEntry(source_slug=source_slug, priority=int(priority))
        for source_slug, priority in sorted(
            SOURCE_PRIORITY.items(),
            key=lambda item: (-int(item[1]), str(item[0])),
        )
    )
    primary_source_slug = sources[0].source_slug if sources else None
    return ReconcilePolicySummary(
        policy_enabled=bool(sources),
        primary_source_slug=primary_source_slug,
        source_count=len(sources),
        sources=sources,
    )


async def _fetch_latest_snapshot_fetched_at(sql_executor) -> datetime | None:
    rows = await sql_executor.fetch(
        """
        SELECT MAX(fetched_at) AS latest_fetched_at
        FROM api_payload_snapshot
        """
    )
    if not rows:
        return None
    row = rows[0]
    return row.get("latest_fetched_at")


async def _fetch_historical_retry_share(sql_executor) -> float:
    rows = await sql_executor.fetch(
        """
        SELECT
            COUNT(*) FILTER (WHERE scope LIKE 'historical%')::bigint AS recent_total_runs,
            COUNT(*) FILTER (
                WHERE scope LIKE 'historical%'
                  AND status = 'retry_scheduled'
                  AND COALESCE(error_class, '') <> 'AdmissionDeferredError'
            )::bigint AS retry_scheduled_runs
        FROM etl_job_run
        WHERE started_at >= now() - interval '2 hours'
        """
    )
    if not rows:
        return 0.0
    row = rows[0]
    total_runs = int(row.get("recent_total_runs") or 0)
    retry_runs = int(row.get("retry_scheduled_runs") or 0)
    if total_runs <= 0:
        return 0.0
    return retry_runs / total_runs


def _build_go_live_summary(
    *,
    database_ok: bool,
    redis_ok: bool,
    drift_summary: DriftSummary,
    coverage_alert_summary: CoverageAlertSummary,
    queue_summary: QueueSummary,
    snapshot_latest_fetched_at: datetime | None,
    historical_retry_share: float,
    housekeeping_dry_run: bool,
    now: datetime,
) -> GoLiveSummary:
    queue_lag_summary = {
        item.stream: int(item.lag or 0)
        for item in queue_summary.streams
        if item.lag is not None
    }
    historical_enrichment_lag = int(queue_lag_summary.get(STREAM_HISTORICAL_ENRICHMENT, 0))
    snapshot_age_seconds = (
        None
        if snapshot_latest_fetched_at is None
        else max(0, int((now - snapshot_latest_fetched_at).total_seconds()))
    )

    flags: list[GoLiveFlag] = []
    if not database_ok:
        flags.append(GoLiveFlag(severity="error", reason="database_unhealthy", actual=False, threshold=True))
    if not redis_ok:
        flags.append(GoLiveFlag(severity="error", reason="redis_unhealthy", actual=False, threshold=True))
    if snapshot_latest_fetched_at is None:
        flags.append(GoLiveFlag(severity="error", reason="snapshot_missing", actual=None, threshold="<=300s"))
    elif snapshot_age_seconds is not None and snapshot_age_seconds > _SNAPSHOT_FRESHNESS_MAX_AGE_SECONDS:
        flags.append(
            GoLiveFlag(
                severity="error",
                reason="snapshot_stale",
                actual=snapshot_age_seconds,
                threshold=_SNAPSHOT_FRESHNESS_MAX_AGE_SECONDS,
            )
        )
    if historical_enrichment_lag > _HISTORICAL_ENRICHMENT_GO_LIVE_MAX_LAG:
        flags.append(
            GoLiveFlag(
                severity="error",
                reason="historical_enrichment_backlog_high",
                actual=historical_enrichment_lag,
                threshold=_HISTORICAL_ENRICHMENT_GO_LIVE_MAX_LAG,
            )
        )
    if historical_retry_share > _HISTORICAL_RETRY_SHARE_MAX:
        flags.append(
            GoLiveFlag(
                severity="error",
                reason="historical_retry_share_high",
                actual=historical_retry_share,
                threshold=_HISTORICAL_RETRY_SHARE_MAX,
            )
        )
    if housekeeping_dry_run:
        flags.append(
            GoLiveFlag(
                severity="error",
                reason="housekeeping_dry_run_enabled",
                actual=True,
                threshold=False,
            )
        )
    if drift_summary.flag_count > 0:
        flags.append(
            GoLiveFlag(
                severity="error",
                reason="live_snapshot_drift_detected",
                actual=drift_summary.flag_count,
                threshold=0,
            )
        )
    if coverage_alert_summary.flag_count > 0:
        flags.append(
            GoLiveFlag(
                severity="warning",
                reason="coverage_stale_scopes_present",
                actual=coverage_alert_summary.flag_count,
                threshold=0,
            )
        )

    _append_queue_gate(
        flags,
        reason="hydrate_backlog_high",
        lag=queue_lag_summary.get(STREAM_HYDRATE),
        threshold=HYDRATE_MAX_LAG,
        severity="warning",
    )
    _append_queue_gate(
        flags,
        reason="live_hot_backlog_high",
        lag=queue_lag_summary.get(STREAM_LIVE_HOT),
        threshold=LIVE_HOT_MAX_LAG,
        severity="warning",
    )
    _append_queue_gate(
        flags,
        reason="live_warm_backlog_high",
        lag=queue_lag_summary.get(STREAM_LIVE_WARM),
        threshold=LIVE_WARM_MAX_LAG,
        severity="warning",
    )
    _append_queue_gate(
        flags,
        reason="historical_hydrate_backlog_high",
        lag=queue_lag_summary.get(STREAM_HISTORICAL_HYDRATE),
        threshold=HISTORICAL_HYDRATE_MAX_LAG,
        severity="warning",
    )
    _append_queue_gate(
        flags,
        reason="structure_sync_backlog_high",
        lag=queue_lag_summary.get(STREAM_STRUCTURE_SYNC),
        threshold=STRUCTURE_SYNC_MAX_LAG,
        severity="warning",
    )

    ready = not any(flag.severity == "error" for flag in flags)
    return GoLiveSummary(
        ready=ready,
        flag_count=len(flags),
        flags=tuple(flags),
        snapshot_latest_fetched_at=snapshot_latest_fetched_at,
        snapshot_age_seconds=snapshot_age_seconds,
        historical_enrichment_lag=historical_enrichment_lag,
        historical_retry_share=historical_retry_share,
        housekeeping_dry_run=housekeeping_dry_run,
        queue_lag_summary=queue_lag_summary,
    )


def _append_queue_gate(
    flags: list[GoLiveFlag],
    *,
    reason: str,
    lag: int | None,
    threshold: int,
    severity: str,
) -> None:
    if lag is None:
        return
    if int(lag) <= int(threshold):
        return
    flags.append(
        GoLiveFlag(
            severity=severity,
            reason=reason,
            actual=int(lag),
            threshold=int(threshold),
        )
    )
