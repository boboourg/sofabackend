"""Health and metrics helpers for the hybrid ETL runtime."""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

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


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


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
class LiveFreshnessSlo:
    """Single SLO measurement (live freshness P0.C, 2026-05-14)."""

    name: str
    actual: float | int | None
    threshold: float | int
    breached: bool
    note: str | None = None


@dataclass(frozen=True)
class LiveFreshnessSummary:
    """Live-path freshness signals exposed at /ops/health.

    Status is ``healthy`` when no SLO is breached, ``degraded`` when at least
    one is breached. SLOs are deliberately observability-only — they do NOT
    change runtime behaviour, only surface live signals to operators.
    See docs/ARCHITECTURE_AUDIT.md (Part 3 P0.C).
    """

    status: str = "unknown"
    slos: tuple[LiveFreshnessSlo, ...] = field(default_factory=tuple)
    # Raw metrics surfaced even when SLO is not breached, for trend analysis:
    oldest_hot_score_age_seconds: int | None = None
    refresh_live_event_success_rate_5min: float | None = None
    tier_1_blocked_rate_cumulative: float | None = None
    tier_1_active_events: int = 0
    refresh_live_event_succeeded_5min: int = 0
    refresh_live_event_retry_5min: int = 0
    refresh_live_event_failed_5min: int = 0
    # Task 2 (2026-05-15): tier_1 P5b quarantined events. Correlates with
    # upstream selective throttling (Real Madrid-class incidents).
    tier_1_quarantined_events: int | None = None


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
    live_freshness: LiveFreshnessSummary = LiveFreshnessSummary()


_SNAPSHOT_FRESHNESS_MAX_AGE_SECONDS = 300
# Default 1000 historically; env override allows ops to raise the gate to a
# realistic value without touching code (live-first prioritization means the
# enrichment lane intentionally drifts behind during peak hours).
_HISTORICAL_ENRICHMENT_GO_LIVE_MAX_LAG = _env_int(
    "HISTORICAL_ENRICHMENT_GO_LIVE_MAX_LAG", 1000
)
_HISTORICAL_RETRY_SHARE_MAX = 0.01
_LIVE_SNAPSHOT_TERMINAL_GRACE_SECONDS = 30

# Live freshness SLO thresholds (P0.C 2026-05-14). All env-overridable.
# Defaults sourced from docs/ARCHITECTURE_AUDIT.md Part 5 (revised 24/7 gates).
_LIVE_FRESHNESS_OLDEST_HOT_AGE_MAX_SECONDS = _env_int(
    "LIVE_FRESHNESS_OLDEST_HOT_AGE_MAX_SECONDS", 900
)  # 15 minutes — see ARCHITECTURE_AUDIT.md C.2
_LIVE_FRESHNESS_SUCCESS_RATE_MIN_BASIS_POINTS = _env_int(
    "LIVE_FRESHNESS_SUCCESS_RATE_MIN_BASIS_POINTS", 9500
)  # 95% (basis points to avoid float env parse)
_LIVE_FRESHNESS_TIER_1_BLOCKED_MAX_BASIS_POINTS = _env_int(
    "LIVE_FRESHNESS_TIER_1_BLOCKED_MAX_BASIS_POINTS", 8000
)  # 80%


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
    # Post-review fix (2026-05-15): SELECT COUNT(*) on api_payload_snapshot
    # (~5M rows) full-scans for 30-60s and was the dominant /ops/health
    # latency. Use pg_class.reltuples — instant catalog read that returns
    # the planner's autovacuum-maintained row estimate. Accurate enough
    # for a health dashboard (it does not need exact precision); the
    # autovacuum cadence on prod keeps drift under ~5%. Fallback to 0 if
    # the catalog read fails (best-effort — never crash /ops/health).
    snapshot_count = int(
        await _fetch_count(
            sql_executor,
            "SELECT COALESCE(reltuples, 0)::bigint FROM pg_class "
            "WHERE relname = 'api_payload_snapshot'",
        )
    )
    capability_rollup_count = int(
        await _fetch_count(
            sql_executor,
            "SELECT COALESCE(reltuples, 0)::bigint FROM pg_class "
            "WHERE relname = 'endpoint_capability_rollup'",
        )
    )
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
    live_freshness = await _build_live_freshness_summary(
        sql_executor=sql_executor,
        redis_backend=redis_backend,
        queue_summary=queue_summary,
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
        live_freshness=live_freshness,
    )


async def _build_live_freshness_summary(
    *,
    sql_executor,
    redis_backend,
    queue_summary: QueueSummary,
    now: datetime,
) -> LiveFreshnessSummary:
    """Compose the live freshness summary (P0.C 2026-05-14, expanded 2026-05-15).

    Best-effort: any underlying probe failure returns ``None`` for that
    metric. Health endpoint must never fail because one sub-probe could
    not complete.

    Sources:
      * oldest score age in ``zset:live:hot`` (Redis ZRANGE, fast)
      * tier_1 blocked rate (from QueueSummary.live_dispatch_metrics)
      * refresh_live_event 5-min success rate (etl_job_run via BRIN
        index ``idx_etl_job_run_started_at_brin``, landed
        2026-05-14 commit fd59951)
      * tier_1 quarantined events (Redis SCAN on
        ``live:tier1_quarantine:*``)

    Historical note: the 5-minute count query was disabled briefly in
    P0.C because the original implementation triggered a 30s full scan
    on ``etl_job_run`` (no usable index on ``started_at``). With the
    BRIN index in place this query reliably runs in low-double-digit
    milliseconds on prod (~5M rows), so it has been restored.
    """

    oldest_age_seconds = await _fetch_oldest_hot_score_age_seconds(
        redis_backend, now
    )
    counts_5min = await _fetch_refresh_live_event_5min_counts(
        sql_executor=sql_executor, now=now
    )
    tier_1_quarantined = await _fetch_tier_1_quarantined_count(redis_backend)

    tier_1_blocked = _tier_1_blocked_rate(queue_summary)
    tier_1_active = int((queue_summary.live_tier_counts or {}).get("tier_1", 0))
    success_rate = _refresh_success_rate(counts_5min)

    age_threshold = _LIVE_FRESHNESS_OLDEST_HOT_AGE_MAX_SECONDS
    success_threshold = _LIVE_FRESHNESS_SUCCESS_RATE_MIN_BASIS_POINTS / 10_000.0
    blocked_threshold = _LIVE_FRESHNESS_TIER_1_BLOCKED_MAX_BASIS_POINTS / 10_000.0

    slos: list[LiveFreshnessSlo] = [
        LiveFreshnessSlo(
            name="oldest_hot_score_age_seconds",
            actual=oldest_age_seconds,
            threshold=age_threshold,
            breached=(
                oldest_age_seconds is not None and oldest_age_seconds > age_threshold
            ),
            note=(
                "stale entries in zset:live:hot indicate cleanup is lagging "
                "(see ARCHITECTURE_AUDIT.md C.2)"
                if oldest_age_seconds is not None and oldest_age_seconds > age_threshold
                else None
            ),
        ),
        LiveFreshnessSlo(
            name="refresh_live_event_success_rate_5min",
            actual=success_rate,
            threshold=success_threshold,
            breached=(
                success_rate is not None and success_rate < success_threshold
            ),
            note=(
                "live refresh success rate dropped — check retry log + "
                "stream:etl:live_tier_* downstream"
                if success_rate is not None and success_rate < success_threshold
                else None
            ),
        ),
        LiveFreshnessSlo(
            name="tier_1_blocked_rate_cumulative",
            actual=tier_1_blocked,
            # tier_1 SLO is only meaningful when tier_1 events are actually
            # being polled. With zero active tier_1 events the metric is
            # cumulative (since planner restart) and not a current signal.
            threshold=blocked_threshold,
            breached=(
                tier_1_active > 0
                and tier_1_blocked is not None
                and tier_1_blocked > blocked_threshold
            ),
            note=(
                "tier_1 dispatch lease likely too long for current poll cadence"
                if (
                    tier_1_active > 0
                    and tier_1_blocked is not None
                    and tier_1_blocked > blocked_threshold
                )
                else None
            ),
        ),
        LiveFreshnessSlo(
            name="tier_1_quarantined_events",
            actual=tier_1_quarantined,
            threshold=_LIVE_FRESHNESS_TIER_1_QUARANTINED_MAX,
            breached=(
                tier_1_quarantined is not None
                and tier_1_quarantined > _LIVE_FRESHNESS_TIER_1_QUARANTINED_MAX
            ),
            note=(
                "many tier_1 events in P5b quarantine — likely selective "
                "upstream throttling (Real Madrid-class). Check live-tier-1 "
                "worker logs for ErrCode 28 timeouts."
                if (
                    tier_1_quarantined is not None
                    and tier_1_quarantined > _LIVE_FRESHNESS_TIER_1_QUARANTINED_MAX
                )
                else None
            ),
        ),
    ]

    if any(slo.breached for slo in slos):
        status = "degraded"
    elif (
        oldest_age_seconds is None
        and success_rate is None
        and tier_1_blocked is None
    ):
        status = "unknown"
    else:
        status = "healthy"

    return LiveFreshnessSummary(
        status=status,
        slos=tuple(slos),
        oldest_hot_score_age_seconds=oldest_age_seconds,
        refresh_live_event_success_rate_5min=success_rate,
        tier_1_blocked_rate_cumulative=tier_1_blocked,
        tier_1_active_events=tier_1_active,
        tier_1_quarantined_events=tier_1_quarantined,
        refresh_live_event_succeeded_5min=counts_5min["succeeded"],
        refresh_live_event_retry_5min=counts_5min["retry_scheduled"],
        refresh_live_event_failed_5min=counts_5min["failed"],
    )


async def _fetch_oldest_hot_score_age_seconds(
    redis_backend, now: datetime
) -> int | None:
    """Return age in seconds of the oldest entry in ``zset:live:hot``.

    Returns 0 when the oldest score is in the future (event scheduled, not
    stale). Returns ``None`` when Redis is unavailable or the zset is empty.
    """

    if redis_backend is None:
        return None
    zrange = getattr(redis_backend, "zrange", None)
    if not callable(zrange):
        return None
    try:
        result = zrange("zset:live:hot", 0, 0, withscores=True)
    except Exception:
        return None
    if not result:
        return None
    try:
        member, score = result[0]
        del member
        score_ms = int(float(score))
    except (TypeError, ValueError, IndexError):
        return None
    now_ms = int(now.timestamp() * 1000)
    age_ms = now_ms - score_ms
    if age_ms <= 0:
        return 0
    return age_ms // 1000


# refresh_live_event 5-min count query: restored 2026-05-15 after BRIN
# index ``idx_etl_job_run_started_at_brin`` (migration 2026-05-14) makes
# the originally-30s full scan a ~10ms operation on prod (4.9M rows).
# Best-effort: any failure returns _EMPTY_REFRESH_COUNTS so the SLO
# simply does not breach (no false alarm on transient DB hiccup).
_EMPTY_REFRESH_COUNTS = {"succeeded": 0, "retry_scheduled": 0, "failed": 0}

# Task 2 (2026-05-15): default tier_1 quarantine SLO. P0(c) rollout on
# prod typically sees 0-3 events in quarantine; >10 indicates broad
# Sofascore upstream throttling (Real Madrid-class incident) and is
# worth a Telegram alert. Env-overridable in service_app via the
# monitoring config (config.py / SOFASCORE_MONITORING_TIER_1_QUARANTINED_*).
_LIVE_FRESHNESS_TIER_1_QUARANTINED_MAX = 10


async def _fetch_refresh_live_event_5min_counts(
    *,
    sql_executor: Any,
    now: datetime,
) -> dict[str, int]:
    """Count refresh_live_event jobs by status in the last 5 minutes.

    Uses the BRIN index on ``etl_job_run(started_at)`` so the scan is
    bounded to the recent BRIN block range even on a 4.9M-row table.
    """

    counts = dict(_EMPTY_REFRESH_COUNTS)
    try:
        rows = await sql_executor.fetch(
            """
            SELECT status, COUNT(*) AS c
            FROM etl_job_run
            WHERE started_at > $1
              AND job_type = 'refresh_live_event'
            GROUP BY status
            """,
            now - timedelta(minutes=5),
        )
    except Exception as exc:  # noqa: BLE001 — best-effort, never crash health
        logger.warning("refresh_live_event_5min counts probe failed: %r", exc)
        return counts
    for row in rows:
        try:
            status = str(row["status"])
            count_val = int(row["c"])
        except (KeyError, TypeError, ValueError):
            continue
        if status in counts:
            counts[status] = count_val
    return counts


async def _fetch_tier_1_quarantined_count(redis_backend: Any) -> int | None:
    """Count active tier_1 P5b quarantine markers via Redis SCAN.

    Pattern ``live:tier1_quarantine:{event_id}``. Returns None if SCAN
    is unavailable (Redis backend mismatch) so the SLO can distinguish
    "could not measure" from "0".
    """

    pattern = "live:tier1_quarantine:*"
    scan_iter = getattr(redis_backend, "scan_iter", None)
    if callable(scan_iter):
        try:
            return sum(1 for _ in scan_iter(match=pattern, count=200))
        except Exception as exc:  # noqa: BLE001 — best-effort
            logger.warning("tier_1_quarantine SCAN failed: %r", exc)
            return None
    keys = getattr(redis_backend, "keys", None)
    if callable(keys):
        try:
            result = keys(pattern)
            return len(list(result)) if result is not None else 0
        except Exception as exc:  # noqa: BLE001
            logger.warning("tier_1_quarantine KEYS fallback failed: %r", exc)
            return None
    return None


def _tier_1_blocked_rate(queue_summary: QueueSummary) -> float | None:
    """Compute tier_1 ``claim_failed_blocked`` / ``claim_attempts`` ratio.

    Returns cumulative ratio since the planner started (the metrics are
    HINCRBY-stored in Redis). Not a rolling window — surface it as such to
    operators via the SLO ``note`` field when breached.
    """

    metrics = queue_summary.live_dispatch_metrics or {}
    attempts = int(metrics.get("claim_attempts:tier_1") or 0)
    blocked = int(metrics.get("claim_failed_blocked:tier_1") or 0)
    if attempts <= 0:
        return None
    return min(1.0, blocked / attempts)


def _refresh_success_rate(counts: dict[str, int]) -> float | None:
    total = (
        int(counts.get("succeeded") or 0)
        + int(counts.get("retry_scheduled") or 0)
        + int(counts.get("failed") or 0)
    )
    if total <= 0:
        return None
    return int(counts.get("succeeded") or 0) / total


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


async def fetch_live_snapshot_repair_reasons(
    sql_executor,
    *,
    sport_slugs: tuple[str, ...],
    now: datetime | None = None,
) -> dict[str, str]:
    if not sport_slugs:
        return {}
    now_utc = now or datetime.now(timezone.utc)
    values_sql = ", ".join(f"('{_sql_literal(sport_slug)}')" for sport_slug in sport_slugs)
    rows = await sql_executor.fetch(
        f"""
        WITH target_sports(sport_slug) AS (
            VALUES {values_sql}
        ),
        latest_live_snapshot AS (
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
            -- 2026-05-15 hotfix: the previous OR-branch (`= '{{sport_slug}}'
            -- literal` OR `source_url LIKE '%X%'`) forced a Parallel Seq Scan
            -- on 4.8M rows and timed out via asyncpg, crashing
            -- live_discovery_planner in a restart loop (counter=891). Prod
            -- recon proved 100% of rows that match any of the three legacy
            -- predicates ALSO match `endpoint_pattern LIKE '...'`, so the
            -- other two predicates were redundant. With this simplification
            -- + idx_aps_endpoint_live_aggregate partial index, Postgres
            -- picks Index Only Scan (~110ms vs 4400ms).
            WHERE aps.endpoint_pattern LIKE '/api/v1/sport/%/events/live'
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
            target_sports.sport_slug AS sport_slug,
            live_snapshot.latest_fetched_at AS latest_fetched_at,
            terminal_state.latest_finalized_at AS latest_finalized_at
        FROM target_sports
        LEFT JOIN latest_live_snapshot AS live_snapshot
            ON live_snapshot.sport_slug = target_sports.sport_slug
        LEFT JOIN latest_terminal_state AS terminal_state
            ON terminal_state.sport_slug = target_sports.sport_slug
        ORDER BY target_sports.sport_slug
        """
    )
    repair_reasons: dict[str, str] = {}
    for row in rows:
        sport_slug = str(row["sport_slug"])
        latest_fetched_at = row.get("latest_fetched_at")
        latest_finalized_at = row.get("latest_finalized_at")
        allowed_lag_seconds = int(resolve_sport_profile(sport_slug).live_discovery_interval_seconds) + _LIVE_SNAPSHOT_TERMINAL_GRACE_SECONDS
        if not isinstance(latest_fetched_at, datetime):
            repair_reasons[sport_slug] = "snapshot_missing"
            continue
        snapshot_age_seconds = max(0, int((now_utc - latest_fetched_at).total_seconds()))
        terminal_lag_seconds = None
        if isinstance(latest_finalized_at, datetime):
            terminal_lag_seconds = max(0, int((latest_finalized_at - latest_fetched_at).total_seconds()))
        if terminal_lag_seconds is not None and latest_fetched_at < latest_finalized_at and terminal_lag_seconds > allowed_lag_seconds:
            repair_reasons[sport_slug] = "snapshot_older_than_terminal_state"
            continue
        if snapshot_age_seconds > allowed_lag_seconds:
            repair_reasons[sport_slug] = "snapshot_stale"
    return repair_reasons


def _sql_literal(value: str) -> str:
    return str(value).replace("'", "''")


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
