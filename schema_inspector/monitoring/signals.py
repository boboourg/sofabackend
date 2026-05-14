"""Signal definitions, classification, and message formatting.

A ``Signal`` is a named numeric measurement with WARN/CRIT thresholds
and a direction. ``classify_signal`` is a pure function that returns one
of ``OK`` / ``WARN`` / ``CRIT`` given a value and thresholds. The daemon
combines those classifications with dedupe to decide whether to fire a
Telegram alert.

Directions:

* ``"max"`` — alert when value goes ABOVE the threshold
  (e.g. ``oldest_hot_score_age_seconds``: bigger is worse).
* ``"min"`` — alert when value goes BELOW the threshold
  (e.g. ``refresh_live_event_success_rate_5min``: smaller is worse).

``value=None`` means the signal could not be measured (e.g. the
underlying SQL query was disabled by P0.C). It is treated as ``OK`` —
we never alert on missing data. Operators investigate gaps through
other channels (log inspection, watchdog).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal


Severity = Literal["OK", "WARN", "CRIT"]
Direction = Literal["max", "min"]


@dataclass(frozen=True)
class SignalDefinition:
    """Static definition: name, thresholds, direction, units."""

    name: str
    direction: Direction
    threshold_warn: float | int
    threshold_crit: float | int
    unit: str = ""
    help: str = ""


@dataclass(frozen=True)
class SignalSnapshot:
    """One measurement of a signal at a point in time."""

    name: str
    value: float | int | None
    severity: Severity
    direction: Direction
    threshold_warn: float | int
    threshold_crit: float | int
    timestamp: datetime
    unit: str = ""
    note: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)


def classify_signal(
    *,
    value: float | int | None,
    threshold_warn: float | int,
    threshold_crit: float | int,
    direction: Direction,
) -> Severity:
    """Pure threshold classifier.

    Returns ``OK`` if value is None (no data) — we never alert on missing
    measurements; that's a separate concern handled by the watchdog.

    Order matters: CRIT is checked before WARN to handle thresholds
    where CRIT and WARN are equal (degenerate config — defaults to CRIT).
    """

    if value is None:
        return "OK"
    if direction == "max":
        if value > threshold_crit:
            return "CRIT"
        if value > threshold_warn:
            return "WARN"
        return "OK"
    # direction == "min"
    if value < threshold_crit:
        return "CRIT"
    if value < threshold_warn:
        return "WARN"
    return "OK"


# ---------------------------------------------------------------------------
# Static signal definitions (Phase 1: SLO signals from /ops/live-freshness).
#
# Defaults match docs/N1_MONITORING_PLAN.md. Real thresholds get tuned in
# Phase 4 ("Deploy + tune") via env vars on MonitoringConfig.
# ---------------------------------------------------------------------------

SIGNAL_OLDEST_HOT_AGE = SignalDefinition(
    name="oldest_hot_score_age_seconds",
    direction="max",
    threshold_warn=120,
    threshold_crit=300,
    unit="s",
    help=(
        "Age (seconds) of the oldest entry in zset:live:hot. Bigger = "
        "live polling lagging behind real time."
    ),
)

SIGNAL_TIER_1_BLOCKED = SignalDefinition(
    name="tier_1_blocked_rate_cumulative",
    direction="max",
    threshold_warn=0.20,
    threshold_crit=0.50,
    unit="ratio",
    help=(
        "Cumulative tier_1 claim_blocked / claim_attempts ratio. Bigger = "
        "tier_1 dispatch lease too long for current poll cadence."
    ),
)

SIGNAL_REFRESH_SUCCESS = SignalDefinition(
    name="refresh_live_event_success_rate_5min",
    direction="min",
    threshold_warn=0.95,
    threshold_crit=0.85,
    unit="ratio",
    help=(
        "Live refresh job success rate over the last 5 minutes. Smaller = "
        "live workers failing more than expected."
    ),
)

# Phase 2 — queue length signals. Per-stream XLEN. Direction "max":
# larger queues = workers falling behind. Defaults match
# docs/N1_MONITORING_PLAN.md and are env-overridable via MonitoringConfig.

SIGNAL_HYDRATE_XLEN = SignalDefinition(
    name="hydrate_xlen",
    direction="max",
    threshold_warn=1000,
    threshold_crit=5000,
    unit="",
    help="XLEN(stream:etl:hydrate). Bigger = hydrate workers can't keep up.",
)
SIGNAL_LIVE_HOT_XLEN = SignalDefinition(
    name="live_hot_xlen",
    direction="max",
    threshold_warn=500,
    threshold_crit=2000,
    unit="",
    help="XLEN(stream:etl:live_hot). Bigger = live-hot workers can't keep up.",
)
SIGNAL_LIVE_WARM_XLEN = SignalDefinition(
    name="live_warm_xlen",
    direction="max",
    threshold_warn=500,
    threshold_crit=2000,
    unit="",
    help="XLEN(stream:etl:live_warm). Bigger = live-warm workers can't keep up.",
)
SIGNAL_LIVE_DISCOVERY_XLEN = SignalDefinition(
    name="live_discovery_xlen",
    direction="max",
    threshold_warn=200,
    threshold_crit=1000,
    unit="",
    help="XLEN(stream:etl:live_discovery). Bigger = planner outpaces discovery.",
)
SIGNAL_DISCOVERY_XLEN = SignalDefinition(
    name="discovery_xlen",
    direction="max",
    threshold_warn=200,
    threshold_crit=1000,
    unit="",
    help="XLEN(stream:etl:discovery). Bigger = scheduled-planner outpaces discovery.",
)


# Phase 3 — job signals. Activated only after the
# 2026-05-14_etl_job_run_started_at_index.sql migration lands on prod, see
# docs/N1_MONITORING_PLAN.md "Phase 3" + "Risks".

SIGNAL_FAILED_JOBS_15MIN = SignalDefinition(
    name="failed_jobs_15min",
    direction="max",
    threshold_warn=20,
    threshold_crit=50,
    unit="",
    help=(
        "Count of etl_job_run rows with status='failed' over the last 15 "
        "minutes. Bigger = more workers blowing up than usual."
    ),
)
SIGNAL_RETRY_RATE_15MIN = SignalDefinition(
    name="retry_rate_15min",
    direction="max",
    threshold_warn=0.02,
    threshold_crit=0.05,
    unit="ratio",
    help=(
        "Share of etl_job_run rows in the last 15 minutes that ended in "
        "retry_scheduled. Baseline 0.32%% — 2%% warn, 5%% crit per "
        "ARCHITECTURE_AUDIT.md."
    ),
)
SIGNAL_NO_RECENT_JOBS_AGE = SignalDefinition(
    name="no_recent_jobs_age_seconds",
    direction="max",
    threshold_warn=300,
    threshold_crit=600,
    unit="s",
    help=(
        "Age (seconds) of the most recent etl_job_run.started_at. Bigger "
        "= planners stopped scheduling work — a silent failure mode."
    ),
)


SIGNAL_DEFINITIONS = {
    SIGNAL_OLDEST_HOT_AGE.name: SIGNAL_OLDEST_HOT_AGE,
    SIGNAL_TIER_1_BLOCKED.name: SIGNAL_TIER_1_BLOCKED,
    SIGNAL_REFRESH_SUCCESS.name: SIGNAL_REFRESH_SUCCESS,
    SIGNAL_HYDRATE_XLEN.name: SIGNAL_HYDRATE_XLEN,
    SIGNAL_LIVE_HOT_XLEN.name: SIGNAL_LIVE_HOT_XLEN,
    SIGNAL_LIVE_WARM_XLEN.name: SIGNAL_LIVE_WARM_XLEN,
    SIGNAL_LIVE_DISCOVERY_XLEN.name: SIGNAL_LIVE_DISCOVERY_XLEN,
    SIGNAL_DISCOVERY_XLEN.name: SIGNAL_DISCOVERY_XLEN,
    SIGNAL_FAILED_JOBS_15MIN.name: SIGNAL_FAILED_JOBS_15MIN,
    SIGNAL_RETRY_RATE_15MIN.name: SIGNAL_RETRY_RATE_15MIN,
    SIGNAL_NO_RECENT_JOBS_AGE.name: SIGNAL_NO_RECENT_JOBS_AGE,
}


def make_snapshot(
    *,
    definition: SignalDefinition,
    value: float | int | None,
    timestamp: datetime | None = None,
    threshold_warn: float | int | None = None,
    threshold_crit: float | int | None = None,
    note: str | None = None,
    extra: dict[str, Any] | None = None,
) -> SignalSnapshot:
    """Build a ``SignalSnapshot`` and classify it in one call.

    Allows the caller to override thresholds (e.g. from MonitoringConfig
    env vars) without redefining the static SignalDefinition.
    """

    warn = definition.threshold_warn if threshold_warn is None else threshold_warn
    crit = definition.threshold_crit if threshold_crit is None else threshold_crit
    severity = classify_signal(
        value=value,
        threshold_warn=warn,
        threshold_crit=crit,
        direction=definition.direction,
    )
    return SignalSnapshot(
        name=definition.name,
        value=value,
        severity=severity,
        direction=definition.direction,
        threshold_warn=warn,
        threshold_crit=crit,
        timestamp=timestamp or datetime.now(timezone.utc),
        unit=definition.unit,
        note=note,
        extra=dict(extra or {}),
    )


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

_SEVERITY_PREFIX = {
    "OK": "OK",
    "WARN": "WARN",
    "CRIT": "CRIT",
}


def format_alert_message(
    snapshot: SignalSnapshot,
    *,
    host_label: str = "sofascore",
    resolved: bool = False,
    repeat_count: int = 0,
    first_alerted_at: datetime | None = None,
) -> str:
    """Render a Telegram-friendly message for one signal snapshot.

    Plain text only (no Markdown) — avoids `*`/`_` parsing accidents when
    a note happens to contain them.
    """

    if resolved:
        return (
            f"RESOLVED: {snapshot.name}\n"
            f"Value: {_format_value(snapshot.value, snapshot.unit)}\n"
            f"Time: {snapshot.timestamp.isoformat()}\n"
            f"Host: {host_label}"
        )
    prefix = _SEVERITY_PREFIX.get(snapshot.severity, snapshot.severity)
    if snapshot.severity == "CRIT":
        threshold = snapshot.threshold_crit
        prefix = "[CRIT]"
    elif snapshot.severity == "WARN":
        threshold = snapshot.threshold_warn
        prefix = "[WARN]"
    else:
        threshold = snapshot.threshold_warn
        prefix = f"[{snapshot.severity}]"
    lines = [
        f"{prefix} {snapshot.name}",
        f"Value: {_format_value(snapshot.value, snapshot.unit)} "
        f"(threshold: {_format_value(threshold, snapshot.unit)})",
        f"Time: {snapshot.timestamp.isoformat()}",
        f"Host: {host_label}",
    ]
    if snapshot.note:
        lines.append(f"Note: {snapshot.note}")
    if repeat_count > 1 and first_alerted_at is not None:
        lines.append(
            f"Repeat #{repeat_count} (first at {first_alerted_at.isoformat()})"
        )
    if snapshot.extra:
        for key in sorted(snapshot.extra):
            lines.append(f"{key}: {snapshot.extra[key]}")
    return "\n".join(lines)


def _format_value(value: float | int | None, unit: str) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        # Render ratios with 3-digit precision; raw seconds keep one decimal.
        if unit == "ratio":
            return f"{value:.3f}"
        return f"{value:.1f}{unit}" if unit else f"{value:.1f}"
    return f"{value}{unit}" if unit else f"{value}"
