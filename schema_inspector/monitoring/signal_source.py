"""Pull SignalSnapshots from local API ``/ops/*`` endpoints.

Phase 1: only ``/ops/live-freshness``. Returns the three SLO signals
defined in ``signals.py``. Phase 2 will add a queue signals source over
``/ops/queues/summary``.

The function is pure-ish: given an injected ``http_client`` and clock, it
emits snapshots without touching any module-level state. This keeps it
trivially testable — see :mod:`tests.test_monitoring_signal_source`.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from .signals import (
    SIGNAL_DISCOVERY_XLEN,
    SIGNAL_FAILED_JOBS_15MIN,
    SIGNAL_HYDRATE_XLEN,
    SIGNAL_LIVE_DISCOVERY_XLEN,
    SIGNAL_LIVE_HOT_XLEN,
    SIGNAL_LIVE_WARM_XLEN,
    SIGNAL_NO_RECENT_JOBS_AGE,
    SIGNAL_OLDEST_HOT_AGE,
    SIGNAL_REFRESH_SUCCESS,
    SIGNAL_RETRY_RATE_15MIN,
    SIGNAL_TIER_1_BLOCKED,
    SignalDefinition,
    SignalSnapshot,
    make_snapshot,
)


logger = logging.getLogger(__name__)


async def fetch_slo_signals_from_api(
    *,
    base_url: str,
    http_client: Any,
    timeout_seconds: float,
    now: datetime | None = None,
    overrides: dict[str, dict[str, float | int]] | None = None,
) -> list[SignalSnapshot]:
    """Fetch /ops/live-freshness and return three SLO snapshots.

    ``overrides`` maps signal name → ``{"warn": ..., "crit": ...}`` so
    callers can pass in env-configured thresholds without rebuilding the
    static SignalDefinition. Missing overrides fall back to the default
    thresholds embedded in ``signals.py``.

    On any HTTP / parse error a single ``SignalSnapshot`` with severity
    ``OK`` and value ``None`` is *not* fabricated — instead an empty list
    is returned. The daemon's tick loop interprets an empty result as
    "this tick yielded no data" and continues without alerting.
    """

    url = base_url.rstrip("/") + "/ops/live-freshness"
    payload: dict[str, Any]
    try:
        response = await http_client.get(url, timeout=timeout_seconds)
        if response.status_code != 200:
            logger.warning(
                "fetch_slo_signals: non-200 from %s status=%d",
                url,
                response.status_code,
            )
            return []
        payload = response.json()
    except Exception as exc:  # noqa: BLE001 — every error path must be logged
        logger.warning("fetch_slo_signals: fetch failed url=%s err=%r", url, exc)
        return []

    timestamp = now or datetime.now(timezone.utc)
    overrides = overrides or {}

    snapshots: list[SignalSnapshot] = []

    oldest_age = _coerce_int(payload.get("oldest_hot_score_age_seconds"))
    ov = overrides.get(SIGNAL_OLDEST_HOT_AGE.name, {})
    snapshots.append(
        make_snapshot(
            definition=SIGNAL_OLDEST_HOT_AGE,
            value=oldest_age,
            timestamp=timestamp,
            threshold_warn=ov.get("warn"),
            threshold_crit=ov.get("crit"),
            note=_pick_slo_note(payload, SIGNAL_OLDEST_HOT_AGE.name),
        )
    )

    tier_1_blocked = _coerce_float(payload.get("tier_1_blocked_rate_cumulative"))
    tier_1_active = _coerce_int(payload.get("tier_1_active_events")) or 0
    ov = overrides.get(SIGNAL_TIER_1_BLOCKED.name, {})
    snapshots.append(
        make_snapshot(
            definition=SIGNAL_TIER_1_BLOCKED,
            # Only meaningful when tier_1 is actively polling. With zero
            # events the cumulative ratio is a stale snapshot from the last
            # planner restart and not actionable.
            value=tier_1_blocked if tier_1_active > 0 else None,
            timestamp=timestamp,
            threshold_warn=ov.get("warn"),
            threshold_crit=ov.get("crit"),
            note=_pick_slo_note(payload, SIGNAL_TIER_1_BLOCKED.name),
            extra={"tier_1_active_events": tier_1_active},
        )
    )

    success_rate = _coerce_float(payload.get("refresh_live_event_success_rate_5min"))
    ov = overrides.get(SIGNAL_REFRESH_SUCCESS.name, {})
    snapshots.append(
        make_snapshot(
            definition=SIGNAL_REFRESH_SUCCESS,
            value=success_rate,
            timestamp=timestamp,
            threshold_warn=ov.get("warn"),
            threshold_crit=ov.get("crit"),
            note=_pick_slo_note(payload, SIGNAL_REFRESH_SUCCESS.name),
        )
    )

    return snapshots


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _pick_slo_note(payload: dict[str, Any], slo_name: str) -> str | None:
    """Extract the ``note`` field from /ops/live-freshness for one SLO."""

    slos = payload.get("slos")
    if not isinstance(slos, list):
        return None
    for entry in slos:
        if not isinstance(entry, dict):
            continue
        if str(entry.get("name") or "") != slo_name:
            continue
        note = entry.get("note")
        if not note:
            return None
        return str(note)
    return None


# ---------------------------------------------------------------------------
# Phase 2 — queue signals from /ops/queues/summary.
# ---------------------------------------------------------------------------


# Maps Redis stream name → (SignalDefinition, default warn/crit thresholds
# already encoded in SignalDefinition). When the caller passes overrides
# they keyed by SignalDefinition.name.
_QUEUE_STREAM_TO_DEFINITION: dict[str, SignalDefinition] = {
    "stream:etl:hydrate": SIGNAL_HYDRATE_XLEN,
    "stream:etl:live_hot": SIGNAL_LIVE_HOT_XLEN,
    "stream:etl:live_warm": SIGNAL_LIVE_WARM_XLEN,
    "stream:etl:live_discovery": SIGNAL_LIVE_DISCOVERY_XLEN,
    "stream:etl:discovery": SIGNAL_DISCOVERY_XLEN,
}


async def fetch_queue_signals_from_api(
    *,
    base_url: str,
    http_client: Any,
    timeout_seconds: float,
    now: datetime | None = None,
    overrides: dict[str, dict[str, float | int]] | None = None,
) -> list[SignalSnapshot]:
    """Fetch /ops/queues/summary and emit queue XLEN snapshots.

    The endpoint returns ``{"streams": [{"stream": "...", "length": N,
    "pending_total": M, ...}, ...]}``. For each stream we care about
    (see ``_QUEUE_STREAM_TO_DEFINITION``) we emit a snapshot of its
    length. Other streams are ignored — they don't have configured
    thresholds and would generate noise without action.

    Best-effort: any HTTP / parse error returns an empty list. The
    daemon's tick loop treats empty as "no data this tick".
    """

    url = base_url.rstrip("/") + "/ops/queues/summary"
    payload: dict[str, Any]
    try:
        response = await http_client.get(url, timeout=timeout_seconds)
        if response.status_code != 200:
            logger.warning(
                "fetch_queue_signals: non-200 from %s status=%d",
                url,
                response.status_code,
            )
            return []
        payload = response.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("fetch_queue_signals: fetch failed url=%s err=%r", url, exc)
        return []

    timestamp = now or datetime.now(timezone.utc)
    overrides = overrides or {}
    streams = payload.get("streams")
    if not isinstance(streams, list):
        return []

    # Sum streams that share a stream name (e.g. two consumer groups on
    # the same stream produce two entries). length is a property of the
    # stream itself, not the group, so duplicates are normal — take max.
    length_by_stream: dict[str, int] = {}
    for entry in streams:
        if not isinstance(entry, dict):
            continue
        stream_name = str(entry.get("stream") or "")
        if stream_name not in _QUEUE_STREAM_TO_DEFINITION:
            continue
        try:
            length = int(entry.get("length") or 0)
        except (TypeError, ValueError):
            continue
        length_by_stream[stream_name] = max(
            length_by_stream.get(stream_name, 0), length
        )

    snapshots: list[SignalSnapshot] = []
    for stream_name, definition in _QUEUE_STREAM_TO_DEFINITION.items():
        length = length_by_stream.get(stream_name)
        ov = overrides.get(definition.name, {})
        snapshots.append(
            make_snapshot(
                definition=definition,
                value=length,
                timestamp=timestamp,
                threshold_warn=ov.get("warn"),
                threshold_crit=ov.get("crit"),
                extra={"stream": stream_name},
            )
        )
    return snapshots


# ---------------------------------------------------------------------------
# Phase 3 — job signals from /ops/jobs/runs.
#
# Activated only when the etl_job_run.started_at BRIN index is present in
# the database. The signal source itself is index-tolerant (it works
# without the index, just slowly) — gating is purely about not running
# 30s queries on every monitoring tick.
# ---------------------------------------------------------------------------


async def fetch_job_signals_from_api(
    *,
    base_url: str,
    http_client: Any,
    timeout_seconds: float,
    now: datetime | None = None,
    overrides: dict[str, dict[str, float | int]] | None = None,
    window_seconds: int = 900,  # 15 minutes
    fetch_limit: int = 200,
) -> list[SignalSnapshot]:
    """Fetch /ops/jobs/runs and emit three job-health snapshots.

    Computes locally (no extra DB roundtrips):

    * ``failed_jobs_15min`` — rows in the latest ``fetch_limit`` whose
      ``status == 'failed'`` and ``started_at`` falls in the last
      ``window_seconds`` seconds. Direction "max": more failures = worse.
    * ``retry_rate_15min`` — share of rows in the same window with status
      ``'retry_scheduled'``. Direction "max": higher retry rate = worse.
    * ``no_recent_jobs_age_seconds`` — age of the most recent
      ``started_at``. Direction "max": no jobs = silent planner failure.

    Returns ``[]`` on HTTP / parse error so the daemon's tick loop simply
    treats this signal source as "no data this tick".
    """

    url = base_url.rstrip("/") + f"/ops/jobs/runs?limit={int(fetch_limit)}"
    payload: dict[str, Any]
    try:
        response = await http_client.get(url, timeout=timeout_seconds)
        if response.status_code != 200:
            logger.warning(
                "fetch_job_signals: non-200 from %s status=%d",
                url,
                response.status_code,
            )
            return []
        payload = response.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("fetch_job_signals: fetch failed url=%s err=%r", url, exc)
        return []

    timestamp = now or datetime.now(timezone.utc)
    overrides = overrides or {}

    job_runs = payload.get("jobRuns")
    if not isinstance(job_runs, list):
        job_runs = []

    failed = 0
    retried = 0
    in_window_total = 0
    most_recent_started_at: datetime | None = None
    cutoff_epoch = timestamp.timestamp() - float(window_seconds)

    for entry in job_runs:
        if not isinstance(entry, dict):
            continue
        started_at_raw = entry.get("started_at")
        started_at = _parse_iso_timestamp(started_at_raw)
        if started_at is None:
            continue
        if most_recent_started_at is None or started_at > most_recent_started_at:
            most_recent_started_at = started_at
        if started_at.timestamp() < cutoff_epoch:
            continue
        in_window_total += 1
        status = str(entry.get("status") or "").lower()
        if status == "failed":
            failed += 1
        elif status == "retry_scheduled":
            retried += 1

    retry_rate: float | None
    if in_window_total > 0:
        retry_rate = retried / in_window_total
    else:
        retry_rate = None

    if most_recent_started_at is None:
        no_jobs_age_seconds: int | None = None
    else:
        delta = (timestamp - most_recent_started_at).total_seconds()
        no_jobs_age_seconds = int(delta) if delta > 0 else 0

    snapshots: list[SignalSnapshot] = []
    ov = overrides.get(SIGNAL_FAILED_JOBS_15MIN.name, {})
    snapshots.append(
        make_snapshot(
            definition=SIGNAL_FAILED_JOBS_15MIN,
            value=failed,
            timestamp=timestamp,
            threshold_warn=ov.get("warn"),
            threshold_crit=ov.get("crit"),
            extra={"window_seconds": window_seconds, "sampled_rows": in_window_total},
        )
    )
    ov = overrides.get(SIGNAL_RETRY_RATE_15MIN.name, {})
    snapshots.append(
        make_snapshot(
            definition=SIGNAL_RETRY_RATE_15MIN,
            value=retry_rate,
            timestamp=timestamp,
            threshold_warn=ov.get("warn"),
            threshold_crit=ov.get("crit"),
            extra={
                "window_seconds": window_seconds,
                "retried": retried,
                "total": in_window_total,
            },
        )
    )
    ov = overrides.get(SIGNAL_NO_RECENT_JOBS_AGE.name, {})
    snapshots.append(
        make_snapshot(
            definition=SIGNAL_NO_RECENT_JOBS_AGE,
            value=no_jobs_age_seconds,
            timestamp=timestamp,
            threshold_warn=ov.get("warn"),
            threshold_crit=ov.get("crit"),
        )
    )
    return snapshots


def _parse_iso_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        # Coerce naive datetimes (no tzinfo) to UTC.
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if not isinstance(value, str):
        return None
    # Postgres ISO-8601 output: "2026-05-14T18:30:00+00:00" or with
    # microseconds. ``fromisoformat`` handles both.
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        # Some serializers append "Z" — emulate that.
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None


async def fetch_all_signals_from_api(
    *,
    base_url: str,
    http_client: Any,
    timeout_seconds: float,
    now: datetime | None = None,
    overrides: dict[str, dict[str, float | int]] | None = None,
    include_job_signals: bool = False,
) -> list[SignalSnapshot]:
    """Compose SLO + queue (+ optional job) signals.

    Failures in any branch don't poison the others — each branch returns
    ``[]`` independently. Job signals are off by default so the daemon
    can ship before the BRIN index migration lands on prod; flip
    ``include_job_signals=True`` (or set
    ``SOFASCORE_MONITORING_JOB_SIGNALS_ENABLED=1``) afterwards.
    """

    slo = await fetch_slo_signals_from_api(
        base_url=base_url,
        http_client=http_client,
        timeout_seconds=timeout_seconds,
        now=now,
        overrides=overrides,
    )
    queue = await fetch_queue_signals_from_api(
        base_url=base_url,
        http_client=http_client,
        timeout_seconds=timeout_seconds,
        now=now,
        overrides=overrides,
    )
    combined: list[SignalSnapshot] = list(slo) + list(queue)
    if include_job_signals:
        jobs = await fetch_job_signals_from_api(
            base_url=base_url,
            http_client=http_client,
            timeout_seconds=timeout_seconds,
            now=now,
            overrides=overrides,
        )
        combined.extend(jobs)
    return combined
