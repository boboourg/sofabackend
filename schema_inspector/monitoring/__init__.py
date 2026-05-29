"""Monitoring daemon (N1, 2026-05-14).

Polls /ops/* endpoints, classifies signals against thresholds, and sends
deduplicated Telegram alerts. See docs/N1_MONITORING_PLAN.md for the
contract and roadmap.

Public surface:

* ``MonitoringConfig`` — env-backed configuration object.
* ``MonitoringDaemon`` — orchestrator with ``run_forever``.
* ``SignalSnapshot`` — single observation of a signal.
* ``classify_signal`` — pure threshold comparison.
* ``TelegramAlertSink`` — HTTP POST sink for Telegram Bot API.
* ``RedisDedupeStore`` — Redis-backed alert dedupe with TTL.
* ``fetch_slo_signals_from_api`` — pull SLO snapshots from
  ``/ops/live-freshness``.

Phase 1 covers only the SLO signals from ``/ops/live-freshness``.
Phase 2 adds queue signals (XLEN per stream) from
``/ops/queues/summary``. Phase 3 adds job signals once
``etl_job_run.started_at`` is indexed.
"""

from __future__ import annotations

from .alerter import AlertSink, NullAlertSink, TelegramAlertSink
from .config import MonitoringConfig
from .daemon import MonitoringDaemon
from .dedupe import DedupeStore, NullDedupeStore, RedisDedupeStore
from .signal_source import (
    fetch_all_signals_from_api,
    fetch_disk_signals,
    fetch_queue_signals_from_api,
    fetch_slo_signals_from_api,
)
from .signals import (
    SIGNAL_DEFINITIONS,
    SignalDefinition,
    SignalSnapshot,
    classify_signal,
    format_alert_message,
)

__all__ = [
    "AlertSink",
    "DedupeStore",
    "MonitoringConfig",
    "MonitoringDaemon",
    "NullAlertSink",
    "NullDedupeStore",
    "RedisDedupeStore",
    "SIGNAL_DEFINITIONS",
    "SignalDefinition",
    "SignalSnapshot",
    "TelegramAlertSink",
    "classify_signal",
    "fetch_all_signals_from_api",
    "fetch_disk_signals",
    "fetch_queue_signals_from_api",
    "fetch_slo_signals_from_api",
    "format_alert_message",
]
