"""Env-backed monitoring daemon configuration."""

from __future__ import annotations

import os
import socket
from dataclasses import dataclass


def _env_int(env: dict[str, str], name: str, default: int) -> int:
    value = env.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _env_float(env: dict[str, str], name: str, default: float) -> float:
    value = env.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _env_str(env: dict[str, str], name: str, default: str | None = None) -> str | None:
    value = env.get(name)
    if value is None:
        return default
    value = str(value).strip()
    return value or default


def _env_bool(env: dict[str, str], name: str, default: bool) -> bool:
    value = env.get(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class MonitoringConfig:
    """All env-tunable knobs of the monitoring daemon, frozen.

    Defaults are intentionally conservative: 60s interval, 10 min WARN
    dedupe, 30 min CRIT dedupe. Operators can override per-knob via
    ``SOFASCORE_MONITORING_*`` env vars. See docs/N1_MONITORING_PLAN.md.
    """

    enabled: bool = True
    base_url: str = "http://127.0.0.1:8000"
    interval_seconds: float = 60.0
    warn_ttl_seconds: int = 600  # 10 minutes
    crit_ttl_seconds: int = 1800  # 30 minutes
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    telegram_timeout_seconds: float = 10.0
    host_label: str = "sofascore"
    # Phase 0 (2026-05-30): 5s was too tight — /ops/jobs/runs (and other ops
    # aggregates) overran it, so the job/SLO signals never reached Telegram.
    # Paired with the 2h-bounded /ops/jobs/runs query; 15s is the safety margin.
    http_request_timeout_seconds: float = 15.0
    # Thresholds for SLO signals — match the contract in
    # docs/N1_MONITORING_PLAN.md §"P0 — SLO signals". All env-overridable.
    oldest_hot_age_warn_seconds: int = 120
    oldest_hot_age_crit_seconds: int = 300
    tier_1_blocked_warn_rate: float = 0.20
    tier_1_blocked_crit_rate: float = 0.50
    refresh_success_warn_rate: float = 0.95
    refresh_success_crit_rate: float = 0.85
    # Task 2 (2026-05-15): tier_1 P5b quarantine signal thresholds.
    # Baseline on prod 0-3 events; >5 worth a WARN, >10 worth CRIT.
    tier_1_quarantined_warn: int = 5
    tier_1_quarantined_crit: int = 10
    # Phase 2 (queue signals): alert on consumer lag, not XLEN. XLEN is
    # stream memory/trim pressure and can be huge even when consumers are
    # caught up, so it is carried only as alert context.
    hydrate_lag_warn: int = 800
    hydrate_lag_crit: int = 1500
    live_hot_lag_warn: int = 200
    live_hot_lag_crit: int = 500
    live_warm_lag_warn: int = 5000
    live_warm_lag_crit: int = 20000
    live_discovery_lag_warn: int = 50
    live_discovery_lag_crit: int = 200
    discovery_lag_warn: int = 200
    discovery_lag_crit: int = 1000
    # Phase 3 — job signals (default OFF until BRIN index lands on prod;
    # see migrations/2026-05-14_etl_job_run_started_at_index.sql).
    job_signals_enabled: bool = False
    failed_jobs_warn: int = 20
    failed_jobs_crit: int = 50
    retry_rate_warn: float = 0.02
    retry_rate_crit: float = 0.05
    no_recent_jobs_warn_seconds: int = 300
    no_recent_jobs_crit_seconds: int = 600
    # Phase 0 (2026-05-26 incident): Postgres data-mount free-space alert.
    # Default mount path is the conventional PGDATA location; override per-host
    # via SOFASCORE_MONITORING_DISK_MOUNT_PATH. Thresholds in GB: WARN <20,
    # CRIT <10. Set the path to "" to disable the signal.
    disk_mount_path: str = "/var/lib/postgresql"
    disk_free_warn_gb: float = 20.0
    disk_free_crit_gb: float = 10.0
    # Synthetic watchdog (2026-05-30): fire a CRIT when every signal source
    # returns empty/errors for this many consecutive ticks. signal_source.py
    # returns [] on any fetch error and the daemon treats [] as "no data ->
    # no alert", so without this the monitor goes silent during a full
    # outage. Default 3 ticks (~3 min at 60s interval) debounces a single
    # transient /ops/* blip while still catching a real incident fast. <=0
    # disables the watchdog.
    blind_alert_consecutive_ticks: int = 3

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "MonitoringConfig":
        resolved = dict(os.environ) if env is None else dict(env)
        return cls(
            enabled=_env_bool(resolved, "SOFASCORE_MONITORING_ENABLED", True),
            base_url=_env_str(
                resolved, "SOFASCORE_MONITORING_BASE_URL", "http://127.0.0.1:8000"
            )
            or "http://127.0.0.1:8000",
            interval_seconds=_env_float(
                resolved, "SOFASCORE_MONITORING_INTERVAL_SECONDS", 60.0
            ),
            warn_ttl_seconds=_env_int(
                resolved, "SOFASCORE_MONITORING_DEDUPE_WARN_TTL_SECONDS", 600
            ),
            crit_ttl_seconds=_env_int(
                resolved, "SOFASCORE_MONITORING_DEDUPE_CRIT_TTL_SECONDS", 1800
            ),
            telegram_bot_token=_env_str(
                resolved, "SOFASCORE_MONITORING_TELEGRAM_BOT_TOKEN"
            ),
            telegram_chat_id=_env_str(
                resolved, "SOFASCORE_MONITORING_TELEGRAM_CHAT_ID"
            ),
            telegram_timeout_seconds=_env_float(
                resolved, "SOFASCORE_MONITORING_TELEGRAM_TIMEOUT_SECONDS", 10.0
            ),
            host_label=_env_str(
                resolved, "SOFASCORE_MONITORING_HOST_LABEL", socket.gethostname()
            )
            or "sofascore",
            http_request_timeout_seconds=_env_float(
                resolved, "SOFASCORE_MONITORING_HTTP_TIMEOUT_SECONDS", 15.0
            ),
            oldest_hot_age_warn_seconds=_env_int(
                resolved, "SOFASCORE_MONITORING_OLDEST_HOT_AGE_WARN_SECONDS", 120
            ),
            oldest_hot_age_crit_seconds=_env_int(
                resolved, "SOFASCORE_MONITORING_OLDEST_HOT_AGE_CRIT_SECONDS", 300
            ),
            tier_1_blocked_warn_rate=_env_float(
                resolved, "SOFASCORE_MONITORING_TIER_1_BLOCKED_WARN_RATE", 0.20
            ),
            tier_1_blocked_crit_rate=_env_float(
                resolved, "SOFASCORE_MONITORING_TIER_1_BLOCKED_CRIT_RATE", 0.50
            ),
            refresh_success_warn_rate=_env_float(
                resolved, "SOFASCORE_MONITORING_REFRESH_SUCCESS_WARN_RATE", 0.95
            ),
            refresh_success_crit_rate=_env_float(
                resolved, "SOFASCORE_MONITORING_REFRESH_SUCCESS_CRIT_RATE", 0.85
            ),
            tier_1_quarantined_warn=_env_int(
                resolved, "SOFASCORE_MONITORING_TIER_1_QUARANTINED_WARN", 5
            ),
            tier_1_quarantined_crit=_env_int(
                resolved, "SOFASCORE_MONITORING_TIER_1_QUARANTINED_CRIT", 10
            ),
            hydrate_lag_warn=_env_int(
                resolved, "SOFASCORE_MONITORING_HYDRATE_LAG_WARN", 800
            ),
            hydrate_lag_crit=_env_int(
                resolved, "SOFASCORE_MONITORING_HYDRATE_LAG_CRIT", 1500
            ),
            live_hot_lag_warn=_env_int(
                resolved, "SOFASCORE_MONITORING_LIVE_HOT_LAG_WARN", 200
            ),
            live_hot_lag_crit=_env_int(
                resolved, "SOFASCORE_MONITORING_LIVE_HOT_LAG_CRIT", 500
            ),
            live_warm_lag_warn=_env_int(
                resolved, "SOFASCORE_MONITORING_LIVE_WARM_LAG_WARN", 5000
            ),
            live_warm_lag_crit=_env_int(
                resolved, "SOFASCORE_MONITORING_LIVE_WARM_LAG_CRIT", 20000
            ),
            live_discovery_lag_warn=_env_int(
                resolved, "SOFASCORE_MONITORING_LIVE_DISCOVERY_LAG_WARN", 50
            ),
            live_discovery_lag_crit=_env_int(
                resolved, "SOFASCORE_MONITORING_LIVE_DISCOVERY_LAG_CRIT", 200
            ),
            discovery_lag_warn=_env_int(
                resolved, "SOFASCORE_MONITORING_DISCOVERY_LAG_WARN", 200
            ),
            discovery_lag_crit=_env_int(
                resolved, "SOFASCORE_MONITORING_DISCOVERY_LAG_CRIT", 1000
            ),
            job_signals_enabled=_env_bool(
                resolved, "SOFASCORE_MONITORING_JOB_SIGNALS_ENABLED", False
            ),
            failed_jobs_warn=_env_int(
                resolved, "SOFASCORE_MONITORING_FAILED_JOBS_WARN", 20
            ),
            failed_jobs_crit=_env_int(
                resolved, "SOFASCORE_MONITORING_FAILED_JOBS_CRIT", 50
            ),
            retry_rate_warn=_env_float(
                resolved, "SOFASCORE_MONITORING_RETRY_RATE_WARN", 0.02
            ),
            retry_rate_crit=_env_float(
                resolved, "SOFASCORE_MONITORING_RETRY_RATE_CRIT", 0.05
            ),
            no_recent_jobs_warn_seconds=_env_int(
                resolved, "SOFASCORE_MONITORING_NO_RECENT_JOBS_WARN_SECONDS", 300
            ),
            no_recent_jobs_crit_seconds=_env_int(
                resolved, "SOFASCORE_MONITORING_NO_RECENT_JOBS_CRIT_SECONDS", 600
            ),
            disk_mount_path=_env_str(
                resolved,
                "SOFASCORE_MONITORING_DISK_MOUNT_PATH",
                "/var/lib/postgresql",
            )
            or "",
            disk_free_warn_gb=_env_float(
                resolved, "SOFASCORE_MONITORING_DISK_FREE_WARN_GB", 20.0
            ),
            disk_free_crit_gb=_env_float(
                resolved, "SOFASCORE_MONITORING_DISK_FREE_CRIT_GB", 10.0
            ),
            blind_alert_consecutive_ticks=_env_int(
                resolved,
                "SOFASCORE_MONITORING_BLIND_ALERT_CONSECUTIVE_TICKS",
                3,
            ),
        )

    def has_telegram(self) -> bool:
        return bool(self.telegram_bot_token) and bool(self.telegram_chat_id)
