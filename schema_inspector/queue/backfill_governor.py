"""Backfill governor — task 5 (2026-05-15).

Live data freshness must never be starved by historical/structure
backfill. The pre-existing ``QueueBackpressure`` covers ONE failure
mode (downstream stream lag) but cannot see broader live-side issues
like P5b quarantine spikes or stale zset:live:hot scores. This module
adds a complementary governor that pauses backfill planners whenever
live signals breach their CRIT thresholds, regardless of the
downstream stream lag.

The governor implements the same ``blocking_reason() -> str | None``
contract as ``QueueBackpressure``, so it plugs into the existing
``Structure/Historical/HistoricalTournamentPlannerDaemon`` wiring
without further changes. A small ``CompositeBackpressure`` helper
combines multiple checks — the first one that returns non-None wins.

Signals checked:

1. ``oldest_hot_score_age_seconds`` from ``zset:live:hot``. When this
   crosses the CRIT threshold, sweeper / live workers are visibly
   falling behind — backfill must yield.

2. ``tier_1_quarantined_events`` from Redis SCAN on
   ``live:tier1_quarantine:*``. A high count means Sofascore is
   selectively throttling top-tier matches; we should not burn proxy
   budget on history while live tier_1 is starving.

Both thresholds are env-overridable via SOFASCORE_BACKFILL_GOVERNOR_*
on MonitoringConfig (re-used so we have ONE source of truth for live
thresholds; see ``schema_inspector/monitoring/config.py``).

The governor is best-effort: any Redis exception falls back to "not
blocked" (we do NOT want a transient Redis hiccup to grind history
to a halt while live is actually fine).
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

logger = logging.getLogger(__name__)


# Re-use the live-state hot zset key. Imported lazily to avoid a
# circular import: queue.live_state imports schema_inspector.queue
# package which imports this module via __init__.
LIVE_HOT_ZSET_KEY = "zset:live:hot"
TIER_1_QUARANTINE_KEY_PATTERN = "live:tier1_quarantine:*"


@dataclass(frozen=True)
class BackfillGovernorThresholds:
    """Threshold knobs for ``BackfillGovernor``.

    Defaults mirror the live-freshness SLO CRIT thresholds in
    docs/N1_MONITORING_PLAN.md so a single bad signal blocks
    backfill without any additional tuning.
    """

    oldest_hot_score_age_max_seconds: int = 1800
    tier_1_quarantined_max_events: int = 10


class BackfillGovernor:
    """Pause backfill planners when live-side signals breach CRIT.

    Implements ``blocking_reason()`` so it slots into the same
    interface as ``QueueBackpressure``. Checks run synchronously
    against Redis — each call costs O(1) ZRANGE + one SCAN over the
    tier_1 quarantine key namespace (~tens of keys at most on prod).
    """

    def __init__(
        self,
        *,
        redis_backend: Any,
        thresholds: BackfillGovernorThresholds | None = None,
        cache_ttl_seconds: float = 5.0,
        clock: Any = None,
    ) -> None:
        self.redis_backend = redis_backend
        self.thresholds = thresholds or BackfillGovernorThresholds()
        self.cache_ttl_seconds = float(cache_ttl_seconds)
        self._clock = clock or time.monotonic
        # Cache the last computed reason so a tight planner-loop does
        # not hammer Redis once per second. Refresh every ~5 seconds.
        self._cached_reason: str | None = None
        self._cached_at: float = -1.0

    def blocking_reason(self) -> str | None:
        """Return a human-readable block reason or None when OK."""

        now_mono = float(self._clock())
        if self._cached_at >= 0 and (now_mono - self._cached_at) < self.cache_ttl_seconds:
            return self._cached_reason
        reason = self._compute_blocking_reason()
        self._cached_reason = reason
        self._cached_at = now_mono
        return reason

    def _compute_blocking_reason(self) -> str | None:
        age = self._fetch_oldest_hot_score_age_seconds()
        if age is not None and age > self.thresholds.oldest_hot_score_age_max_seconds:
            return (
                f"oldest_hot_score_age={age}s>"
                f"{self.thresholds.oldest_hot_score_age_max_seconds}s"
            )
        quarantined = self._fetch_tier_1_quarantined_count()
        if quarantined is not None and quarantined > self.thresholds.tier_1_quarantined_max_events:
            return (
                f"tier_1_quarantined_events={quarantined}>"
                f"{self.thresholds.tier_1_quarantined_max_events}"
            )
        return None

    def _fetch_oldest_hot_score_age_seconds(self) -> int | None:
        try:
            backend = self.redis_backend
            zrange = getattr(backend, "zrange", None)
            if not callable(zrange):
                return None
            entries = zrange(LIVE_HOT_ZSET_KEY, 0, 0, withscores=True)
        except Exception as exc:  # noqa: BLE001 — defensive
            logger.debug("BackfillGovernor: ZRANGE failed: %r", exc)
            return None
        if not entries:
            return None
        try:
            entry = entries[0]
        except (IndexError, TypeError):
            return None
        # Accept both (member, score) tuples and parallel-flat lists.
        if isinstance(entry, tuple) and len(entry) >= 2:
            score = entry[1]
        elif isinstance(entry, list) and len(entry) >= 2:
            score = entry[1]
        else:
            # Some backends return just the member when withscores=False
            # was silently ignored; treat as "could not measure".
            return None
        try:
            score_ms = int(float(score))
        except (TypeError, ValueError):
            return None
        now_ms = int(time.time() * 1000)
        age_ms = now_ms - score_ms
        if age_ms <= 0:
            return 0
        return age_ms // 1000

    def _fetch_tier_1_quarantined_count(self) -> int | None:
        try:
            backend = self.redis_backend
            scan_iter = getattr(backend, "scan_iter", None)
            if callable(scan_iter):
                return sum(
                    1 for _ in scan_iter(match=TIER_1_QUARANTINE_KEY_PATTERN, count=200)
                )
            keys_method = getattr(backend, "keys", None)
            if callable(keys_method):
                result = keys_method(TIER_1_QUARANTINE_KEY_PATTERN)
                return len(list(result)) if result is not None else 0
        except Exception as exc:  # noqa: BLE001 — defensive
            logger.debug("BackfillGovernor: tier_1 quarantine probe failed: %r", exc)
            return None
        return None


class CompositeBackpressure:
    """Compose multiple ``blocking_reason()`` providers.

    Returns the first non-None reason. Order matters — put the
    cheaper / more-important check first. The planners only see one
    string, but with the source prefix the operator can tell which
    check fired (``BackfillGovernor:`` vs ``QueueBackpressure:``).
    """

    def __init__(self, checks: Sequence[Any]) -> None:
        self.checks = tuple(checks)

    def blocking_reason(self) -> str | None:
        for check in self.checks:
            if check is None:
                continue
            getter = getattr(check, "blocking_reason", None)
            if not callable(getter):
                continue
            try:
                reason = getter()
            except Exception as exc:  # noqa: BLE001 — defensive
                logger.debug("CompositeBackpressure: %s.blocking_reason() failed: %r", type(check).__name__, exc)
                continue
            if reason:
                return f"{type(check).__name__}: {reason}"
        return None
