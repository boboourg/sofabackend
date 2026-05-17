"""Backfill governor — task 5 (2026-05-15, env-override added 2026-05-15 post-review).

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

Both thresholds are env-overridable via ``SOFASCORE_BACKFILL_GOVERNOR_*``:

  SOFASCORE_BACKFILL_GOVERNOR_OLDEST_HOT_AGE_MAX_SECONDS  (default 1800)
  SOFASCORE_BACKFILL_GOVERNOR_TIER_1_QUARANTINED_MAX      (default 10)

Note on threshold semantics: ``/ops/live-freshness`` reports
``breached=true`` at the WARN threshold (e.g. 900s on prod), but the
governor pauses backfill only at the CRIT threshold (1800s on prod).
That gap is intentional: WARN signals "operator attention" and is
expected to bounce as the sweeper catches up; pausing backfill at
WARN would be too aggressive and would never let historical lanes
drain. CRIT means "live is actually degrading" — that is the right
moment to yield proxy budget.

The governor is best-effort: any Redis exception falls back to "not
blocked" (we do NOT want a transient Redis hiccup to grind history
to a halt while live is actually fine).
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Sequence

logger = logging.getLogger(__name__)


# Re-use the live-state hot zset key. Imported lazily to avoid a
# circular import: queue.live_state imports schema_inspector.queue
# package which imports this module via __init__.
LIVE_HOT_ZSET_KEY = "zset:live:hot"
TIER_1_QUARANTINE_KEY_PATTERN = "live:tier1_quarantine:*"

_ENV_OLDEST_HOT_AGE_MAX = "SOFASCORE_BACKFILL_GOVERNOR_OLDEST_HOT_AGE_MAX_SECONDS"
_ENV_OLDEST_HOT_AGE_WARN = "SOFASCORE_BACKFILL_GOVERNOR_OLDEST_HOT_AGE_WARN_SECONDS"
_ENV_TIER_1_QUARANTINED_MAX = "SOFASCORE_BACKFILL_GOVERNOR_TIER_1_QUARANTINED_MAX"


@dataclass(frozen=True)
class BackfillGovernorThresholds:
    """Threshold knobs for ``BackfillGovernor``.

    Defaults mirror the live-freshness SLO CRIT thresholds in
    docs/N1_MONITORING_PLAN.md so a single bad signal blocks
    backfill without any additional tuning. All fields are
    env-overridable through :meth:`from_env`.

    ``oldest_hot_score_age_warn_seconds`` defines the start of the
    gradient throttle zone — between warn and max we deterministically
    skip a fraction of planner ticks proportional to how deep into the
    zone we are. Above max we full-pause (binary CRIT behaviour, kept
    for backward compatibility). When left unset (the sentinel ``-1``),
    warn defaults to half of max.
    """

    oldest_hot_score_age_max_seconds: int = 1800
    tier_1_quarantined_max_events: int = 10
    oldest_hot_score_age_warn_seconds: int = -1  # sentinel: derive from max

    def __post_init__(self) -> None:
        if self.oldest_hot_score_age_warn_seconds < 0:
            derived = max(1, self.oldest_hot_score_age_max_seconds // 2)
            object.__setattr__(self, "oldest_hot_score_age_warn_seconds", derived)
        # Clamp warn to (0, max) range so the math in BackfillGovernor.
        # _compute_blocking_reason never divides by zero or produces a
        # negative throttle fraction.
        if self.oldest_hot_score_age_warn_seconds >= self.oldest_hot_score_age_max_seconds:
            object.__setattr__(
                self,
                "oldest_hot_score_age_warn_seconds",
                max(1, self.oldest_hot_score_age_max_seconds - 1),
            )

    @classmethod
    def from_env(
        cls, env: Mapping[str, str] | None = None
    ) -> "BackfillGovernorThresholds":
        """Build thresholds from environment variables.

        Falls back to the dataclass defaults when an env var is missing
        or unparseable. Negative / zero values are silently clamped to
        the default to keep an operator typo from disabling the
        governor.
        """

        resolved = dict(os.environ) if env is None else dict(env)
        default_age = cls.__dataclass_fields__["oldest_hot_score_age_max_seconds"].default
        default_quarantine = cls.__dataclass_fields__["tier_1_quarantined_max_events"].default

        def _parse_positive_int(key: str, default: int) -> int:
            raw = resolved.get(key)
            if raw is None:
                return int(default)
            try:
                value = int(raw)
            except (TypeError, ValueError):
                logger.warning(
                    "BackfillGovernor env %s=%r is not int — using default %d",
                    key,
                    raw,
                    default,
                )
                return int(default)
            if value <= 0:
                logger.warning(
                    "BackfillGovernor env %s=%s must be positive — using default %d",
                    key,
                    value,
                    default,
                )
                return int(default)
            return value

        # Warn threshold: -1 sentinel triggers __post_init__ to derive
        # warn = max // 2. Explicit env override wins.
        warn_raw = resolved.get(_ENV_OLDEST_HOT_AGE_WARN)
        if warn_raw is None or warn_raw == "":
            warn_value = -1
        else:
            try:
                warn_value = int(warn_raw)
                if warn_value <= 0:
                    warn_value = -1
            except (TypeError, ValueError):
                warn_value = -1

        return cls(
            oldest_hot_score_age_max_seconds=_parse_positive_int(
                _ENV_OLDEST_HOT_AGE_MAX, default_age
            ),
            tier_1_quarantined_max_events=_parse_positive_int(
                _ENV_TIER_1_QUARANTINED_MAX, default_quarantine
            ),
            oldest_hot_score_age_warn_seconds=warn_value,
        )


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
        # Gradient-throttle accumulator: every call in the WARN zone
        # adds the current throttle fraction; when the accumulator
        # crosses 1.0 we deliberately skip one tick and subtract 1.0.
        # This converts a fractional throttle (e.g. 0.3 = 30%) into a
        # deterministic skip pattern (~every third call returns a
        # "throttled" reason).
        self._throttle_accumulator: float = 0.0

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
            # CRIT — full pause. Reset throttle accumulator so we don't
            # carry partial-throttle state into the recovery window once
            # the age drops back below max.
            self._throttle_accumulator = 0.0
            return (
                f"oldest_hot_score_age={age}s>"
                f"{self.thresholds.oldest_hot_score_age_max_seconds}s"
            )
        # WARN zone: deterministic fractional throttle proportional to
        # how deep the age is between warn and max thresholds.
        if age is not None:
            throttle_reason = self._maybe_throttle(age)
            if throttle_reason is not None:
                return throttle_reason
        quarantined = self._fetch_tier_1_quarantined_count()
        if quarantined is not None and quarantined > self.thresholds.tier_1_quarantined_max_events:
            return (
                f"tier_1_quarantined_events={quarantined}>"
                f"{self.thresholds.tier_1_quarantined_max_events}"
            )
        return None

    def _maybe_throttle(self, age: int) -> str | None:
        """Compute fractional throttle for a WARN-zone age.

        Returns a "throttled" reason string when this call should skip
        the planner tick, or None to let the tick proceed.

        Math: fraction = (age - warn) / (max - warn), bounded to [0, 1).
        We accumulate the fraction per call; on overflow (>=1.0) we
        emit one skip and reduce the accumulator by 1.0. This converts
        a continuous fraction into a deterministic skip rate.
        """

        warn = self.thresholds.oldest_hot_score_age_warn_seconds
        crit = self.thresholds.oldest_hot_score_age_max_seconds
        if age <= warn:
            # Healthy — keep the accumulator decaying so a sudden bump
            # into the WARN zone starts fresh rather than firing
            # immediately.
            self._throttle_accumulator = 0.0
            return None
        zone_width = max(1, crit - warn)
        fraction = (age - warn) / zone_width
        # Clamp to (0, 1) — CRIT path already handled age > crit.
        if fraction <= 0.0:
            return None
        if fraction >= 1.0:
            fraction = 0.999
        self._throttle_accumulator += fraction
        if self._throttle_accumulator >= 1.0:
            self._throttle_accumulator -= 1.0
            return (
                f"throttled (oldest_hot_score_age={age}s, "
                f"fraction={fraction:.2f}, warn={warn}s, max={crit}s)"
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
