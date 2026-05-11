"""Per-event quarantine store for tier_1 root-only bad-route events.

P0(c) tier_1 root-only quarantine rollout: a small subset of events
(observed ~23 of ~400 inprogress) get stuck in libcurl timeout loops
(~106 s per attempt) through specific proxy paths. They burn tier_1
worker capacity on guaranteed-failing root fetches and suppress
tier_1's direct contribution to freshness even though the cluster
overall recovers via hydrate / tier_2 / tier_3 / warm lanes.

This store maintains two Redis keys per event:

* ``live:tier1_retry_failed:{event_id}`` — INCR-style counter of
  RetryableJobError failures within a sliding window (TTL =
  ``window_seconds`` so naturally rotates out).
* ``live:tier1_quarantine:{event_id}`` — active quarantine marker
  with TTL equal to the cooldown duration. Holds JSON
  ``{"until_ms": <epoch_ms>, "cycles": <N>}`` so we can compute the
  exponential cooldown for the next trigger.

Semantics:

* On real-work failure: increment counter. If counter ≥ ``threshold``,
  set quarantine with cooldown = ``min(max_cooldown,
  base_cooldown * 2^(cycles-1))`` and reset counter (so a new
  threshold-worth of failures must accumulate to extend quarantine).
* On real-work success: clear both keys.
* On coalesced (lt100ms inflight short-circuit): do nothing — coalesce
  is not a fetch attempt and must not feed back into the counter.
* On ``is_quarantined()``: read marker, return ``until_ms``; caller
  compares to ``now_ms``. Defensive fail-open (returns 0) on any
  backend exception so a Redis outage cannot block all tier_1 fetches.

Global cap brake: ``global_cap_exceeded()`` scans the quarantine key
pattern, compares against ``inprogress_event_count`` and the
``global_cap_pct`` (default 25). When exceeded, the worker layer
fails open — quarantine is suppressed cluster-wide until the count
drops, preventing a runaway-quarantine scenario where a transient
upstream incident parks the entire inprogress set.

Independent from ``LiveEventRootInFlightStore`` (single-flight) and
``LiveEdgesThrottle`` (warm-publish rate limit). All three lock /
throttle / quarantine layers are needed for different reasons and
operate on disjoint Redis keys, so they neither block each other
nor accidentally clear each other's state.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

LIVE_TIER_1_RETRY_FAILED_KEY = "live:tier1_retry_failed:{event_id}"
LIVE_TIER_1_QUARANTINE_KEY = "live:tier1_quarantine:{event_id}"

_DEFAULT_THRESHOLD = 3
_DEFAULT_WINDOW_SECONDS = 600
_DEFAULT_BASE_COOLDOWN_SECONDS = 60
_DEFAULT_MAX_COOLDOWN_SECONDS = 600
_DEFAULT_GLOBAL_CAP_PCT = 25


class LiveTier1RetryQuarantineStore:
    """Tracks per-event failure counts and quarantine cooldowns for tier_1 root-only.

    The store is constructed unconditionally by ``service_app`` so the
    dependency wiring stays stable; the live-tier worker's ``handle()``
    only consults it when ``LIVE_TIER_1_QUARANTINE_ENABLED`` is set.
    Disabled-flag callers should never pass the instance into
    LiveWorkerService — keeps the no-op fast path branch-free at
    runtime.
    """

    def __init__(
        self,
        backend: Any,
        *,
        threshold: int | None = None,
        window_seconds: int | None = None,
        base_cooldown_seconds: int | None = None,
        max_cooldown_seconds: int | None = None,
        global_cap_pct: int | None = None,
        now_ms_factory=None,
    ) -> None:
        self.backend = backend
        self.threshold = max(
            1,
            int(
                threshold
                if threshold is not None
                else _env_positive_int(
                    "LIVE_TIER_1_QUARANTINE_THRESHOLD", _DEFAULT_THRESHOLD
                )
            ),
        )
        self.window_seconds = max(
            1,
            int(
                window_seconds
                if window_seconds is not None
                else _env_positive_int(
                    "LIVE_TIER_1_QUARANTINE_WINDOW_SECONDS",
                    _DEFAULT_WINDOW_SECONDS,
                )
            ),
        )
        self.base_cooldown_seconds = max(
            1,
            int(
                base_cooldown_seconds
                if base_cooldown_seconds is not None
                else _env_positive_int(
                    "LIVE_TIER_1_QUARANTINE_BASE_COOLDOWN_SECONDS",
                    _DEFAULT_BASE_COOLDOWN_SECONDS,
                )
            ),
        )
        self.max_cooldown_seconds = max(
            self.base_cooldown_seconds,
            int(
                max_cooldown_seconds
                if max_cooldown_seconds is not None
                else _env_positive_int(
                    "LIVE_TIER_1_QUARANTINE_MAX_COOLDOWN_SECONDS",
                    _DEFAULT_MAX_COOLDOWN_SECONDS,
                )
            ),
        )
        self.global_cap_pct = max(
            1,
            min(
                100,
                int(
                    global_cap_pct
                    if global_cap_pct is not None
                    else _env_positive_int(
                        "LIVE_TIER_1_QUARANTINE_GLOBAL_CAP_PCT",
                        _DEFAULT_GLOBAL_CAP_PCT,
                    )
                ),
            ),
        )
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))

    def is_quarantined(self, *, event_id: int, now_ms: int | None = None) -> int:
        """Return ``until_ms`` (epoch ms) if event is currently quarantined, else 0.

        Caller compares to its own ``now_ms`` to decide whether to skip.
        Returning 0 also covers the fail-open path on Redis exceptions
        so an outage cannot block tier_1 fetches.
        """
        key = LIVE_TIER_1_QUARANTINE_KEY.format(event_id=int(event_id))
        try:
            raw = self.backend.get(key)
        except Exception as exc:  # pragma: no cover - defensive fail-open
            logger.warning(
                "LiveTier1RetryQuarantineStore Redis GET failed for event_id=%s: %s — fail-open (allow fetch)",
                event_id,
                exc,
            )
            return 0
        if raw is None:
            return 0
        decoded = _decode_value(raw)
        if not decoded:
            return 0
        try:
            payload = json.loads(decoded)
        except (TypeError, ValueError):
            return 0
        try:
            return int(payload.get("until_ms") or 0)
        except (TypeError, ValueError):
            return 0

    def record_failure(self, *, event_id: int, now_ms: int | None = None) -> bool:
        """Increment failure counter; trigger quarantine at threshold.

        Returns True iff a new (or extended) quarantine was set.
        Caller does not need this return value — provided for logging
        and tests. Counter is sliding-window via TTL: each new failure
        resets TTL to ``window_seconds``, so K failures within a window
        accumulate; a single failure followed by ``window_seconds`` of
        silence naturally decays away. On threshold, counter is cleared
        so the next quarantine cycle requires a fresh K failures.
        """
        counter_key = LIVE_TIER_1_RETRY_FAILED_KEY.format(event_id=int(event_id))
        quarantine_key = LIVE_TIER_1_QUARANTINE_KEY.format(event_id=int(event_id))
        try:
            count = self.backend.incr(counter_key)
        except Exception as exc:  # pragma: no cover - defensive fail-open
            logger.warning(
                "LiveTier1RetryQuarantineStore INCR failed for event_id=%s: %s — skipping quarantine bookkeeping",
                event_id,
                exc,
            )
            return False
        try:
            # Refresh TTL on every increment so the window is sliding-rolling
            # from the LATEST failure (not the first). Cheap: 1 EXPIRE call.
            self.backend.expire(counter_key, self.window_seconds)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(
                "LiveTier1RetryQuarantineStore EXPIRE on counter failed for event_id=%s: %s",
                event_id,
                exc,
            )
        try:
            count_int = int(count)
        except (TypeError, ValueError):
            count_int = 0
        if count_int < self.threshold:
            return False

        # Threshold reached. Determine cycle number from any existing
        # quarantine marker (so re-triggers within the same TTL window
        # apply exponential backoff). If no marker exists, this is cycle 1.
        cycles = 1
        try:
            existing_raw = self.backend.get(quarantine_key)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(
                "LiveTier1RetryQuarantineStore GET on existing quarantine failed for event_id=%s: %s",
                event_id,
                exc,
            )
            existing_raw = None
        if existing_raw is not None:
            decoded_existing = _decode_value(existing_raw)
            if decoded_existing:
                try:
                    parsed = json.loads(decoded_existing)
                    cycles = max(1, int(parsed.get("cycles") or 1) + 1)
                except (TypeError, ValueError):
                    cycles = 1
        cooldown_seconds = min(
            self.max_cooldown_seconds,
            self.base_cooldown_seconds * (2 ** (cycles - 1)),
        )
        now_value = int(now_ms if now_ms is not None else self.now_ms_factory())
        until_ms = now_value + cooldown_seconds * 1000
        payload = json.dumps({"until_ms": until_ms, "cycles": cycles})
        try:
            self.backend.set(quarantine_key, payload, ex=cooldown_seconds)
        except TypeError:
            # Fallback for backends without ex= kwarg — best-effort
            # set + expire (race-acceptable; failure mode is one extra
            # tick of quarantine miss/overrun, not corruption).
            try:
                self.backend.set(quarantine_key, payload)
                if hasattr(self.backend, "expire"):
                    self.backend.expire(quarantine_key, cooldown_seconds)
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning(
                    "LiveTier1RetryQuarantineStore set+expire fallback failed for event_id=%s: %s",
                    event_id,
                    exc,
                )
                return False
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning(
                "LiveTier1RetryQuarantineStore SET quarantine failed for event_id=%s: %s",
                event_id,
                exc,
            )
            return False
        # Reset counter so re-trigger requires fresh K failures.
        try:
            self.backend.delete(counter_key)
        except Exception as exc:  # pragma: no cover - defensive
            logger.debug(
                "LiveTier1RetryQuarantineStore counter DEL after trigger failed for event_id=%s: %s",
                event_id,
                exc,
            )
        logger.info(
            "Tier_1 root-only quarantine triggered: event_id=%s cycles=%s cooldown_seconds=%s until_ms=%s",
            event_id,
            cycles,
            cooldown_seconds,
            until_ms,
        )
        return True

    def record_success(self, *, event_id: int) -> None:
        """Clear both counter and active quarantine on real-work success.

        Coalesced (lt100ms inflight short-circuit) returns from the
        worker layer MUST NOT call this — they are not fetch attempts
        and clearing on coalesced would let a sticky event escape
        quarantine without proving the route works.
        """
        counter_key = LIVE_TIER_1_RETRY_FAILED_KEY.format(event_id=int(event_id))
        quarantine_key = LIVE_TIER_1_QUARANTINE_KEY.format(event_id=int(event_id))
        for key in (counter_key, quarantine_key):
            try:
                self.backend.delete(key)
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug(
                    "LiveTier1RetryQuarantineStore DEL on success failed for key=%s event_id=%s: %s",
                    key,
                    event_id,
                    exc,
                )

    def quarantined_count(self) -> int:
        """Return the count of currently-active quarantine markers.

        Used by ``global_cap_exceeded`` for the safety brake. Uses
        Redis SCAN if available (non-blocking, production-safe);
        fakes/older clients without SCAN fall back to an empty result
        (i.e. cap-check is effectively disabled — fail-open).
        """
        scan_iter = getattr(self.backend, "scan_iter", None)
        pattern = LIVE_TIER_1_QUARANTINE_KEY.format(event_id="*")
        if callable(scan_iter):
            try:
                return sum(1 for _ in scan_iter(match=pattern))
            except TypeError:
                # backend.scan_iter() may not accept ``match`` kwarg
                try:
                    return sum(1 for key in scan_iter() if _matches_pattern(key, pattern))
                except Exception as exc:  # pragma: no cover - defensive
                    logger.debug(
                        "LiveTier1RetryQuarantineStore scan_iter fallback failed: %s",
                        exc,
                    )
                    return 0
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("LiveTier1RetryQuarantineStore scan_iter failed: %s", exc)
                return 0
        keys_method = getattr(self.backend, "keys", None)
        if callable(keys_method):
            try:
                result = keys_method(pattern)
                return len(list(result)) if result is not None else 0
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("LiveTier1RetryQuarantineStore keys() failed: %s", exc)
                return 0
        return 0

    def global_cap_exceeded(self, *, inprogress_event_count: int) -> bool:
        """Return True iff > ``global_cap_pct``% of inprogress events are quarantined.

        Caller is responsible for providing a snapshot of inprogress
        event count (e.g. from a planner-side gauge or the live state
        store). When ``inprogress_event_count`` is 0 or negative, the
        cap-check is treated as "cannot evaluate → fail-open" and
        returns False so quarantine still works in test / startup
        scenarios.
        """
        if inprogress_event_count <= 0:
            return False
        quarantined = self.quarantined_count()
        cap = (inprogress_event_count * self.global_cap_pct) / 100.0
        return quarantined > cap


def _matches_pattern(key: object, pattern: str) -> bool:
    """Glob-style match for fallback SCAN paths.

    Supports the ``*`` wildcard used by Redis MATCH patterns. Sufficient
    for the single-wildcard key shape used by this store; we deliberately
    avoid pulling in fnmatch to keep the dependency surface minimal.
    """
    key_str = _decode_value(key) or ""
    if "*" not in pattern:
        return key_str == pattern
    prefix, _, suffix = pattern.partition("*")
    return key_str.startswith(prefix) and key_str.endswith(suffix)


def _decode_value(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return str(value)


def _env_positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return default
    try:
        value = int(str(raw))
    except ValueError:
        logger.warning("Invalid %s=%r; falling back to %d", name, raw, default)
        return default
    if value < 1:
        logger.warning("%s must be >= 1 (got %d); falling back to %d", name, value, default)
        return default
    return value
