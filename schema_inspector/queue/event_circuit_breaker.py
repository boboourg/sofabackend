"""Generic per-event circuit breaker — extends the P0(c) quarantine
pattern to non-tier_1 lanes (hydrate, discovery, historical).

Background: P0(c) ``LiveTier1RetryQuarantineStore`` proved successful
on tier_1 lane (cluster-wide retry rate -43% for bad-route sticky
events). P3 audit-trail fix exposed the Smartproxy timeout pattern
across all lanes — discovery_worker and hydrate_worker are now the
dominant Process B in retry storms. Forensics (ErrCode 28: 83.6%,
ErrCode 60: 8.2%, ErrCode 35: 4.9%) confirm the same sticky-event
pattern persists outside tier_1.

This module is a lane-parameterized port of the P0(c) store. The
existing ``LiveTier1RetryQuarantineStore`` continues to run unchanged
for tier_1 (separate Redis namespace) to preserve the canary win.

State model (per lane):

* ``live:event_cb:{lane}:retry_failed:{event_id}`` — INCR counter,
  TTL = ``window_seconds`` (sliding window)
* ``live:event_cb:{lane}:quarantine:{event_id}`` — active marker
  JSON ``{"until_ms": <ts>, "cycles": <N>}``, TTL = cooldown

Semantics (same as P0(c)):

* On retryable failure: increment counter; at threshold trigger
  quarantine with exponential cooldown (base × 2^(cycles-1) capped at
  max_cooldown)
* On real-work success: clear both keys
* On coalesced / skip: do NOT touch counter
* On is_quarantined(): defensive fail-open returns 0 on Redis error
* Global-cap brake suppresses quarantine when too many events
  parked (default 25% of inprogress)

Lanes covered by this module:

* ``hydrate`` — operational hydrate_worker pool
* ``discovery`` — discovery_worker per-event publish gate
* ``historical_hydrate`` — historical-hydrate_worker pool (optional,
  not enabled by default since historical work is less time-sensitive)
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)

EVENT_CB_RETRY_FAILED_KEY = "live:event_cb:{lane}:retry_failed:{event_id}"
EVENT_CB_QUARANTINE_KEY = "live:event_cb:{lane}:quarantine:{event_id}"

_DEFAULT_THRESHOLD = 3
_DEFAULT_WINDOW_SECONDS = 600
_DEFAULT_BASE_COOLDOWN_SECONDS = 60
_DEFAULT_MAX_COOLDOWN_SECONDS = 600
_DEFAULT_GLOBAL_CAP_PCT = 25


class EventCircuitBreaker:
    """Per-event circuit breaker for any worker lane.

    The lane name namespaces both the Redis key prefix and the env-var
    knob names so two lanes (e.g. hydrate + discovery) can run with
    independent thresholds and quarantine states.

    Env knobs (per lane, uppercase):

    * ``EVENT_CB_{LANE}_THRESHOLD`` (default 3)
    * ``EVENT_CB_{LANE}_WINDOW_SECONDS`` (default 600)
    * ``EVENT_CB_{LANE}_BASE_COOLDOWN_SECONDS`` (default 60)
    * ``EVENT_CB_{LANE}_MAX_COOLDOWN_SECONDS`` (default 600)
    * ``EVENT_CB_{LANE}_GLOBAL_CAP_PCT`` (default 25)

    The store is constructed unconditionally by ``service_app``. Worker
    layers consult it only when the per-lane enable flag (e.g.
    ``HYDRATE_EVENT_CIRCUIT_BREAKER_ENABLED``,
    ``DISCOVERY_EVENT_CIRCUIT_BREAKER_ENABLED``) is set — keeps the
    legacy path branch-free at runtime when disabled.
    """

    def __init__(
        self,
        backend: Any,
        *,
        lane: str,
        threshold: int | None = None,
        window_seconds: int | None = None,
        base_cooldown_seconds: int | None = None,
        max_cooldown_seconds: int | None = None,
        global_cap_pct: int | None = None,
        now_ms_factory=None,
    ) -> None:
        self.backend = backend
        self.lane = str(lane).strip().lower()
        if not self.lane:
            raise ValueError("EventCircuitBreaker requires a non-empty lane name")
        lane_upper = self.lane.upper()
        self.threshold = max(
            1,
            int(
                threshold
                if threshold is not None
                else _env_positive_int(f"EVENT_CB_{lane_upper}_THRESHOLD", _DEFAULT_THRESHOLD)
            ),
        )
        self.window_seconds = max(
            1,
            int(
                window_seconds
                if window_seconds is not None
                else _env_positive_int(
                    f"EVENT_CB_{lane_upper}_WINDOW_SECONDS", _DEFAULT_WINDOW_SECONDS
                )
            ),
        )
        self.base_cooldown_seconds = max(
            1,
            int(
                base_cooldown_seconds
                if base_cooldown_seconds is not None
                else _env_positive_int(
                    f"EVENT_CB_{lane_upper}_BASE_COOLDOWN_SECONDS",
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
                    f"EVENT_CB_{lane_upper}_MAX_COOLDOWN_SECONDS",
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
                        f"EVENT_CB_{lane_upper}_GLOBAL_CAP_PCT", _DEFAULT_GLOBAL_CAP_PCT
                    )
                ),
            ),
        )
        self.now_ms_factory = now_ms_factory or (lambda: int(time.time() * 1000))

    def _counter_key(self, event_id: int) -> str:
        return EVENT_CB_RETRY_FAILED_KEY.format(lane=self.lane, event_id=int(event_id))

    def _quarantine_key(self, event_id: int) -> str:
        return EVENT_CB_QUARANTINE_KEY.format(lane=self.lane, event_id=int(event_id))

    def is_quarantined(self, *, event_id: int, now_ms: int | None = None) -> int:
        """Return ``until_ms`` (epoch ms) if event is quarantined, else 0.

        Defensive fail-open (returns 0) on any backend exception so a
        Redis outage cannot block the worker layer entirely.
        """
        del now_ms  # caller-supplied now is unused — we read Redis
        key = self._quarantine_key(event_id)
        try:
            raw = self.backend.get(key)
        except Exception as exc:  # pragma: no cover - defensive fail-open
            logger.warning(
                "EventCircuitBreaker(lane=%s) Redis GET failed for event_id=%s: %s — fail-open",
                self.lane, event_id, exc,
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
        """Increment counter; trigger quarantine on threshold.

        Returns True iff a new (or extended) quarantine marker was set.
        Caller does not need this return — provided for logging/tests.
        """
        counter_key = self._counter_key(event_id)
        quarantine_key = self._quarantine_key(event_id)
        try:
            count = self.backend.incr(counter_key)
        except Exception as exc:  # pragma: no cover - defensive fail-open
            logger.warning(
                "EventCircuitBreaker(lane=%s) INCR failed for event_id=%s: %s",
                self.lane, event_id, exc,
            )
            return False
        try:
            self.backend.expire(counter_key, self.window_seconds)
        except Exception as exc:  # pragma: no cover
            logger.debug(
                "EventCircuitBreaker(lane=%s) EXPIRE on counter failed for event_id=%s: %s",
                self.lane, event_id, exc,
            )
        try:
            count_int = int(count)
        except (TypeError, ValueError):
            count_int = 0
        if count_int < self.threshold:
            return False

        cycles = 1
        try:
            existing_raw = self.backend.get(quarantine_key)
        except Exception as exc:  # pragma: no cover
            logger.debug(
                "EventCircuitBreaker(lane=%s) GET on existing quarantine failed for event_id=%s: %s",
                self.lane, event_id, exc,
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
            # Older redis-py / fake backends without ex kwarg — fall back.
            try:
                self.backend.set(quarantine_key, payload)
                if hasattr(self.backend, "expire"):
                    self.backend.expire(quarantine_key, cooldown_seconds)
            except Exception as exc:  # pragma: no cover
                logger.warning(
                    "EventCircuitBreaker(lane=%s) set+expire fallback failed for event_id=%s: %s",
                    self.lane, event_id, exc,
                )
                return False
        except Exception as exc:  # pragma: no cover
            logger.warning(
                "EventCircuitBreaker(lane=%s) SET quarantine failed for event_id=%s: %s",
                self.lane, event_id, exc,
            )
            return False
        try:
            self.backend.delete(counter_key)
        except Exception as exc:  # pragma: no cover
            logger.debug(
                "EventCircuitBreaker(lane=%s) counter DEL after trigger failed: %s",
                self.lane, exc,
            )
        logger.info(
            "EventCircuitBreaker(lane=%s) triggered: event_id=%s cycles=%s cooldown_seconds=%s until_ms=%s",
            self.lane, event_id, cycles, cooldown_seconds, until_ms,
        )
        return True

    def record_success(self, *, event_id: int) -> None:
        """Clear both counter and quarantine marker."""
        for key in (self._counter_key(event_id), self._quarantine_key(event_id)):
            try:
                self.backend.delete(key)
            except Exception as exc:  # pragma: no cover
                logger.debug(
                    "EventCircuitBreaker(lane=%s) DEL on success failed for key=%s event_id=%s: %s",
                    self.lane, key, event_id, exc,
                )

    def quarantined_count(self) -> int:
        """Count currently-active quarantine markers for THIS lane."""
        scan_iter = getattr(self.backend, "scan_iter", None)
        pattern = EVENT_CB_QUARANTINE_KEY.format(lane=self.lane, event_id="*")
        if callable(scan_iter):
            try:
                return sum(1 for _ in scan_iter(match=pattern))
            except TypeError:
                try:
                    return sum(1 for key in scan_iter() if _matches_pattern(key, pattern))
                except Exception as exc:  # pragma: no cover
                    logger.debug(
                        "EventCircuitBreaker(lane=%s) scan_iter fallback failed: %s",
                        self.lane, exc,
                    )
                    return 0
            except Exception as exc:  # pragma: no cover
                logger.debug(
                    "EventCircuitBreaker(lane=%s) scan_iter failed: %s",
                    self.lane, exc,
                )
                return 0
        keys_method = getattr(self.backend, "keys", None)
        if callable(keys_method):
            try:
                result = keys_method(pattern)
                return len(list(result)) if result is not None else 0
            except Exception as exc:  # pragma: no cover
                logger.debug(
                    "EventCircuitBreaker(lane=%s) keys() failed: %s",
                    self.lane, exc,
                )
                return 0
        return 0

    def global_cap_exceeded(self, *, inprogress_event_count: int) -> bool:
        """Return True iff > global_cap_pct% of inprogress events are
        quarantined on THIS lane. inprogress_event_count <= 0 → returns
        False (fail-open when cap cannot be evaluated)."""
        if inprogress_event_count <= 0:
            return False
        quarantined = self.quarantined_count()
        cap = (inprogress_event_count * self.global_cap_pct) / 100.0
        return quarantined > cap


def _matches_pattern(key: object, pattern: str) -> bool:
    """Glob-style match for fallback SCAN paths (single * wildcard)."""
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
