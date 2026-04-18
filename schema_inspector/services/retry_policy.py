"""Retry classification and backoff helpers for continuous workers."""

from __future__ import annotations


class RetryableJobError(RuntimeError):
    def __init__(self, message: str, *, delay_ms: int | None = None) -> None:
        super().__init__(message)
        self.delay_ms = None if delay_ms is None else int(delay_ms)


class AdmissionDeferredError(RetryableJobError):
    """Signals that a job should be retried later because downstream capacity is saturated."""


_RETRYABLE_SQLSTATES = {
    "40P01",  # deadlock_detected
    "55P03",  # lock_not_available / lock_timeout
}

_RETRYABLE_MARKERS = (
    "lock timeout",
    "could not obtain lock",
    "deadlock detected",
    "locknotavailableerror",
    "deadlockdetectederror",
)


def is_retryable_db_error(exc: Exception) -> bool:
    if isinstance(exc, RetryableJobError):
        return True
    if isinstance(exc, TimeoutError):
        return True

    sqlstate = str(getattr(exc, "sqlstate", "") or "").upper()
    if sqlstate in _RETRYABLE_SQLSTATES:
        return True

    rendered = f"{exc.__class__.__name__} {exc}".lower()
    return any(marker in rendered for marker in _RETRYABLE_MARKERS)


def retry_delay_ms(
    *,
    attempt: int,
    exc: Exception | None = None,
    base_ms: int = 5_000,
    cap_ms: int = 60_000,
) -> int:
    custom_delay_ms = getattr(exc, "delay_ms", None)
    if custom_delay_ms is not None:
        return max(0, int(custom_delay_ms))
    normalized_attempt = max(1, int(attempt))
    multiplier = 2 ** (normalized_attempt - 1)
    return min(int(cap_ms), int(base_ms) * multiplier)
