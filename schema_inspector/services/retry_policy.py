"""Retry classification and backoff helpers for continuous workers."""

from __future__ import annotations

from curl_cffi.requests import RequestsError


class RetryableJobError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        delay_ms: int | None = None,
        audit_status: str = "retry_scheduled",
    ) -> None:
        super().__init__(message)
        self.delay_ms = None if delay_ms is None else int(delay_ms)
        self.audit_status = str(audit_status or "retry_scheduled").strip().lower() or "retry_scheduled"


class AdmissionDeferredError(RetryableJobError):
    """Signals that a job should be retried later because downstream capacity is saturated."""

    def __init__(self, message: str, *, delay_ms: int | None = None) -> None:
        super().__init__(message, delay_ms=delay_ms, audit_status="deferred_backpressure")


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
    return is_retryable_worker_error(exc)


def is_retryable_worker_error(exc: Exception) -> bool:
    if isinstance(exc, RetryableJobError):
        return True
    if isinstance(exc, TimeoutError):
        return True
    if isinstance(exc, RequestsError):
        return True
    if _is_retryable_upstream_error(exc):
        return True

    sqlstate = str(getattr(exc, "sqlstate", "") or "").upper()
    if sqlstate in _RETRYABLE_SQLSTATES:
        return True

    rendered = f"{exc.__class__.__name__} {exc}".lower()
    return any(marker in rendered for marker in _RETRYABLE_MARKERS)


def retry_audit_status(exc: Exception) -> str:
    status = str(getattr(exc, "audit_status", "") or "").strip().lower()
    if status:
        return status
    if _is_retryable_upstream_error(exc) or isinstance(exc, RequestsError):
        return "retry_upstream"
    return "retry_scheduled"


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
    if exc is not None:
        if _is_access_denied_error(exc):
            base_ms = 30_000
            cap_ms = 300_000
        elif _is_rate_limited_error(exc):
            base_ms = 60_000
            cap_ms = 300_000
        elif isinstance(exc, RequestsError):
            base_ms = 10_000
            cap_ms = 180_000
    normalized_attempt = max(1, int(attempt))
    multiplier = 2 ** (normalized_attempt - 1)
    return min(int(cap_ms), int(base_ms) * multiplier)


def _is_retryable_upstream_error(exc: Exception) -> bool:
    return _is_access_denied_error(exc) or _is_rate_limited_error(exc)


def _is_access_denied_error(exc: Exception) -> bool:
    try:
        from ..sofascore_client import SofascoreAccessDeniedError
    except Exception:  # pragma: no cover
        return False
    return isinstance(exc, SofascoreAccessDeniedError)


def _is_rate_limited_error(exc: Exception) -> bool:
    try:
        from ..sofascore_client import SofascoreRateLimitError
    except Exception:  # pragma: no cover
        return False
    return isinstance(exc, SofascoreRateLimitError)
