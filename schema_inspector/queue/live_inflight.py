"""Redis-backed single-flight guard for live event refresh jobs."""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

LIVE_EVENT_INFLIGHT_KEY = "live:refresh_inflight:{event_id}"
LIVE_EVENT_DETAILS_INFLIGHT_KEY = "live:details_inflight:{event_id}"
LIVE_EVENT_ROOT_INFLIGHT_KEY = "live:root_inflight:{event_id}"
_DEFAULT_TTL_MS = 600_000
_DEFAULT_DETAILS_TTL_MS = 300_000
_DEFAULT_ROOT_TTL_MS = 60_000

_RELEASE_IF_OWNER_SCRIPT = """
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
end
return 0
"""


class LiveEventInFlightStore:
    """Short-lived cross-consumer lock for one live event refresh.

    This is intentionally separate from the live bootstrap lock. Bootstrap
    prevents multiple full hydrations before an event is marked bootstrapped;
    this store prevents ordinary live_delta refreshes for the same event from
    running concurrently across Redis Stream consumers.
    """

    def __init__(self, backend: Any, *, ttl_ms: int | None = None) -> None:
        self.backend = backend
        self.ttl_ms = int(ttl_ms if ttl_ms is not None else _env_positive_int("SOFASCORE_LIVE_EVENT_INFLIGHT_TTL_MS", _DEFAULT_TTL_MS))

    def claim(self, *, event_id: int, owner: str) -> bool:
        key = LIVE_EVENT_INFLIGHT_KEY.format(event_id=int(event_id))
        return bool(_call_backend(self.backend.set, key, str(owner), nx=True, px=self.ttl_ms))

    def release(self, *, event_id: int, owner: str) -> None:
        key = LIVE_EVENT_INFLIGHT_KEY.format(event_id=int(event_id))
        if _release_with_lua(self.backend, key, str(owner)):
            return
        current = _decode_value(_call_backend(getattr(self.backend, "get"), key))
        if current != str(owner):
            return
        _call_backend(getattr(self.backend, "delete"), key)


class LiveEventDetailsInFlightStore:
    """Cross-consumer lock for the *details* fanout of one live event.

    P0(a) split-details rollout: details (per-player heatmap/statistics/
    rating-breakdown, shotmaps, comments, h2h, ...) run in a dedicated
    worker pool consuming ``stream:etl:live_details``. This lock prevents
    duplicate concurrent details fanouts for the same event when multiple
    enqueues land in the stream before the first details run finishes.
    Independent from ``LiveEventInFlightStore`` (which guards the live
    tier ROOT+edges critical path) — release of one does not affect the
    other. TTL is shorter than the critical lock (5 min default) because
    a hung details fanout should auto-recover faster than a full live
    refresh.
    """

    def __init__(self, backend: Any, *, ttl_ms: int | None = None) -> None:
        self.backend = backend
        self.ttl_ms = int(
            ttl_ms
            if ttl_ms is not None
            else _env_positive_int(
                "SOFASCORE_LIVE_DETAILS_INFLIGHT_TTL_MS",
                _DEFAULT_DETAILS_TTL_MS,
            )
        )

    def claim(self, *, event_id: int, owner: str) -> bool:
        key = LIVE_EVENT_DETAILS_INFLIGHT_KEY.format(event_id=int(event_id))
        return bool(_call_backend(self.backend.set, key, str(owner), nx=True, px=self.ttl_ms))

    def release(self, *, event_id: int, owner: str) -> None:
        key = LIVE_EVENT_DETAILS_INFLIGHT_KEY.format(event_id=int(event_id))
        if _release_with_lua(self.backend, key, str(owner)):
            return
        current = _decode_value(_call_backend(getattr(self.backend, "get"), key))
        if current != str(owner):
            return
        _call_backend(getattr(self.backend, "delete"), key)


class LiveEventRootInFlightStore:
    """Cross-consumer lock for the *root-only* fast path of one live event.

    P0(b) tier_1 root-only rollout (``LIVE_TIER_1_ROOT_ONLY``): tier_1
    workers, when the flag is ON, run a short ``run_event(hydration_mode=
    "root_only")`` (≈1-2 s wall-clock) that persists only the
    ``/event`` snapshot and finalises terminal payloads, then enqueues a
    separate ``refresh_live_event`` job to ``stream:etl:live_warm`` for
    edges/details. This lock prevents duplicate concurrent root-only
    fetches for the same event when the planner publishes the next
    refresh tick before the previous one finishes.

    Independent from ``LiveEventInFlightStore`` (which guards the legacy
    ROOT+edges+details critical path on tier_1/tier_2/tier_3 lanes when
    root-only flag is OFF, and on live_warm always). The two locks MUST
    NOT block each other:

    * tier_1 root-only path -> ``live:root_inflight``
    * live_warm full refresh -> ``live:refresh_inflight``
    * tier_2/tier_3 (root-only flag does not apply) -> ``live:refresh_inflight``

    TTL is short (60 s default) because root-only is expected to take
    1-2 s; a 60 s ceiling auto-recovers from a hung worker without
    blocking subsequent ticks for too long.
    """

    def __init__(self, backend: Any, *, ttl_ms: int | None = None) -> None:
        self.backend = backend
        self.ttl_ms = int(
            ttl_ms
            if ttl_ms is not None
            else _env_positive_int(
                "SOFASCORE_LIVE_ROOT_INFLIGHT_TTL_MS",
                _DEFAULT_ROOT_TTL_MS,
            )
        )

    def claim(self, *, event_id: int, owner: str) -> bool:
        key = LIVE_EVENT_ROOT_INFLIGHT_KEY.format(event_id=int(event_id))
        return bool(_call_backend(self.backend.set, key, str(owner), nx=True, px=self.ttl_ms))

    def release(self, *, event_id: int, owner: str) -> None:
        key = LIVE_EVENT_ROOT_INFLIGHT_KEY.format(event_id=int(event_id))
        if _release_with_lua(self.backend, key, str(owner)):
            return
        current = _decode_value(_call_backend(getattr(self.backend, "get"), key))
        if current != str(owner):
            return
        _call_backend(getattr(self.backend, "delete"), key)


def _release_with_lua(backend: Any, key: str, owner: str) -> bool:
    eval_method = getattr(backend, "eval", None)
    if not callable(eval_method):
        return False
    try:
        eval_method(_RELEASE_IF_OWNER_SCRIPT, 1, key, owner)
        return True
    except Exception as exc:  # pragma: no cover - defensive fallback for non-Redis backends
        logger.debug("Live event in-flight Lua release failed, falling back: %s", exc)
        return False


def _call_backend(method, *args, **kwargs):
    try:
        return method(*args, **kwargs)
    except TypeError:
        filtered_kwargs = {key: value for key, value in kwargs.items() if key not in {"px", "nx"}}
        return method(*args, **filtered_kwargs)


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
