"""Redis-backed single-flight guard for live event refresh jobs."""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

LIVE_EVENT_INFLIGHT_KEY = "live:refresh_inflight:{event_id}"
_DEFAULT_TTL_MS = 600_000

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
