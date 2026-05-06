"""Per-target last-refresh cursor for the Resource Refresh Loop.

The cursor lives in a single Redis hash keyed by a stable signature of
``(endpoint_pattern, path_params)``. The signature MUST include every path
parameter -- not just the entity_id -- so paginated endpoints
(e.g. ``team/{team_id}/events/last/{page}``) keep separate cursors per page.

This store is intentionally narrow: it does NOT prevent concurrent fetches
(``FreshnessStore`` covers that with a short TTL on the resource side) and
it does NOT carry retry state. Its sole job is "did the planner already
publish a refresh job for this exact target within the last
refresh_interval_seconds".
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Mapping

logger = logging.getLogger(__name__)

DEFAULT_HASH_KEY = "hash:etl:resource_refresh_cursor"


class ResourceCursorStore:
    """Redis-backed last-refresh cursor for resource refresh targets."""

    def __init__(self, backend: Any, *, hash_key: str = DEFAULT_HASH_KEY) -> None:
        self.backend = backend
        self.hash_key = hash_key

    def load_last_refresh_ms(
        self,
        *,
        endpoint_pattern: str,
        path_params: Mapping[str, object],
    ) -> int:
        """Return the last refresh timestamp (epoch ms) or 0 if absent."""

        field_name = build_cursor_field(endpoint_pattern, path_params)
        try:
            raw = self.backend.hget(self.hash_key, field_name)
        except Exception as exc:  # pragma: no cover -- fail-open on backend errors
            logger.warning("ResourceCursorStore.load failed (fail-open): %s", exc)
            return 0
        if raw in (None, ""):
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def save_last_refresh_ms(
        self,
        *,
        endpoint_pattern: str,
        path_params: Mapping[str, object],
        when_ms: int | None = None,
    ) -> None:
        """Mark target as refreshed at ``when_ms`` (default = now)."""

        field_name = build_cursor_field(endpoint_pattern, path_params)
        value = str(int(when_ms if when_ms is not None else _now_ms()))
        try:
            try:
                self.backend.hset(self.hash_key, mapping={field_name: value})
            except TypeError:
                # Older redis-py shim that does not accept the mapping kwarg.
                self.backend.hset(self.hash_key, {field_name: value})
        except Exception as exc:  # pragma: no cover -- best-effort
            logger.warning("ResourceCursorStore.save failed: %s", exc)


def build_cursor_field(endpoint_pattern: str, path_params: Mapping[str, object]) -> str:
    """Build a stable hash field name for a (pattern, path_params) target.

    Two distinct targets MUST produce two distinct field names; otherwise
    the planner would silently coalesce different cursors. We use a JSON
    rendering with sorted keys to keep the field deterministic regardless
    of dict iteration order.
    """

    rendered = json.dumps(
        dict(path_params),
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"{endpoint_pattern}|{rendered}"


def _now_ms() -> int:
    return int(time.time() * 1000)


__all__ = ["ResourceCursorStore", "build_cursor_field", "DEFAULT_HASH_KEY"]
