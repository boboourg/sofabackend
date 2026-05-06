"""Resource Refresh Loop negative cache (Redis).

When the upstream returns 404 or a soft-error payload for a refresh target,
the planner must not republish the same target until the upstream might
plausibly reappear. This module owns that single bit of state, keyed by the
exact same (endpoint_pattern, path_params) tuple as ``ResourceCursorStore``.

Why a separate Redis-only structure rather than the existing
``EndpointNegativeCacheRepository``: the existing repository is keyed by
``(scope_kind, unique_tournament_id, season_id, endpoint_pattern)`` and was
built for season-widget availability tracking. Team/player leaf endpoints
have no UT/season context. A flat Redis ``set`` of per-target keys with a
7-day TTL is sufficient for this use case: one key per stale entity, the
key auto-expires, no migration needed.

API contract:

* ``is_negatively_cached(pattern, path_params)`` returns ``True`` when the
  target should be skipped by the planner.
* ``mark_404(pattern, path_params)`` records a fresh 404 with the configured
  TTL. Subsequent ``is_negatively_cached`` calls within the TTL return True.
* All errors from the Redis backend are swallowed and treated as cache miss
  (fail-open) so a Redis hiccup never wedges the planner.

Configuration::

    SCHEMA_INSPECTOR_RESOURCE_NEGATIVE_TTL_SECONDS=604800   # 7 days
"""

from __future__ import annotations

import logging
import os
from typing import Any, Mapping

from .resource_cursor import build_cursor_field

logger = logging.getLogger(__name__)

KEY_PREFIX = "neg:resource_refresh:"
DEFAULT_TTL_SECONDS = 7 * 24 * 3600  # 7 days
TTL_ENV_KEY = "SCHEMA_INSPECTOR_RESOURCE_NEGATIVE_TTL_SECONDS"


class ResourceNegativeCache:
    """Per-target 404 cache backed by Redis SET keys with TTL."""

    def __init__(
        self,
        backend: Any,
        *,
        ttl_seconds: int | None = None,
        key_prefix: str = KEY_PREFIX,
        env: dict[str, str] | None = None,
    ) -> None:
        self.backend = backend
        self.key_prefix = key_prefix
        self._ttl_override = ttl_seconds
        self._env = env if env is not None else dict(os.environ)

    @property
    def ttl_seconds(self) -> int:
        if self._ttl_override is not None:
            return int(self._ttl_override)
        raw = self._env.get(TTL_ENV_KEY)
        if raw in (None, ""):
            return DEFAULT_TTL_SECONDS
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return DEFAULT_TTL_SECONDS
        return value if value > 0 else DEFAULT_TTL_SECONDS

    def is_negatively_cached(
        self,
        *,
        endpoint_pattern: str,
        path_params: Mapping[str, object],
    ) -> bool:
        if self.backend is None:
            return False
        key = self._build_key(endpoint_pattern, path_params)
        try:
            return bool(self.backend.exists(key))
        except Exception as exc:  # pragma: no cover -- fail-open
            logger.warning("ResourceNegativeCache.is_negatively_cached failed (fail-open): %s", exc)
            return False

    def mark_404(
        self,
        *,
        endpoint_pattern: str,
        path_params: Mapping[str, object],
        ttl_seconds: int | None = None,
    ) -> None:
        if self.backend is None:
            return
        key = self._build_key(endpoint_pattern, path_params)
        ttl = int(ttl_seconds) if ttl_seconds is not None else self.ttl_seconds
        try:
            self.backend.set(key, "1", ex=ttl)
        except Exception as exc:  # pragma: no cover -- best-effort
            logger.warning("ResourceNegativeCache.mark_404 failed: %s", exc)

    def clear(
        self,
        *,
        endpoint_pattern: str,
        path_params: Mapping[str, object],
    ) -> None:
        """Forget a previously-cached 404. Used by ops tools / tests."""

        if self.backend is None:
            return
        key = self._build_key(endpoint_pattern, path_params)
        try:
            self.backend.delete(key)
        except Exception as exc:  # pragma: no cover
            logger.warning("ResourceNegativeCache.clear failed: %s", exc)

    def _build_key(self, endpoint_pattern: str, path_params: Mapping[str, object]) -> str:
        # Reuse the cursor field builder so the negative cache and cursor share
        # an identical (pattern, path_params) signature -- prevents key drift.
        return f"{self.key_prefix}{build_cursor_field(endpoint_pattern, path_params)}"


__all__ = ["ResourceNegativeCache", "KEY_PREFIX", "DEFAULT_TTL_SECONDS"]
