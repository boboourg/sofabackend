"""Empty-data marker for the Resource Refresh Loop.

D6 introduces this marker for endpoints that return HTTP 200 with an
empty body. The pre-D2 upstream probe established two such cases:

* ``/api/v1/player/{id}/last-year-summary`` returns 200 + ~40 B
  ``{"summary": [], "uniqueTournamentsMap": {}}`` for ~70% of the
  active-squad scope (older actives, retired, youth/reserves). On
  ~48k players × 8 KB / day that would burn ~35k useless requests
  per day if every refresh actually fired upstream.
* ``/api/v1/player/{id}/national-team-statistics`` returns 200 + 17 B
  ``{"statistics": []}`` for ~30% of the narrowed national-team scope.

``ResourceNegativeCache`` cannot help here because the upstream status
is 200, not 4xx. Instead the worker inspects the freshly stored payload
through an endpoint-specific predicate; when the predicate flags it as
empty the entity is recorded here. The planner consults the store on
each tick and skips publishing a refresh job for that ``(pattern,
entity_id)`` pair until the configured TTL expires.

Data model:
  hash key:  ``hash:etl:resource_refresh:empty_data``
  field:     ``{endpoint_pattern}|{entity_id}``
  value:     observed_at_ms (int)

The store fails open: any Redis error returns ``False`` from the
"recently empty?" check, so the worst case is one extra fetch — never a
silent permanent skip. Symmetric with ``PaginationDoneStore``.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_HASH_KEY = "hash:etl:resource_refresh:empty_data"


class EmptyDataStore:
    """Tracks ``(endpoint_pattern, entity_id)`` last empty-body observation."""

    def __init__(self, backend: Any, *, hash_key: str = DEFAULT_HASH_KEY) -> None:
        self.backend = backend
        self.hash_key = hash_key

    def mark_empty(
        self,
        *,
        endpoint_pattern: str,
        entity_id: int,
        when_ms: int | None = None,
    ) -> None:
        if self.backend is None:
            return
        field = _build_field(endpoint_pattern, entity_id)
        value = str(int(when_ms if when_ms is not None else _now_ms()))
        try:
            try:
                self.backend.hset(self.hash_key, mapping={field: value})
            except TypeError:
                self.backend.hset(self.hash_key, {field: value})
        except Exception as exc:  # pragma: no cover -- best-effort
            logger.warning("EmptyDataStore.mark_empty failed: %s", exc)

    def is_empty_recently(
        self,
        *,
        endpoint_pattern: str,
        entity_id: int,
        ttl_seconds: int,
        now_ms: int | None = None,
    ) -> bool:
        """True when the last empty observation is younger than ``ttl_seconds``."""

        if self.backend is None or ttl_seconds <= 0:
            return False
        field = _build_field(endpoint_pattern, entity_id)
        try:
            raw = self.backend.hget(self.hash_key, field)
        except Exception as exc:  # pragma: no cover -- fail-open
            logger.warning(
                "EmptyDataStore.is_empty_recently failed (fail-open): %s", exc
            )
            return False
        if raw in (None, ""):
            return False
        try:
            observed_at_ms = int(raw)
        except (TypeError, ValueError):
            return False
        observed_now = int(now_ms if now_ms is not None else _now_ms())
        return (observed_now - observed_at_ms) < int(ttl_seconds) * 1000

    def clear(
        self,
        *,
        endpoint_pattern: str,
        entity_id: int,
    ) -> None:
        if self.backend is None:
            return
        field = _build_field(endpoint_pattern, entity_id)
        try:
            delete = getattr(self.backend, "hdel", None)
            if callable(delete):
                delete(self.hash_key, field)
        except Exception as exc:  # pragma: no cover
            logger.warning("EmptyDataStore.clear failed: %s", exc)


def _build_field(endpoint_pattern: str, entity_id: int) -> str:
    return f"{endpoint_pattern}|{int(entity_id)}"


def _now_ms() -> int:
    return int(time.time() * 1000)


__all__ = ["EmptyDataStore", "DEFAULT_HASH_KEY"]
