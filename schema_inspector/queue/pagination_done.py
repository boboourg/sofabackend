"""Per-entity "fully paginated" marker for the Resource Refresh Loop.

Used by the worker-side auto-pagination chain (Stage 8 / C.4). When a
chain reaches ``hasNextPage=false`` for a target ``(endpoint, entity_id)``
the worker stamps that pair with the current epoch_ms; subsequent
planner-driven page=0 refreshes consult this store and skip starting a
new chain until the audit interval has elapsed.

Data model:
  hash key:  ``hash:etl:resource_refresh:pagination_done``
  field:     ``{endpoint_pattern}|{entity_id}`` -- not per-page; the marker
             is "this whole tail walk is done".
  value:     completed_at_ms (int)

The store is intentionally narrow: just two operations. It does NOT
store retry state, page number, or chain depth -- those live on the
JobEnvelope passed back through the stream. It also fail-opens on Redis
errors: the worst case is one extra walk if Redis hiccups, never a
silent stale state.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)

DEFAULT_HASH_KEY = "hash:etl:resource_refresh:pagination_done"


class PaginationDoneStore:
    """Tracks ``(endpoint_pattern, entity_id)`` last-completion timestamps."""

    def __init__(self, backend: Any, *, hash_key: str = DEFAULT_HASH_KEY) -> None:
        self.backend = backend
        self.hash_key = hash_key

    def mark_completed(
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
            logger.warning("PaginationDoneStore.mark_completed failed: %s", exc)

    def is_completed_recently(
        self,
        *,
        endpoint_pattern: str,
        entity_id: int,
        audit_interval_seconds: int,
        now_ms: int | None = None,
    ) -> bool:
        """True when the last completion is younger than the audit interval.

        Returns False on missing entry or any Redis error so the worst-case
        outcome is "we walk again" instead of "we silently skip forever".
        """

        if self.backend is None or audit_interval_seconds <= 0:
            return False
        field = _build_field(endpoint_pattern, entity_id)
        try:
            raw = self.backend.hget(self.hash_key, field)
        except Exception as exc:  # pragma: no cover -- fail-open
            logger.warning(
                "PaginationDoneStore.is_completed_recently failed (fail-open): %s",
                exc,
            )
            return False
        if raw in (None, ""):
            return False
        try:
            completed_at_ms = int(raw)
        except (TypeError, ValueError):
            return False
        observed_now = int(now_ms if now_ms is not None else _now_ms())
        return (observed_now - completed_at_ms) < int(audit_interval_seconds) * 1000

    def clear(
        self,
        *,
        endpoint_pattern: str,
        entity_id: int,
    ) -> None:
        """Forget the marker; used by ops tooling when a forced re-walk is needed."""

        if self.backend is None:
            return
        field = _build_field(endpoint_pattern, entity_id)
        try:
            delete = getattr(self.backend, "hdel", None)
            if callable(delete):
                delete(self.hash_key, field)
        except Exception as exc:  # pragma: no cover
            logger.warning("PaginationDoneStore.clear failed: %s", exc)


def _build_field(endpoint_pattern: str, entity_id: int) -> str:
    return f"{endpoint_pattern}|{int(entity_id)}"


def _now_ms() -> int:
    return int(time.time() * 1000)


__all__ = ["PaginationDoneStore", "DEFAULT_HASH_KEY"]
