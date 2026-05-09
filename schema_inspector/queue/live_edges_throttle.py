"""Per-event rate-limiter for live-edges enqueue.

P0(b) tier_1 root-only rollout: when the live-tier worker finishes the
ROOT-only fast path, it would naturally enqueue a follow-up
``refresh_live_event`` (full hydration) to ``stream:etl:live_warm`` on
every poll cycle. With ``LIVE_TIER_1_POLL_SECONDS`` ~10 s this floods
the warm stream with duplicate work for the same event.

This throttle uses a Redis ``SET key value NX EX <interval>`` to mark
"recently enqueued" with a short TTL. The first publish for an event
within the interval succeeds; concurrent attempts return False and the
caller skips the enqueue. After the TTL expires, a fresh publish
happens. Result: regardless of root-poll cadence, edges-fanout for one
event is enqueued at most once per ``LIVE_EDGES_MIN_INTERVAL_SECONDS``
(default 60 s).

Intentionally separate from the in-flight lock — the in-flight lock
prevents duplicate concurrent runs on the consumer side; this throttle
prevents over-publish on the producer side. Both layers are needed to
keep ``stream:etl:live_warm`` length bounded.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

LIVE_EDGES_LAST_ENQUEUED_KEY = "live:edges_last_enqueued:{event_id}"
_DEFAULT_INTERVAL_SECONDS = 60


class LiveEdgesThrottle:
    """Rate-limits ``refresh_live_event`` (edges follow-up) enqueues per event."""

    def __init__(self, backend: Any, *, interval_seconds: int | None = None) -> None:
        self.backend = backend
        self.interval_seconds = int(
            interval_seconds
            if interval_seconds is not None
            else _env_positive_int(
                "LIVE_EDGES_MIN_INTERVAL_SECONDS",
                _DEFAULT_INTERVAL_SECONDS,
            )
        )

    def should_enqueue(self, *, event_id: int, marker: str = "1") -> bool:
        """Return True iff a publish for ``event_id`` is permitted now.

        Side effect on True: marks the key with TTL ``interval_seconds`` so
        subsequent calls within the window return False. Atomic via Redis
        ``SET NX EX``.
        """
        key = LIVE_EDGES_LAST_ENQUEUED_KEY.format(event_id=int(event_id))
        try:
            result = self.backend.set(key, str(marker), nx=True, ex=self.interval_seconds)
        except TypeError:
            # Older redis-py / fake backends without nx/ex kwargs — fall back to
            # a simpler "exists then setex" pattern. Race-acceptable since the
            # worst-case is one extra duplicate enqueue.
            try:
                if self.backend.get(key) is not None:
                    return False
                self.backend.set(key, str(marker))
                if hasattr(self.backend, "expire"):
                    self.backend.expire(key, self.interval_seconds)
                return True
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning("LiveEdgesThrottle fallback set failed: %s", exc)
                return True
        except Exception as exc:  # pragma: no cover - defensive fail-open
            logger.warning(
                "LiveEdgesThrottle Redis set failed for event_id=%s: %s — fail-open (allow enqueue)",
                event_id,
                exc,
            )
            return True
        return bool(result)


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
