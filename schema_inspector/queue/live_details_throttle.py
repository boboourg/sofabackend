"""Per-event rate-limiter for live-details enqueue.

P0(a) split-details rollout: when the live-tier worker finishes ROOT +
core edges, it would naturally enqueue a details job to
``stream:etl:live_details`` on every poll cycle. With
``LIVE_TIER_1_POLL_SECONDS=5`` that floods the details stream with
duplicate work for the same event.

This throttle uses a Redis ``SET key value NX EX <interval>`` to mark
"recently enqueued" with a short TTL. The first publish for an event
within the interval succeeds; concurrent attempts return False and the
caller skips the enqueue. After the TTL expires, a fresh publish
happens. The result: regardless of root-poll cadence, details for one
event are enqueued at most once per ``LIVE_DETAILS_MIN_INTERVAL_SECONDS``
(default 30s).

This is intentionally separate from
``LiveEventDetailsInFlightStore``:

* throttle    — prevents over-enqueue on the publishing side
* inflight    — prevents duplicate concurrent fanout on the consuming side

Both are needed: without throttle the stream fills with duplicates the
inflight lock would coalesce; without inflight a backlog flush could
produce concurrent fanouts for the same event.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

LIVE_DETAILS_LAST_ENQUEUED_KEY = "live:details_last_enqueued:{event_id}"
_DEFAULT_INTERVAL_SECONDS = 30


class LiveDetailsThrottle:
    """Rate-limits ``refresh_live_event_details`` enqueues per event."""

    def __init__(self, backend: Any, *, interval_seconds: int | None = None) -> None:
        self.backend = backend
        self.interval_seconds = int(
            interval_seconds
            if interval_seconds is not None
            else _env_positive_int(
                "LIVE_DETAILS_MIN_INTERVAL_SECONDS",
                _DEFAULT_INTERVAL_SECONDS,
            )
        )

    def should_enqueue(self, *, event_id: int, marker: str = "1") -> bool:
        """Return True iff a publish for ``event_id`` is permitted now.

        Side effect on True: marks the key with TTL ``interval_seconds`` so
        subsequent calls within the window return False. Atomic via Redis
        ``SET NX EX``.
        """
        key = LIVE_DETAILS_LAST_ENQUEUED_KEY.format(event_id=int(event_id))
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
                logger.warning("LiveDetailsThrottle fallback set failed: %s", exc)
                return True
        except Exception as exc:  # pragma: no cover - defensive fail-open
            logger.warning(
                "LiveDetailsThrottle Redis set failed for event_id=%s: %s — fail-open (allow enqueue)",
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
