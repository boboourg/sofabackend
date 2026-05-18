"""Push consumer-side WS deltas onto Redis Pub/Sub channels so the
mirror WS server can fan them out to subscribed mobile clients.

Channel layout (NATS-like, but flattened for redis):
  ``ws:fanout:sport:<slug>``        — every event delta for that sport
  ``ws:fanout:odds:<slug>:<market>``— every odds delta (market_id=1 today)
  ``ws:fanout:event:<event_id>``    — derived; the same event delta is
                                      republished here so clients that
                                      only care about one match get a
                                      single-subject feed

Wire format per message::

    {"subject": "<original sofascore subject>",
     "payload": {<flat dot-notation JSON exactly as upstream pushed it>}}

The publisher is intentionally **best-effort**: any redis error is
caught and logged so the consumer's hot-loop never aborts because the
mirror layer is unhappy. If ``redis`` is ``None`` the publisher is a
no-op (used in tests + when the mirror WS server is not deployed).
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


_CHANNEL_PREFIX = "ws:fanout"


class RedisFanoutPublisher:
    """Thin wrapper around either an **async** redis client (whose
    ``publish`` returns an awaitable) or a **sync** one (whose
    ``publish`` returns an int). We detect at call time so the same
    publisher works for the consumer (sync ``redis_backend`` already
    in the ServiceApp) and for the test suite (a FakeRedis with
    ``async def publish``).

    ``redis`` may be ``None`` — in that case every publish is a no-op
    so the consumer runs unchanged on hosts where the mirror WS layer
    is not deployed.
    """

    def __init__(self, redis: Any | None) -> None:
        self.redis = redis

    async def _publish(self, channel: str, message: str) -> None:
        if self.redis is None:
            return
        try:
            result = self.redis.publish(channel, message)
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.exception("ws_fanout publish failed for channel=%s", channel)

    async def publish_event(self, subject: str, payload: dict[str, Any]) -> None:
        """Publish an event-type delta to the sport channel and (if the
        payload carries an id) also to the event-scoped channel."""
        sport = _subject_to_sport(subject)
        message = json.dumps({"subject": subject, "payload": payload}, separators=(",", ":"))

        if sport:
            await self._publish(f"{_CHANNEL_PREFIX}:sport:{sport}", message)

        event_id = payload.get("id")
        if isinstance(event_id, int):
            await self._publish(f"{_CHANNEL_PREFIX}:event:{event_id}", message)

    async def publish_odds(
        self,
        subject: str,
        payload: dict[str, Any],
        *,
        event_id: int | None = None,
    ) -> None:
        """Publish an odds-type delta. Channels:
          * ``ws:fanout:odds:<sport>:<market_id>`` (always)
          * ``ws:fanout:event:<event_id>`` (only when caller resolved
            the offer_id → event_id mapping; the writer already does
            this via the event_market lookup, so it can pass it down)
        """
        sport, market_id = _subject_to_sport_market(subject)
        message = json.dumps({"subject": subject, "payload": payload}, separators=(",", ":"))

        if sport and market_id is not None:
            await self._publish(
                f"{_CHANNEL_PREFIX}:odds:{sport}:{market_id}",
                message,
            )

        if isinstance(event_id, int):
            await self._publish(f"{_CHANNEL_PREFIX}:event:{event_id}", message)


def _subject_to_sport(subject: str) -> str | None:
    """``sport.football`` → ``football``."""
    if subject.startswith("sport."):
        return subject[len("sport."):] or None
    return None


def _subject_to_sport_market(subject: str) -> tuple[str | None, str | None]:
    """``odds.football.1`` → (``football``, ``1``)."""
    if not subject.startswith("odds."):
        return None, None
    parts = subject.split(".", 2)
    if len(parts) < 3:
        return None, None
    return parts[1] or None, parts[2] or None
