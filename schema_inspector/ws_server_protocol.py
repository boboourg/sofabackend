"""Mirror WS server protocol + subscription manager.

The mirror server emits **the same NATS subset** that Sofascore does:

  CLIENT → SERVER frames (text, ``\\r\\n``-terminated):
    CONNECT {json}      — opt; server ignores body, just acks.
    SUB <subject> <sid> — subscribe a sid to a subject.
    UNSUB <sid>         — cancel a sid.
    PING / PONG         — heartbeat.

  SERVER → CLIENT frames:
    INFO {json}         — sent on connect (server_id, version, ...)
    MSG <subject> <sid> <byte-count>\\r\\n<payload>\\r\\n — data push.
    PING / PONG         — heartbeat.

Subjects accepted:
    ``sport.{slug}``        → all event deltas for that sport
    ``odds.{slug}.{market}``→ all odds deltas (market=1 today)
    ``event.{event_id}``    → both event and odds deltas tied to one match

This module contains the pure protocol bits (no asyncio). The
asyncio glue lives in ``services.ws_server_service``.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Iterable


_CHANNEL_PREFIX = "ws:fanout"


# ──── Wire format ────────────────────────────────────────────────────


def parse_client_frames(buffer: str) -> tuple[list[tuple[str, Any]], str]:
    """Parse as many complete frames as ``buffer`` contains.

    Returns ``(messages, leftover)``. Frame shapes:
      * ("CONNECT", "<json body>")
      * ("SUB",     (subject: str, sid: int))
      * ("UNSUB",   sid: int)
      * ("PING",    None)
      * ("PONG",    None)
    """
    messages: list[tuple[str, Any]] = []

    while True:
        line_end = buffer.find("\r\n")
        if line_end == -1:
            return messages, buffer

        line = buffer[:line_end]

        if line.startswith("CONNECT"):
            body = line[len("CONNECT"):].strip()
            messages.append(("CONNECT", body))
            buffer = buffer[line_end + 2:]
            continue

        if line.startswith("SUB"):
            parts = line.split()
            if len(parts) < 3:
                buffer = buffer[line_end + 2:]
                continue
            try:
                sid = int(parts[2])
            except ValueError:
                buffer = buffer[line_end + 2:]
                continue
            messages.append(("SUB", (parts[1], sid)))
            buffer = buffer[line_end + 2:]
            continue

        if line.startswith("UNSUB"):
            parts = line.split()
            if len(parts) < 2:
                buffer = buffer[line_end + 2:]
                continue
            try:
                sid = int(parts[1])
            except ValueError:
                buffer = buffer[line_end + 2:]
                continue
            messages.append(("UNSUB", sid))
            buffer = buffer[line_end + 2:]
            continue

        if line.startswith("PING"):
            messages.append(("PING", None))
            buffer = buffer[line_end + 2:]
            continue

        if line.startswith("PONG"):
            messages.append(("PONG", None))
            buffer = buffer[line_end + 2:]
            continue

        # Unknown command — skip the line so we never get stuck.
        buffer = buffer[line_end + 2:]


def format_msg_frame(*, subject: str, sid: int, payload: str) -> str:
    """Build a NATS MSG frame: ``MSG <subject> <sid> <bytes>\\r\\n<payload>\\r\\n``."""
    return f"MSG {subject} {sid} {len(payload)}\r\n{payload}\r\n"


def format_info_frame(**kwargs: Any) -> str:
    """``INFO {<json>}\\r\\n``."""
    body = json.dumps(kwargs, separators=(",", ":"))
    return f"INFO {body}\r\n"


def subject_to_channel(subject: str) -> str | None:
    """Map a client subject to the redis pub/sub channel the consumer
    publishes on.

    sport.football            → ws:fanout:sport:football
    odds.football.1           → ws:fanout:odds:football:1
    event.16167494            → ws:fanout:event:16167494
    """
    if subject.startswith("sport."):
        slug = subject[len("sport."):]
        return f"{_CHANNEL_PREFIX}:sport:{slug}" if slug else None
    if subject.startswith("odds."):
        parts = subject.split(".", 2)
        if len(parts) < 3:
            return None
        return f"{_CHANNEL_PREFIX}:odds:{parts[1]}:{parts[2]}"
    if subject.startswith("event."):
        rest = subject[len("event."):]
        return f"{_CHANNEL_PREFIX}:event:{rest}" if rest else None
    return None


# ──── Subscription manager ───────────────────────────────────────────


@dataclass
class Subscription:
    client_id: str
    sid: int
    subject: str
    channel: str  # cached for fast matches_for() lookup


class SubscriptionManager:
    """Per-server in-memory registry of (client_id, sid) → subject.

    The server uses it to:
      * decide which redis channels to listen to (the union of channels
        across all live subscriptions);
      * route an incoming fanout message to the matching clients.

    Single-instance: each ws server process owns one. Cross-process
    horizontal scaling would need either a shared state or per-process
    redis-fan-in (each server subscribes to *every* channel and filters
    locally; cheaper than coordination).
    """

    def __init__(self) -> None:
        # client_id → {sid → Subscription}
        self._by_client: dict[str, dict[int, Subscription]] = {}
        # channel → list of subscriptions (avoid re-deriving on every msg)
        self._by_channel: dict[str, list[Subscription]] = {}

    def subscribe(self, client_id: str, *, subject: str, sid: int) -> bool:
        """Register a sid for a client. Returns False if the subject
        cannot be mapped to a fanout channel (caller can send an -ERR)."""
        channel = subject_to_channel(subject)
        if channel is None:
            return False
        sub = Subscription(client_id=client_id, sid=sid, subject=subject, channel=channel)
        self._by_client.setdefault(client_id, {})[sid] = sub
        self._by_channel.setdefault(channel, []).append(sub)
        return True

    def unsubscribe(self, client_id: str, *, sid: int) -> None:
        client_subs = self._by_client.get(client_id)
        if client_subs is None:
            return
        sub = client_subs.pop(sid, None)
        if sub is None:
            return
        if not client_subs:
            self._by_client.pop(client_id, None)
        bucket = self._by_channel.get(sub.channel)
        if bucket is not None:
            self._by_channel[sub.channel] = [s for s in bucket if s is not sub]
            if not self._by_channel[sub.channel]:
                self._by_channel.pop(sub.channel, None)

    def disconnect(self, client_id: str) -> None:
        client_subs = self._by_client.pop(client_id, None)
        if not client_subs:
            return
        for sub in list(client_subs.values()):
            bucket = self._by_channel.get(sub.channel)
            if bucket is not None:
                self._by_channel[sub.channel] = [s for s in bucket if s.client_id != client_id]
                if not self._by_channel[sub.channel]:
                    self._by_channel.pop(sub.channel, None)

    def channels_to_listen(self) -> set[str]:
        return set(self._by_channel)

    def matches_for(self, channel: str) -> list[tuple[str, int, str]]:
        """Return ``(client_id, sid, subject)`` triples for every
        subscription that asked for this channel."""
        bucket = self._by_channel.get(channel) or []
        return [(s.client_id, s.sid, s.subject) for s in bucket]
