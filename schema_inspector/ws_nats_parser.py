"""Pure-text NATS protocol parser for the Sofascore WS feed.

The feed at ``wss://ws.sofascore.com:9222`` ships plain-text NATS
frames inside WebSocket text messages. Each frame is one of:

  ``PING\\r\\n``  /  ``PONG\\r\\n``
  ``+OK\\r\\n``
  ``-ERR 'reason'\\r\\n``
  ``INFO <json>\\r\\n``
  ``MSG <subject> <sid> <byte-count>\\r\\n<payload>\\r\\n``

This module never touches the network — it is the parser-half of the
consumer pipeline. A single ``parse_nats_frames(buffer)`` accepts an
accumulating string and returns ``(messages, leftover)``; the caller
keeps the leftover for the next chunk.

The MSG payload is returned as a raw ``str`` — JSON decoding happens
downstream so the parser stays simple and survives malformed payloads
without dropping the whole batch.
"""
from __future__ import annotations

from typing import Iterable


def parse_nats_frames(buffer: str) -> tuple[list[tuple[str, object]], str]:
    """Parse as many complete NATS frames as ``buffer`` contains.

    Returns ``(messages, leftover)``. ``messages`` is a list of tuples
    ``(kind, payload)`` where ``kind`` is one of ``PING``, ``PONG``,
    ``OK``, ``ERR``, ``INFO``, ``MSG`` and the payload shape depends:
        * PING/PONG/OK → None
        * ERR → the full header string (caller can extract reason)
        * INFO → the JSON tail as a string
        * MSG → ``(subject: str, sid: int, data: str)``

    ``leftover`` is the unconsumed tail (incomplete frame at the end of
    the buffer). Pass it back in with the next chunk.
    """
    messages: list[tuple[str, object]] = []

    while True:
        line_end = buffer.find("\r\n")
        if line_end == -1:
            # No complete header line — hold everything.
            return messages, buffer

        header = buffer[:line_end]

        if header.startswith("PING"):
            messages.append(("PING", None))
            buffer = buffer[line_end + 2:]
            continue
        if header.startswith("PONG"):
            messages.append(("PONG", None))
            buffer = buffer[line_end + 2:]
            continue
        if header.startswith("+OK"):
            messages.append(("OK", None))
            buffer = buffer[line_end + 2:]
            continue
        if header.startswith("-ERR"):
            messages.append(("ERR", header))
            buffer = buffer[line_end + 2:]
            continue
        if header.startswith("INFO"):
            # Strip leading "INFO " (5 chars) — payload starts at index 5.
            messages.append(("INFO", header[5:] if len(header) > 5 else ""))
            buffer = buffer[line_end + 2:]
            continue
        if header.startswith("MSG"):
            parts = header.split()
            if len(parts) < 4:
                # Malformed — drop the header line and continue parsing
                # the next frame so we don't get stuck.
                buffer = buffer[line_end + 2:]
                continue
            try:
                payload_size = int(parts[-1])
            except ValueError:
                buffer = buffer[line_end + 2:]
                continue
            subject = parts[1]
            try:
                sid = int(parts[2])
            except ValueError:
                buffer = buffer[line_end + 2:]
                continue

            payload_start = line_end + 2
            payload_end = payload_start + payload_size

            if len(buffer) < payload_end + 2:
                # Incomplete payload — hold the whole frame.
                return messages, buffer

            data_str = buffer[payload_start:payload_end]
            messages.append(("MSG", (subject, sid, data_str)))
            buffer = buffer[payload_end + 2:]
            continue

        # Unknown header — skip past it to make forward progress.
        buffer = buffer[line_end + 2:]


def build_subscribe_commands(
    sports: Iterable[str],
    *,
    include_odds: bool = True,
    event_sid_offset: int = 1,
    odds_sid_offset: int = 101,
) -> list[bytes]:
    """Build the ``SUB`` commands the consumer issues after the NATS
    handshake. Each command ends with ``\\r\\n`` and is returned as
    bytes ready for ``ws.send``.

    The SID scheme mirrors the existing recorder script:
      * event subscriptions get SIDs starting at ``event_sid_offset``
        (defaults to 1 = football, 2 = tennis, ...).
      * odds subscriptions get SIDs starting at ``odds_sid_offset``
        (defaults to 101 → 113), so the consumer can route by SID
        without subject-string parsing in the hot loop.
    """
    cmds: list[bytes] = []
    for i, sport in enumerate(sports):
        event_sid = event_sid_offset + i
        cmds.append(f"SUB sport.{sport} {event_sid}\r\n".encode("utf-8"))
        if include_odds:
            odds_sid = odds_sid_offset + i
            cmds.append(f"SUB odds.{sport}.1 {odds_sid}\r\n".encode("utf-8"))
    return cmds


def subject_to_sport(subject: str) -> str | None:
    """Extract the sport slug from a known subject. Returns None for
    anything that isn't ``sport.{slug}`` or ``odds.{slug}.{provider}``."""
    if subject.startswith("sport."):
        return subject[len("sport."):]
    if subject.startswith("odds."):
        rest = subject[len("odds."):]
        # rest is "{slug}.{provider}" — take the slug.
        slug = rest.split(".", 1)[0]
        return slug or None
    return None


def subject_to_msg_type(subject: str) -> str | None:
    """``sport.*`` → 'event', ``odds.*`` → 'odds', otherwise None."""
    if subject.startswith("sport."):
        return "event"
    if subject.startswith("odds."):
        return "odds"
    return None
