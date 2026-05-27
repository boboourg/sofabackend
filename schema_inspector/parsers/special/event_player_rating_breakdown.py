"""Special parser for `/event/{id}/player/{player_id}/rating-breakdown` payloads."""

from __future__ import annotations

import re
from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


# Authoritative source for player_id is the URL path segment, not
# ``context_entity_id`` — the historical force_d10 backfill (and the
# pilot_orchestrator's per-event detail fanout) stamped ``context_entity_id``
# with the event_id, which made the FK constraint reject every row.  The URL
# pattern matches both ``/player/{player_id}/rating-breakdown`` and any future
# nested resource that keeps the same shape.
_PLAYER_URL_PATTERN = re.compile(r"/player/(?P<player_id>\d+)")


class EventPlayerRatingBreakdownParser:
    parser_family = "event_player_rating_breakdown"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        player_id = _extract_player_id(snapshot)
        rows: list[Mapping[str, object]] = []
        for action_group, items in payload.items():
            if not isinstance(items, (list, tuple)):
                continue
            for ordinal, item in enumerate(items):
                entry = _as_mapping(item)
                if entry is None:
                    continue
                player_coordinates = _as_mapping(entry.get("playerCoordinates"))
                end_coordinates = _as_mapping(entry.get("passEndCoordinates"))
                rows.append(
                    {
                        "event_id": snapshot.context_event_id,
                        "player_id": player_id,
                        "action_group": str(action_group),
                        "ordinal": ordinal,
                        "event_action_type": _as_str(entry.get("eventActionType")),
                        "is_home": _as_bool(entry.get("isHome")),
                        "keypass": _as_bool(entry.get("keypass")),
                        "outcome": _as_bool(entry.get("outcome")),
                        "start_x": _as_float(player_coordinates.get("x")) if player_coordinates is not None else None,
                        "start_y": _as_float(player_coordinates.get("y")) if player_coordinates is not None else None,
                        "end_x": _as_float(end_coordinates.get("x")) if end_coordinates is not None else None,
                        "end_y": _as_float(end_coordinates.get("y")) if end_coordinates is not None else None,
                    }
                )

        status = PARSE_STATUS_PARSED if rows else PARSE_STATUS_PARSED_EMPTY
        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=status,
            metric_rows={"event_player_rating_breakdown_action": tuple(rows)} if rows else {},
            observed_root_keys=snapshot.observed_root_keys,
        )


def _extract_player_id(snapshot: RawSnapshot) -> int | None:
    """Resolve player_id with URL as authoritative source.

    Priority: ``resolved_url`` → ``source_url`` → ``context_entity_id``.
    URL is authoritative because the endpoint pattern is
    ``/event/{event_id}/player/{player_id}/rating-breakdown`` — the integer
    after ``/player/`` is by definition the player_id we need.

    Falls back to ``context_entity_id`` only when no URL is recorded
    (defensive; should never happen for replayed snapshots).
    """
    for url in (snapshot.resolved_url, snapshot.source_url):
        if not isinstance(url, str):
            continue
        match = _PLAYER_URL_PATTERN.search(url)
        if match is None:
            continue
        return _as_int(match.group("player_id"))
    return snapshot.context_entity_id


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.isdigit() or (stripped.startswith("-") and stripped[1:].isdigit()):
            return int(stripped)
    return None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None
