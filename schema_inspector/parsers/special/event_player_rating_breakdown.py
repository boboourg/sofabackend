"""Special parser for `/event/{id}/player/{player_id}/rating-breakdown` payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class EventPlayerRatingBreakdownParser:
    parser_family = "event_player_rating_breakdown"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
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
                        "player_id": snapshot.context_entity_id,
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


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


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
