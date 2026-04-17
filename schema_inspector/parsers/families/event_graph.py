"""Family parser for `/event/{id}/graph` payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class EventGraphParser:
    parser_family = "event_graph"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        event_id = snapshot.context_event_id or snapshot.context_entity_id
        graph_rows = []
        point_rows = []

        if event_id is not None and any(key in payload for key in ("periodTime", "periodCount", "overtimeLength", "graphPoints")):
            graph_rows.append(
                {
                    "event_id": event_id,
                    "period_time": _as_int(payload.get("periodTime")),
                    "period_count": _as_int(payload.get("periodCount")),
                    "overtime_length": _as_int(payload.get("overtimeLength")),
                }
            )

        graph_points = payload.get("graphPoints")
        if isinstance(graph_points, (list, tuple)):
            for ordinal, item in enumerate(graph_points):
                point = _as_mapping(item)
                if point is None:
                    continue
                point_rows.append(
                    {
                        "event_id": event_id,
                        "ordinal": ordinal,
                        "minute": _as_float(point.get("minute")),
                        "value": _as_int(point.get("value")),
                    }
                )

        metric_rows: dict[str, tuple[Mapping[str, object], ...]] = {}
        if graph_rows:
            metric_rows["event_graph"] = tuple(graph_rows)
        if point_rows:
            metric_rows["event_graph_point"] = tuple(point_rows)

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED if metric_rows else PARSE_STATUS_PARSED_EMPTY,
            metric_rows=metric_rows,
            observed_root_keys=snapshot.observed_root_keys,
        )


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


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
