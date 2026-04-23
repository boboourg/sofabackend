"""Family parser for `/event/{id}/heatmap/{team_id}` payloads."""

from __future__ import annotations

import re
from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


_HEATMAP_URL_PATTERN = re.compile(r"/event/(?P<event_id>\d+)/heatmap/(?P<team_id>\d+)")


class EventTeamHeatmapParser:
    parser_family = "event_team_heatmap"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        event_id = snapshot.context_event_id or snapshot.context_entity_id
        team_id = _extract_team_id(snapshot)
        if event_id is None or team_id is None:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_PARSED_EMPTY,
            )

        heatmap_rows = [{"event_id": event_id, "team_id": team_id}]
        point_rows: list[Mapping[str, object]] = []
        for point_type, payload_key in (("player", "playerPoints"), ("goalkeeper", "goalkeeperPoints")):
            values = payload.get(payload_key)
            if not isinstance(values, (list, tuple)):
                continue
            for ordinal, item in enumerate(values):
                point = _as_mapping(item)
                if point is None:
                    continue
                x = _as_float(point.get("x"))
                y = _as_float(point.get("y"))
                if x is None and y is None:
                    continue
                point_rows.append(
                    {
                        "event_id": event_id,
                        "team_id": team_id,
                        "point_type": point_type,
                        "ordinal": ordinal,
                        "x": x,
                        "y": y,
                    }
                )

        metric_rows: dict[str, tuple[Mapping[str, object], ...]] = {"event_team_heatmap": tuple(heatmap_rows)}
        if point_rows:
            metric_rows["event_team_heatmap_point"] = tuple(point_rows)

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED if point_rows or heatmap_rows else PARSE_STATUS_PARSED_EMPTY,
            metric_rows=metric_rows,
            observed_root_keys=snapshot.observed_root_keys,
        )


def _extract_team_id(snapshot: RawSnapshot) -> int | None:
    for url in (snapshot.resolved_url, snapshot.source_url):
        if not isinstance(url, str):
            continue
        match = _HEATMAP_URL_PATTERN.search(url)
        if match is None:
            continue
        return _as_int(match.group("team_id"))
    return None


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
