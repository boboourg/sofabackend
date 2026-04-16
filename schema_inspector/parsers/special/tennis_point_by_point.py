"""Parser for tennis point-by-point event snapshots."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class TennisPointByPointParser:
    parser_family = "tennis_point_by_point"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        points = payload.get("pointByPoint")
        rows: list[Mapping[str, object]] = []
        if isinstance(points, (list, tuple)):
            for ordinal, item in enumerate(points):
                point = _as_mapping(item)
                if point is None:
                    continue
                score = _as_mapping(point.get("score")) or {}
                rows.append(
                    {
                        "event_id": snapshot.context_event_id or snapshot.context_entity_id,
                        "ordinal": ordinal,
                        "point_id": _as_int(point.get("id")),
                        "set_number": _as_int(point.get("set")),
                        "game_number": _as_int(point.get("game")),
                        "server": _as_str(point.get("server")),
                        "home_score": score.get("home"),
                        "away_score": score.get("away"),
                    }
                )

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED if rows else PARSE_STATUS_PARSED_EMPTY,
            metric_rows={"tennis_point_by_point": tuple(rows)} if rows else {},
            observed_root_keys=snapshot.observed_root_keys,
        )


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None
