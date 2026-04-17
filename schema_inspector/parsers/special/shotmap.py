"""Special parser for shotmap payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class ShotmapParser:
    parser_family = "shotmap"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        points = []
        if isinstance(payload.get("shotmap"), list):
            points.extend(item for item in payload["shotmap"] if isinstance(item, Mapping))
        for key in ("playerPoints", "goalkeeperPoints"):
            if isinstance(payload.get(key), list):
                points.extend(item for item in payload[key] if isinstance(item, Mapping))
        if not points:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_PARSED_EMPTY,
            )

        rows = []
        for ordinal, item in enumerate(points):
            rows.append(
                {
                    "event_id": snapshot.context_event_id,
                    "ordinal": ordinal,
                    "x": _as_float(item.get("x")),
                    "y": _as_float(item.get("y")),
                    "shot_type": _as_str(item.get("shotType")),
                }
            )

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED,
            metric_rows={"shotmap_point": tuple(rows)},
            observed_root_keys=snapshot.observed_root_keys,
        )


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


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


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None
