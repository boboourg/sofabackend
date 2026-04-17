"""Parser for tennis power snapshots."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class TennisPowerParser:
    parser_family = "tennis_power"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        rankings = payload.get("tennisPowerRankings")
        rows: list[Mapping[str, object]] = []
        if isinstance(rankings, list):
            for ordinal, item in enumerate(rankings):
                if not isinstance(item, Mapping):
                    continue
                rows.append(
                    {
                        "event_id": snapshot.context_event_id or snapshot.context_entity_id,
                        "ordinal": ordinal,
                        "set_number": _as_int(item.get("set")),
                        "game_number": _as_int(item.get("game")),
                        "value": item.get("value"),
                        "break_occurred": _as_bool(item.get("breakOccurred")),
                    }
                )
        else:
            ranking_map = _as_mapping(rankings) or {}
            for ordinal, side_name in enumerate(("home", "away")):
                side = _as_mapping(ranking_map.get(side_name))
                if side is None:
                    continue
                rows.append(
                    {
                        "event_id": snapshot.context_event_id or snapshot.context_entity_id,
                        "ordinal": ordinal,
                        "side": side_name,
                        "current": side.get("current"),
                        "delta": side.get("delta"),
                    }
                )

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED if rows else PARSE_STATUS_PARSED_EMPTY,
            metric_rows={"tennis_power": tuple(rows)} if rows else {},
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
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _as_bool(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    return None
