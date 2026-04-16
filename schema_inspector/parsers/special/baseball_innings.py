"""Special parser for baseball innings payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot
from ..entities import extract_entities


class BaseballInningsParser:
    parser_family = "baseball_innings"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        innings = payload.get("innings")
        if not isinstance(innings, list) or not innings:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_PARSED_EMPTY,
            )

        rows = []
        for item in innings:
            if not isinstance(item, Mapping):
                continue
            rows.append(
                {
                    "event_id": snapshot.context_event_id,
                    "inning": _as_int(item.get("inning")),
                    "home_score": _as_int(item.get("homeScore")),
                    "away_score": _as_int(item.get("awayScore")),
                }
            )

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED,
            entity_upserts=extract_entities(snapshot.payload),
            metric_rows={"baseball_inning": tuple(rows)},
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
