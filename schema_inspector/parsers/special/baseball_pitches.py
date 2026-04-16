"""Special parser for baseball at-bat pitches payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class BaseballPitchesParser:
    parser_family = "baseball_pitches"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        pitches = payload.get("pitches")
        if not isinstance(pitches, list) or not pitches:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_PARSED_EMPTY,
            )

        rows = []
        for item in pitches:
            if not isinstance(item, Mapping):
                continue
            rows.append(
                {
                    "event_id": snapshot.context_event_id,
                    "pitch_id": _as_int(item.get("id")),
                    "pitch_type": _as_str(item.get("pitchType")),
                    "outcome": _as_str(item.get("outcome")),
                    "speed": _as_int(item.get("speed")),
                }
            )

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED,
            metric_rows={"baseball_pitch": tuple(rows)},
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


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None
