"""Special parser for baseball at-bat pitches payloads."""

from __future__ import annotations

import re
from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot
from ..entities import extract_entities


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

        at_bat_id = _at_bat_id_from_snapshot(snapshot)
        rows = []
        for item in pitches:
            if not isinstance(item, Mapping):
                continue
            pitcher = _as_mapping(item.get("pitcher"))
            hitter = _as_mapping(item.get("hitter"))
            rows.append(
                {
                    "event_id": snapshot.context_event_id,
                    "at_bat_id": at_bat_id,
                    "ordinal": len(rows),
                    "pitch_id": _as_int(item.get("id")),
                    "pitch_speed": _as_float(item.get("pitchSpeed") or item.get("speed")),
                    "pitch_type": _as_str(item.get("pitchType")),
                    "pitch_zone": _as_str(item.get("pitchZone")),
                    "pitch_x": _as_float(item.get("pitchX")),
                    "pitch_y": _as_float(item.get("pitchY")),
                    "mlb_x": _as_float(item.get("mlbX")),
                    "mlb_y": _as_float(item.get("mlbY")),
                    "outcome": _as_str(item.get("outcome")),
                    "pitcher_id": _as_int(item.get("pitcherId")) or _as_int(pitcher.get("id")) if pitcher else None,
                    "hitter_id": _as_int(item.get("hitterId")) or _as_int(hitter.get("id")) if hitter else None,
                }
            )

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED,
            entity_upserts=extract_entities(snapshot.payload),
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


def _as_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _at_bat_id_from_snapshot(snapshot: RawSnapshot) -> int | None:
    for candidate in (snapshot.resolved_url, snapshot.source_url):
        if not isinstance(candidate, str):
            continue
        match = re.search(r"/atbat/(\d+)/pitches", candidate)
        if match is not None:
            return int(match.group(1))
    return None
