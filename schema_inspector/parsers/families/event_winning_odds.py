"""Family parser for `/event/{id}/provider/{provider_id}/winning-odds` payloads."""

from __future__ import annotations

import re
from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


_WINNING_ODDS_URL_PATTERN = re.compile(r"/event/(?P<event_id>\d+)/provider/(?P<provider_id>\d+)/winning-odds")


class EventWinningOddsParser:
    parser_family = "event_winning_odds"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        event_id = snapshot.context_event_id or snapshot.context_entity_id
        provider_id = _extract_provider_id(snapshot)
        rows: list[Mapping[str, object]] = []

        if event_id is not None and provider_id is not None:
            for side, key in (("home", "home"), ("away", "away")):
                item = _as_mapping(payload.get(key))
                if item is None:
                    continue
                rows.append(
                    {
                        "event_id": event_id,
                        "provider_id": provider_id,
                        "side": side,
                        "odds_id": _as_int(item.get("id")),
                        "actual": _as_int(item.get("actual")),
                        "expected": _as_int(item.get("expected")),
                        "fractional_value": _as_scalar_text(item.get("fractionalValue")),
                    }
                )

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED if rows else PARSE_STATUS_PARSED_EMPTY,
            metric_rows={"event_winning_odds": tuple(rows)} if rows else {},
            observed_root_keys=snapshot.observed_root_keys,
        )


def _extract_provider_id(snapshot: RawSnapshot) -> int | None:
    for url in (snapshot.resolved_url, snapshot.source_url):
        if not isinstance(url, str):
            continue
        match = _WINNING_ODDS_URL_PATTERN.search(url)
        if match is None:
            continue
        return _as_int(match.group("provider_id"))
    return None


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_scalar_text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return None


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
