"""Family parser for `/event/{id}/h2h` payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class EventH2HParser:
    parser_family = "event_h2h"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        event_id = snapshot.context_event_id or snapshot.context_entity_id
        duel_rows: list[Mapping[str, object]] = []

        for duel_type, key in (("team", "teamDuel"), ("manager", "managerDuel")):
            duel = _as_mapping(payload.get(key))
            if duel is None or event_id is None:
                continue
            home_wins = _as_int(duel.get("homeWins"))
            away_wins = _as_int(duel.get("awayWins"))
            draws = _as_int(duel.get("draws"))
            if home_wins is None or away_wins is None or draws is None:
                continue
            duel_rows.append(
                {
                    "event_id": event_id,
                    "duel_type": duel_type,
                    "home_wins": home_wins,
                    "away_wins": away_wins,
                    "draws": draws,
                }
            )

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED if duel_rows else PARSE_STATUS_PARSED_EMPTY,
            metric_rows={"event_duel": tuple(duel_rows)} if duel_rows else {},
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
