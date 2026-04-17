"""Family parser for `/event/{id}/best-players/summary` payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot
from ..entities import extract_entities


class EventBestPlayersParser:
    parser_family = "event_best_players"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        event_id = snapshot.context_event_id or snapshot.context_entity_id
        rows: list[Mapping[str, object]] = []

        for bucket, payload_key in (
            ("best_home", "bestHomeTeamPlayers"),
            ("best_away", "bestAwayTeamPlayers"),
        ):
            for ordinal, item in enumerate(_as_sequence(payload.get(payload_key))):
                entry = _as_mapping(item)
                if entry is None:
                    continue
                player = _as_mapping(entry.get("player"))
                player_id = _as_int(player.get("id")) if player is not None else None
                rows.append(
                    {
                        "event_id": event_id,
                        "bucket": bucket,
                        "ordinal": ordinal,
                        "player_id": player_id,
                        "label": _as_str(entry.get("label")),
                        "value_text": _as_scalar_text(entry.get("value")),
                        "value_numeric": _as_float(entry.get("value")),
                        "is_player_of_the_match": False,
                    }
                )

        player_of_match = _as_mapping(payload.get("playerOfTheMatch"))
        if player_of_match is not None:
            player = _as_mapping(player_of_match.get("player"))
            player_id = _as_int(player.get("id")) if player is not None else None
            rows.append(
                {
                    "event_id": event_id,
                    "bucket": "player_of_the_match",
                    "ordinal": 0,
                    "player_id": player_id,
                    "label": _as_str(player_of_match.get("label")),
                    "value_text": _as_scalar_text(player_of_match.get("value")),
                    "value_numeric": _as_float(player_of_match.get("value")),
                    "is_player_of_the_match": True,
                }
            )

        status = PARSE_STATUS_PARSED if rows else PARSE_STATUS_PARSED_EMPTY
        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=status,
            entity_upserts=extract_entities(snapshot.payload),
            metric_rows={"event_best_player_entry": tuple(rows)} if rows else {},
            observed_root_keys=snapshot.observed_root_keys,
        )


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_sequence(value: object) -> tuple[object, ...]:
    if isinstance(value, (list, tuple)):
        return tuple(value)
    return ()


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


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_scalar_text(value: object) -> str | None:
    if value is None or isinstance(value, (list, tuple, dict, Mapping)):
        return None
    return str(value)
