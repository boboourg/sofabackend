"""Family parser for `/event/{id}/lineups` payloads."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot
from ..entities import extract_entities
from ..relationships import build_relation_map


class EventLineupsParser:
    parser_family = "event_lineups"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        event_id = snapshot.context_event_id or snapshot.context_entity_id
        relation_rows: defaultdict[str, list[Mapping[str, object]]] = defaultdict(list)
        side_rows: list[Mapping[str, object]] = []

        for side_name in ("home", "away"):
            side = _as_mapping(payload.get(side_name))
            if side is None:
                continue
            side_rows.append(
                {
                    "event_id": event_id,
                    "side": side_name,
                    "formation": _as_str(side.get("formation")),
                }
            )
            players = side.get("players")
            if not isinstance(players, (list, tuple)):
                continue
            for order, item in enumerate(players):
                item_mapping = _as_mapping(item)
                if item_mapping is None:
                    continue
                player = _as_mapping(item_mapping.get("player"))
                player_id = _as_int(player.get("id")) if player is not None else None
                if player_id is None:
                    continue
                relation_rows["event_lineup_player"].append(
                    {
                        "event_id": event_id,
                        "player_id": player_id,
                        "team_id": _as_int(item_mapping.get("teamId")),
                        "side": side_name,
                        "position": _as_str(item_mapping.get("position")),
                        "jersey_number": _as_str(item_mapping.get("jerseyNumber")),
                        "order_value": order,
                        "substitute": _as_bool(item_mapping.get("substitute")),
                    }
                )

        status = PARSE_STATUS_PARSED if side_rows or relation_rows else PARSE_STATUS_PARSED_EMPTY
        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=status,
            entity_upserts=extract_entities(snapshot.payload),
            relation_upserts=build_relation_map(relation_rows),
            metric_rows={"event_lineup_side": tuple(side_rows)} if side_rows else {},
            observed_root_keys=snapshot.observed_root_keys,
        )


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None


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
