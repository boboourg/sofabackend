"""Family parser for `/team/{id}` and `/player/{id}` payloads."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_UNSUPPORTED, ParseResult, RawSnapshot
from ..entities import extract_entities
from ..relationships import build_relation_map


class EntityProfilesParser:
    parser_family = "entity_profiles"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        relations: defaultdict[str, list[Mapping[str, object]]] = defaultdict(list)

        team = _as_mapping(payload.get("team"))
        player = _as_mapping(payload.get("player"))
        if team is None and player is None:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_UNSUPPORTED,
                warnings=("Missing team/player envelope.",),
            )

        if team is not None:
            team_id = _as_int(team.get("id"))
            manager = _as_mapping(team.get("manager"))
            venue = _as_mapping(team.get("venue"))
            manager_id = _as_int(manager.get("id")) if manager is not None else None
            venue_id = _as_int(venue.get("id")) if venue is not None else None
            if team_id is not None and manager_id is not None:
                relations["team_manager"].append({"team_id": team_id, "manager_id": manager_id})
            if team_id is not None and venue_id is not None:
                relations["team_venue"].append({"team_id": team_id, "venue_id": venue_id})

        if player is not None:
            player_id = _as_int(player.get("id"))
            team_mapping = _as_mapping(player.get("team"))
            team_id = _as_int(team_mapping.get("id")) if team_mapping is not None else None
            if player_id is not None and team_id is not None:
                relations["player_team"].append({"player_id": player_id, "team_id": team_id})

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED,
            entity_upserts=extract_entities(snapshot.payload),
            relation_upserts=build_relation_map(relations),
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
