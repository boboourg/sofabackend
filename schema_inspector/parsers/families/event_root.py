"""Family parser for `/event/{id}` payloads."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_UNSUPPORTED, ParseResult, RawSnapshot
from ..entities import extract_entities
from ..relationships import build_relation_map


class EventRootParser:
    parser_family = "event_root"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload)
        event = _as_mapping(payload.get("event")) if payload is not None else None
        if event is None:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_UNSUPPORTED,
                warnings=("Missing event envelope.",),
            )

        relations: defaultdict[str, list[Mapping[str, object]]] = defaultdict(list)
        event_id = _as_int(event.get("id")) or snapshot.context_event_id
        home_team = _as_mapping(event.get("homeTeam"))
        away_team = _as_mapping(event.get("awayTeam"))
        season = _as_mapping(event.get("season"))
        tournament = _as_mapping(event.get("tournament"))
        unique_tournament = _as_mapping(tournament.get("uniqueTournament")) if tournament is not None else None
        venue = _as_mapping(event.get("venue"))

        if event_id is not None and home_team is not None:
            home_team_id = _as_int(home_team.get("id"))
            if home_team_id is not None:
                relations["event_team"].append({"event_id": event_id, "team_id": home_team_id, "role": "home"})
                manager = _as_mapping(home_team.get("manager"))
                manager_id = _as_int(manager.get("id")) if manager is not None else None
                if manager_id is not None:
                    relations["team_manager"].append({"team_id": home_team_id, "manager_id": manager_id})
        if event_id is not None and away_team is not None:
            away_team_id = _as_int(away_team.get("id"))
            if away_team_id is not None:
                relations["event_team"].append({"event_id": event_id, "team_id": away_team_id, "role": "away"})
                manager = _as_mapping(away_team.get("manager"))
                manager_id = _as_int(manager.get("id")) if manager is not None else None
                if manager_id is not None:
                    relations["team_manager"].append({"team_id": away_team_id, "manager_id": manager_id})
        if event_id is not None and season is not None:
            season_id = _as_int(season.get("id"))
            if season_id is not None:
                relations["event_season"].append({"event_id": event_id, "season_id": season_id})
        if event_id is not None and tournament is not None:
            tournament_id = _as_int(tournament.get("id"))
            if tournament_id is not None:
                relations["event_tournament"].append({"event_id": event_id, "tournament_id": tournament_id})
        if event_id is not None and unique_tournament is not None:
            unique_tournament_id = _as_int(unique_tournament.get("id"))
            if unique_tournament_id is not None:
                relations["event_unique_tournament"].append(
                    {"event_id": event_id, "unique_tournament_id": unique_tournament_id}
                )
        if event_id is not None and venue is not None:
            venue_id = _as_int(venue.get("id"))
            if venue_id is not None:
                relations["event_venue"].append({"event_id": event_id, "venue_id": venue_id})

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
