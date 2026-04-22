"""Generic entity extraction helpers for raw Sofascore snapshots."""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping


ENTITY_KEY_TO_TYPE = {
    "sport": "sport",
    "country": "country",
    "category": "category",
    "uniqueTournament": "unique_tournament",
    "tournament": "tournament",
    "season": "season",
    "event": "event",
    "homeTeam": "team",
    "awayTeam": "team",
    "parentTeam": "team",
    "team": "team",
    "player": "player",
    "pitcher": "player",
    "hitter": "player",
    "manager": "manager",
    "homeManager": "manager",
    "awayManager": "manager",
    "venue": "venue",
}


def extract_entities(payload: object) -> dict[str, tuple[Mapping[str, object], ...]]:
    found: dict[str, dict[object, Mapping[str, object]]] = defaultdict(dict)
    _walk(payload, parent_key=None, parent_mapping=None, found=found)
    return {
        entity_type: tuple(
            sorted(
                values.values(),
                key=lambda item: (
                    str(item.get("id") if item.get("id") is not None else item.get("alpha2") or item.get("slug") or ""),
                ),
            )
        )
        for entity_type, values in found.items()
        if values
    }


def _walk(
    value: object,
    *,
    parent_key: str | None,
    parent_mapping: Mapping[str, Any] | None,
    found: dict[str, dict[object, Mapping[str, object]]],
) -> None:
    mapping = _as_mapping(value)
    if mapping is not None:
        entity_type = ENTITY_KEY_TO_TYPE.get(parent_key or "")
        if entity_type is not None:
            record = _normalize_entity(entity_type, mapping, parent_mapping)
            if record is not None:
                found[entity_type][_entity_identity(entity_type, record)] = record
        for key, child in mapping.items():
            _walk(child, parent_key=str(key), parent_mapping=mapping, found=found)
        return

    if isinstance(value, (list, tuple)):
        for item in value:
            _walk(item, parent_key=parent_key, parent_mapping=parent_mapping, found=found)


def _normalize_entity(
    entity_type: str,
    mapping: Mapping[str, Any],
    parent_mapping: Mapping[str, Any] | None,
) -> Mapping[str, object] | None:
    if entity_type == "sport":
        entity_id = _as_int(mapping.get("id"))
        if entity_id is None:
            return None
        return {"id": entity_id, "slug": _as_str(mapping.get("slug")), "name": _as_str(mapping.get("name"))}
    if entity_type == "country":
        alpha2 = _as_str(mapping.get("alpha2"))
        name = _as_str(mapping.get("name"))
        if alpha2 is None or name is None:
            return None
        return {
            "alpha2": alpha2,
            "alpha3": _as_str(mapping.get("alpha3")),
            "slug": _as_str(mapping.get("slug")),
            "name": name,
        }
    if entity_type == "category":
        entity_id = _as_int(mapping.get("id"))
        if entity_id is None:
            return None
        sport = _as_mapping(mapping.get("sport"))
        country = _as_mapping(mapping.get("country"))
        return {
            "id": entity_id,
            "slug": _as_str(mapping.get("slug")),
            "name": _as_str(mapping.get("name")),
            "sport_id": _as_int(sport.get("id")) if sport is not None else None,
            "country_alpha2": _as_str(country.get("alpha2")) if country is not None else None,
        }
    if entity_type == "unique_tournament":
        entity_id = _as_int(mapping.get("id"))
        if entity_id is None:
            return None
        category = _as_mapping(mapping.get("category"))
        if category is None and parent_mapping is not None:
            category = _as_mapping(parent_mapping.get("category"))
        country = _as_mapping(mapping.get("country"))
        return {
            "id": entity_id,
            "slug": _as_str(mapping.get("slug")),
            "name": _as_str(mapping.get("name")),
            "category_id": _as_int(category.get("id")) if category is not None else None,
            "country_alpha2": _as_str(country.get("alpha2")) if country is not None else None,
        }
    if entity_type == "tournament":
        entity_id = _as_int(mapping.get("id"))
        if entity_id is None:
            return None
        unique_tournament = _as_mapping(mapping.get("uniqueTournament"))
        category = _as_mapping(mapping.get("category"))
        return {
            "id": entity_id,
            "slug": _as_str(mapping.get("slug")),
            "name": _as_str(mapping.get("name")),
            "category_id": _as_int(category.get("id")) if category is not None else None,
            "unique_tournament_id": _as_int(unique_tournament.get("id")) if unique_tournament is not None else None,
        }
    if entity_type == "season":
        entity_id = _as_int(mapping.get("id"))
        if entity_id is None:
            return None
        return {
            "id": entity_id,
            "name": _as_str(mapping.get("name")),
            "year": _as_str(mapping.get("year")),
            "editor": _as_bool(mapping.get("editor")),
        }
    if entity_type == "event":
        entity_id = _as_int(mapping.get("id"))
        if entity_id is None:
            return None
        tournament = _as_mapping(mapping.get("tournament"))
        season = _as_mapping(mapping.get("season"))
        venue = _as_mapping(mapping.get("venue"))
        home_team = _as_mapping(mapping.get("homeTeam"))
        away_team = _as_mapping(mapping.get("awayTeam"))
        status = _as_mapping(mapping.get("status"))
        unique_tournament = _as_mapping(tournament.get("uniqueTournament")) if tournament is not None else None
        return {
            "id": entity_id,
            "slug": _as_str(mapping.get("slug")),
            "tournament_id": _as_int(tournament.get("id")) if tournament is not None else None,
            "unique_tournament_id": _as_int(unique_tournament.get("id")) if unique_tournament is not None else None,
            "season_id": _as_int(season.get("id")) if season is not None else None,
            "venue_id": _as_int(venue.get("id")) if venue is not None else None,
            "home_team_id": _as_int(home_team.get("id")) if home_team is not None else None,
            "away_team_id": _as_int(away_team.get("id")) if away_team is not None else None,
            "status_code": _as_int(status.get("code")) if status is not None else None,
            "status_type": _as_str(status.get("type")) if status is not None else None,
            "start_timestamp": _as_int(mapping.get("startTimestamp")),
        }
    if entity_type == "team":
        entity_id = _as_int(mapping.get("id"))
        if entity_id is None:
            return None
        manager = _as_mapping(mapping.get("manager"))
        venue = _as_mapping(mapping.get("venue"))
        sport = _as_mapping(mapping.get("sport"))
        category = _as_mapping(mapping.get("category"))
        country = _as_mapping(mapping.get("country"))
        tournament = _as_mapping(mapping.get("tournament"))
        primary_unique_tournament = _as_mapping(mapping.get("primaryUniqueTournament"))
        parent_team = _as_mapping(mapping.get("parentTeam"))
        if parent_mapping is not None:
            parent_tournament = _as_mapping(parent_mapping.get("tournament"))
            if tournament is None:
                tournament = parent_tournament
            if primary_unique_tournament is None and parent_tournament is not None:
                primary_unique_tournament = _as_mapping(parent_tournament.get("uniqueTournament"))
            if category is None and parent_tournament is not None:
                category = _as_mapping(parent_tournament.get("category"))
            if sport is None and category is not None:
                sport = _as_mapping(category.get("sport"))
        return {
            "id": entity_id,
            "slug": _as_str(mapping.get("slug")),
            "name": _as_str(mapping.get("name")),
            "short_name": _as_str(mapping.get("shortName")),
            "manager_id": _as_int(manager.get("id")) if manager is not None else None,
            "venue_id": _as_int(venue.get("id")) if venue is not None else None,
            "sport_id": _as_int(sport.get("id")) if sport is not None else None,
            "category_id": _as_int(category.get("id")) if category is not None else None,
            "country_alpha2": _as_str(country.get("alpha2")) if country is not None else None,
            "tournament_id": _as_int(tournament.get("id")) if tournament is not None else None,
            "primary_unique_tournament_id": (
                _as_int(primary_unique_tournament.get("id")) if primary_unique_tournament is not None else None
            ),
            "parent_team_id": _as_int(parent_team.get("id")) if parent_team is not None else None,
        }
    if entity_type == "player":
        entity_id = _as_int(mapping.get("id"))
        if entity_id is None:
            return None
        team = _as_mapping(mapping.get("team"))
        team_id = _as_int(team.get("id")) if team is not None else None
        if team_id is None and parent_mapping is not None:
            team_id = _as_int(parent_mapping.get("teamId"))
        return {
            "id": entity_id,
            "slug": _as_str(mapping.get("slug")),
            "name": _as_str(mapping.get("name")),
            "short_name": _as_str(mapping.get("shortName")),
            "team_id": team_id,
        }
    if entity_type == "manager":
        entity_id = _as_int(mapping.get("id"))
        if entity_id is None:
            return None
        return {
            "id": entity_id,
            "slug": _as_str(mapping.get("slug")),
            "name": _as_str(mapping.get("name")),
            "short_name": _as_str(mapping.get("shortName")),
        }
    if entity_type == "venue":
        entity_id = _as_int(mapping.get("id"))
        if entity_id is None:
            return None
        country = _as_mapping(mapping.get("country"))
        return {
            "id": entity_id,
            "slug": _as_str(mapping.get("slug")),
            "name": _as_str(mapping.get("name")),
            "country_alpha2": _as_str(country.get("alpha2")) if country is not None else None,
        }
    return None


def _entity_identity(entity_type: str, record: Mapping[str, object]) -> object:
    return record.get("id") or record.get("alpha2") or f"{entity_type}:{record.get('slug')}"


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _as_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


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


def _as_bool(value: object) -> bool | None:
    return value if isinstance(value, bool) else None
