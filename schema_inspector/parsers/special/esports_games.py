"""Special parser for esports games payloads."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class EsportsGamesParser:
    parser_family = "esports_games"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        games = payload.get("games") or payload.get("esportsGames")
        if not isinstance(games, list) or not games:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_PARSED_EMPTY,
            )

        rows = []
        for item in games:
            if not isinstance(item, Mapping):
                continue
            rows.append(
                {
                    "event_id": snapshot.context_event_id,
                    "game_id": _as_int(item.get("id")),
                    "status": _as_str(item.get("status")),
                    "map_name": _as_str(item.get("mapName")),
                }
            )

        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED,
            metric_rows={"esports_game": tuple(rows)},
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
