"""Family parser for season rounds snapshots."""

from __future__ import annotations

from typing import Any, Mapping

from ..base import PARSE_STATUS_PARSED, PARSE_STATUS_PARSED_EMPTY, ParseResult, RawSnapshot


class SeasonRoundsParser:
    parser_family = "season_rounds"
    parser_version = "v1"

    def parse(self, snapshot: RawSnapshot) -> ParseResult:
        payload = _as_mapping(snapshot.payload) or {}
        unique_tournament_id = _as_int(snapshot.context_unique_tournament_id)
        season_id = _as_int(snapshot.context_season_id or snapshot.context_entity_id)
        if unique_tournament_id is None or season_id is None:
            return ParseResult.empty(
                snapshot=snapshot,
                parser_family=self.parser_family,
                parser_version=self.parser_version,
                status=PARSE_STATUS_PARSED_EMPTY,
                warnings=("Missing unique_tournament_id or season_id context for season rounds snapshot.",),
            )

        current_round = _as_mapping(payload.get("currentRound"))
        current_round_number = _as_int(current_round.get("round")) if current_round is not None else None
        rows_by_round: dict[int, dict[str, object]] = {}

        for item in _iter_mappings(payload.get("rounds")):
            round_number = _as_int(item.get("round"))
            if round_number is None:
                continue
            rows_by_round[round_number] = {
                "unique_tournament_id": unique_tournament_id,
                "season_id": season_id,
                "round_number": round_number,
                "round_name": _as_str(item.get("name")),
                "round_slug": _as_str(item.get("slug")),
                "is_current": round_number == current_round_number,
            }

        if current_round_number is not None:
            existing = rows_by_round.get(
                current_round_number,
                {
                    "unique_tournament_id": unique_tournament_id,
                    "season_id": season_id,
                    "round_number": current_round_number,
                    "round_name": None,
                    "round_slug": None,
                    "is_current": True,
                },
            )
            if current_round is not None:
                existing["round_name"] = existing.get("round_name") or _as_str(current_round.get("name"))
                existing["round_slug"] = existing.get("round_slug") or _as_str(current_round.get("slug"))
            existing["is_current"] = True
            rows_by_round[current_round_number] = existing

        rows = tuple(rows_by_round[round_number] for round_number in sorted(rows_by_round))
        return ParseResult(
            snapshot_id=snapshot.snapshot_id,
            parser_family=self.parser_family,
            parser_version=self.parser_version,
            status=PARSE_STATUS_PARSED if rows else PARSE_STATUS_PARSED_EMPTY,
            metric_rows={"season_round": rows} if rows else {},
            observed_root_keys=snapshot.observed_root_keys,
        )


def _as_mapping(value: object) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _iter_mappings(value: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


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
